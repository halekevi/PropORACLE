"""One-shot fit list for 2026-10-20: every true 5/5 Std/Goblin leg to CSV+text."""
from __future__ import annotations

import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.card_side import load_live_card_book  # noqa: E402
from utils.cushion_z import l5_clears, play_side_gap  # noqa: E402
from utils.fantasy_prop_filter import is_fantasy_prop_label  # noqa: E402
from utils.gap_ranker import wilson_low  # noqa: E402
from utils.l5_ranked_by_cell import (  # noqa: E402
    _CORRECTED_UNGATED,
    five_games,
    gap_z,
)
from utils.leg_flags import (  # noqa: E402
    MIN_PAYOUT_X,
    collect_leg_flags,
    flag_count,
    placeable_at_quote,
)
from utils.prop_norm import canon_prop, fold  # noqa: E402
from utils.role_context import enrich_leg_row  # noqa: E402
from utils.ticket_70_pool import skip_combo_player  # noqa: E402

SLATE = "2026-10-20"
ET = ZoneInfo("America/New_York")
MIN_SLICE_N = 30
# Scraped Power quotes used when the card has already been checked.
SCRAPED_QUOTES = {
    ("Standard Under", 3): 6.0,  # Power $20 → $120 on LIVE-NBA-S3-1
    ("Goblin", 3): 1.65,  # all-Goblin Power 25th pct fallback
    ("Goblin", 2): 1.40,
}
OUT_CSV = ROOT / f"fit_list_{SLATE}.csv"
OUT_TXT = ROOT / f"fit_list_{SLATE}.txt"

# One reporting row per sport even when the slate has zero legs.
ALL_SPORTS = [
    "NBA",
    "NBA1H",
    "TENNIS",
    "MLB",
    "WNBA",
    "SOCCER",
    "NHL",
    "NFL",
    "CFB",
    "GOLF",
]

# Tennis Total Games keep cell (corrected attaches). Games Won uses ungated corrected.
_TENNIS_KEEP = {
    ("TENNIS", "match_total_games", "Goblin", "OVER"): (146, 208),
}

_MAIN_CP = Path(r"H:\PropORACLE_main_cp")
_STEP8_GLOBS: dict[str, tuple[str, ...]] = {
    "NBA": ("nba/step8_all_direction_clean.parquet", "nba/step8_all_direction_clean.xlsx"),
    "NBA1H": (
        "nba1h/step8_nba1h_direction_clean.parquet",
        "nba1h/step8_nba1h_direction_clean.xlsx",
    ),
    "TENNIS": (
        "tennis/step8_tennis_direction.csv",
        "tennis/step8_tennis_direction_clean.xlsx",
        "tennis/step8_tennis_direction_clean.parquet",
    ),
    "MLB": ("mlb/step8_mlb_direction_clean.xlsx", "mlb/step8_mlb_direction.csv"),
    "WNBA": ("wnba/step8_wnba_direction_clean.xlsx", "wnba/step8_wnba_direction.csv"),
    "SOCCER": (
        "soccer/step8_soccer_direction_clean.xlsx",
        "soccer/step8_soccer_direction.csv",
    ),
    "NHL": (
        "nhl/step8_nhl_direction_clean.csv",
        "nhl/step8_nhl_direction_clean.xlsx",
    ),
    "NFL": (
        "nfl/step8_nfl_direction_clean.xlsx",
        f"nfl/step8_nfl_direction_clean_{SLATE}.xlsx",
    ),
    "CFB": (
        "cfb/step8_cfb_direction_clean.xlsx",
        f"cfb/step8_cfb_direction_clean_{SLATE}.xlsx",
    ),
    "GOLF": (
        "golf/step8_golf_direction_clean.xlsx",
        f"golf/step8_golf_direction_clean_{SLATE}.xlsx",
    ),
}


def _pick(row: dict) -> str:
    raw = str(row.get("pick_type") or "").strip().lower()
    if "demon" in raw:
        return "Demon"
    if "goblin" in raw:
        return "Goblin"
    if "standard" in raw:
        return "Standard"
    return ""


def _side(row: dict) -> str:
    for key in (
        "side",
        "direction",
        "over_under",
        "final_bet_direction",
        "bet_direction",
        "model_dir",
    ):
        raw = str(row.get(key) or "").strip().upper()
        if raw in {"O", "OVER"}:
            return "OVER"
        if raw in {"U", "UNDER"}:
            return "UNDER"
    # PrizePicks Goblins are Over-only when the board left side blank.
    if _pick(row) == "Goblin":
        return "OVER"
    return ""


def _sport_label(folder: str, row: dict) -> str:
    raw = str(row.get("sport") or folder or "").strip().upper()
    if raw in {"NBA1H", "NBA_1H", "NBA-1H"}:
        return "NBA1H"
    if raw in {"NBA1Q", "NBA_1Q", "NBA-1Q"}:
        return "NBA1Q"
    if folder.upper() == "NBA1H":
        return "NBA1H"
    if folder.upper() == "NBA1Q":
        return "NBA1Q"
    if folder.upper() == "TENNIS":
        return "TENNIS"
    if folder.upper() == "MLB":
        return "MLB"
    if folder.upper() == "NBA":
        return "NBA"
    return raw or folder.upper()


def _game_started(row: dict, now: datetime) -> bool:
    raw = row.get("start_time") or row.get("game_time") or row.get("commence_time")
    if raw in (None, ""):
        return False
    try:
        ts = pd.Timestamp(raw)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.to_pydatetime() <= now
    except Exception:
        return False


def _game_time_et(row: dict) -> str:
    raw = row.get("start_time") or row.get("game_time") or row.get("commence_time")
    if raw in (None, ""):
        return ""
    try:
        ts = pd.Timestamp(raw)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert(ET).strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        return str(raw)[:19]


def _slate_ok(sport: str, row: dict) -> bool:
    """Tennis: ET today + 2 days. Others: start date == slate (or missing)."""
    raw = row.get("start_time") or row.get("game_time") or row.get("commence_time")
    if raw in (None, ""):
        return sport == "TENNIS"
    try:
        ts = pd.Timestamp(raw)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        day = ts.tz_convert(ET).date().isoformat()
    except Exception:
        return False
    if sport == "TENNIS":
        return day in {SLATE, "2026-10-21", "2026-10-22"}
    return day == SLATE


def _load_ranked() -> dict[tuple[str, str, str], dict]:
    path = ROOT / "data" / "reports" / "all_ranked_prop_hit_rates.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    out: dict[tuple[str, str, str], dict] = {}
    for r in raw.get("rows") or []:
        sport = str(r.get("sport") or "").upper()
        book = str(r.get("book") or "")
        prop = str(r.get("prop") or "")
        if not (sport and book and prop):
            continue
        out[(sport, book, prop)] = r
    return out


def _slice(hit: int | None, n: int | None) -> tuple[int, int, float] | None:
    if not n or int(n) <= 0 or hit is None:
        return None
    h, nn = int(hit), int(n)
    return h, nn, h / nn


def price_leg(sport: str, prop: str, pick: str, side: str, ranked: dict) -> dict | None:
    """L5=5 if n>=30 and beats parent; else best parent among listed / L10>=8 / D."""
    if sport == "TENNIS":
        key = (sport, prop, pick, side)
        if key in _TENNIS_KEEP:
            h, n = _TENNIS_KEEP[key]
            return {
                "slice": "tennis_corrected_keep",
                "hits": h,
                "n": n,
                "hr": h / n,
                "wilson": wilson_low(h, n),
            }
        if key in _CORRECTED_UNGATED:
            h, n = _CORRECTED_UNGATED[key]
            return {
                "slice": "tennis_corrected_ungated",
                "hits": h,
                "n": n,
                "hr": h / n,
                "wilson": wilson_low(h, n),
            }
        # Standard tennis / other props: fall through to ranked file if present

    book = f"{pick} {side}"
    # NBA1H shares NBA category cells when period-specific missing
    cat_sport = "NBA" if sport == "NBA1H" else sport
    row = ranked.get((cat_sport, book, prop)) or ranked.get((sport, book, prop))
    if not row:
        return None

    parents: list[tuple[str, int, int, float]] = []
    for name, h_key, n_key in (
        ("listed_L5ge4", "listed_hits", "listed_n"),
        ("L10ge8", "l10ge8_hits", "l10ge8_n"),
        ("D", "d_hits", "d_n"),
    ):
        sl = _slice(row.get(h_key), row.get(n_key))
        if sl and sl[1] >= MIN_SLICE_N:
            parents.append((name, sl[0], sl[1], sl[2]))
    parent = None
    if parents:
        # best = highest hit rate, then larger n
        parents.sort(key=lambda t: (t[3], t[2]), reverse=True)
        parent = parents[0]

    l5 = _slice(row.get("l5eq5_hits"), row.get("l5eq5_n"))
    if l5 and l5[1] >= MIN_SLICE_N:
        if parent is None or l5[2] > parent[3]:
            return {
                "slice": "L5eq5",
                "hits": l5[0],
                "n": l5[1],
                "hr": l5[2],
                "wilson": wilson_low(l5[0], l5[1]),
            }
    if parent is not None:
        return {
            "slice": f"parent_{parent[0]}",
            "hits": parent[1],
            "n": parent[2],
            "hr": parent[3],
            "wilson": wilson_low(parent[1], parent[2]),
        }
    # last resort: ungated all
    all_sl = _slice(row.get("hits"), row.get("n"))
    if all_sl and all_sl[1] >= MIN_SLICE_N:
        return {
            "slice": "parent_all",
            "hits": all_sl[0],
            "n": all_sl[1],
            "hr": all_sl[2],
            "wilson": wilson_low(all_sl[0], all_sl[1]),
        }
    return None


def load_step8(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_excel(path)


def _first_existing(*paths: Path) -> Path | None:
    for path in paths:
        if path.is_file():
            return path
    return None


def find_sources() -> dict[str, Path | None]:
    """Prefer slate-date step8; fall back to main_cp day-ahead for tennis window sports."""
    sources: dict[str, Path | None] = {}
    dates = [SLATE, "2026-10-19"]
    roots = [ROOT, _MAIN_CP]
    for sport in ALL_SPORTS:
        found: Path | None = None
        for date in dates:
            # Tennis may live on day-ahead; others prefer the slate date first.
            if sport != "TENNIS" and date != SLATE:
                continue
            for root in roots:
                cands = [
                    root / "outputs" / date / rel for rel in _STEP8_GLOBS.get(sport, ())
                ]
                found = _first_existing(*cands)
                if found:
                    break
            if found:
                break
        if sport == "TENNIS" and found is None:
            found = _first_existing(
                _MAIN_CP / "outputs" / "2026-10-19" / "tennis" / "step8_tennis_direction.csv",
                ROOT / "outputs" / SLATE / "tennis" / "step8_tennis_direction.csv",
            )
        sources[sport] = found
    return sources


def flags_for(
    *,
    values: list[float],
    line: float,
    side: str,
    pick: str,
    sport: str,
    prop: str,
    wilson: float | None,
    gap_scaled: float | None,
    slice_n: int | None,
    row: dict,
) -> list[str]:
    """Flags only — never a drop. See utils.leg_flags."""
    return collect_leg_flags(
        values=values,
        line=line,
        side=side,
        pick=pick,
        sport=sport,
        prop=prop,
        wilson=wilson,
        gap_scaled=gap_scaled,
        slice_n=slice_n,
        row=row,
        has_stored_five=len(values) >= 5,
    )


def category_key(sport: str, prop: str, side: str, pick: str) -> str:
    return f"{sport}|{prop}|{side}|{pick}"


def player_key(row: dict) -> str:
    return " ".join(str(row.get("player") or row.get("player_name") or "").lower().split())


def game_key(row: dict) -> str:
    for k in ("game_id", "event_id", "pp_game_id", "game_ppid"):
        raw = row.get(k)
        if raw in (None, ""):
            continue
        try:
            if isinstance(raw, float) and math.isnan(raw):
                continue
        except TypeError:
            pass
        text = str(raw).strip()
        if text and text.lower() != "nan":
            return text
    teams = "|".join(
        sorted(
            t
            for t in (
                str(row.get("team") or "").upper(),
                str(row.get("opp_team") or row.get("opp") or row.get("opponent") or "").upper(),
            )
            if t and t.lower() != "nan"
        )
    )
    return f"{teams}|{_game_time_et(row)[:10]}"


def main() -> None:
    now = datetime.now(timezone.utc)
    ranked = _load_ranked()
    sources = find_sources()
    print("sources:")
    for sp, path in sources.items():
        print(f"  {sp}: {path if path else 'NO FIVE-GAME FILE'}")

    offers = load_live_card_book(list(ALL_SPORTS))
    if offers is None:
        print("WARN: live card unavailable — card_side drops will be skipped (kept)")
    else:
        print(f"live card keys: {len(offers.keys)}")

    # Universe rows: true 5/5 only (after Demon/fantasy/1Q strip at scan)
    universe: list[dict] = []
    sport_scan: dict[str, Counter] = defaultdict(Counter)
    for sport in ALL_SPORTS:
        sport_scan[sport]  # ensure every sport appears in the report

    for sport, path in sources.items():
        if path is None:
            sport_scan[sport]["no_five_game_file"] += 1
            continue
        df = load_step8(path)
        sport_scan[sport]["rows_in_file"] += len(df)
        for _, ser in df.iterrows():
            row = ser.to_dict()
            row["sport"] = _sport_label(sport, row)
            sp = row["sport"]
            if sp == "NBA1Q":
                sport_scan[sport]["skipped_1q"] += 1
                continue
            pick = _pick(row)
            if pick == "Demon":
                sport_scan[sport]["skipped_demon"] += 1
                continue
            if pick not in {"Standard", "Goblin"}:
                sport_scan[sport]["skipped_not_std_gob"] += 1
                continue
            prop_raw = row.get("prop") or row.get("prop_type")
            if is_fantasy_prop_label(prop_raw) or skip_combo_player(row):
                sport_scan[sport]["skipped_fantasy_combo"] += 1
                continue
            prop = canon_prop(sp if sp != "NBA1H" else "NBA", prop_raw)
            if "1q" in fold(prop) or "first_quarter" in fold(str(prop_raw)):
                sport_scan[sport]["skipped_1q_prop"] += 1
                continue
            if not _slate_ok(sp, row):
                sport_scan[sport]["skipped_wrong_slate"] += 1
                continue
            values = five_games(row)
            if values is None:
                sport_scan[sport]["no_five_values"] += 1
                continue
            try:
                line = float(row.get("line"))
            except (TypeError, ValueError):
                sport_scan[sport]["bad_line"] += 1
                continue
            if math.isnan(line):
                sport_scan[sport]["bad_line"] += 1
                continue
            # Goblin = OVER only. Standard = emit each side that is true 5/5.
            if pick == "Goblin":
                sides = ["OVER"]
            else:
                sides = [s for s in ("OVER", "UNDER") if l5_clears(values, line, s)]
                if not sides:
                    sport_scan[sport]["not_five_of_five"] += 1
                    continue
            any_side = False
            for side in sides:
                if pick == "Goblin" and not l5_clears(values, line, side):
                    continue
                any_side = True
                # true 5/5 enters universe
                sport_scan[sport]["true_5of5"] += 1
                play = dict(row)
                play["side"] = side
                play["direction"] = side
                play["prop"] = prop
                play["line"] = line
                play["sport"] = sp
                priced = price_leg(sp, prop, pick, side, ranked)
                raw_gap = play_side_gap(sum(values) / 5.0, line, side)
                gz = gap_z(values, line, side)
                wilson = priced["wilson"] if priced else None
                slice_n = int(priced["n"]) if priced else None
                cell_hr = float(priced["hr"]) if priced else None
                fl = flags_for(
                    values=values,
                    line=line,
                    side=side,
                    pick=pick,
                    sport=sp,
                    prop=prop,
                    wilson=wilson,
                    gap_scaled=gz,
                    slice_n=slice_n,
                    row=play,
                )
                drop_reasons: list[str] = []
                if offers is not None and not offers.offers_side(play):
                    drop_reasons.append("card_side_missing")
                if _game_started(play, now):
                    drop_reasons.append("game_started")
                if priced is None:
                    drop_reasons.append("no_price_slice")

                status = "dropped" if drop_reasons else "kept"
                n_flags = flag_count(fl)
                role = enrich_leg_row(play, sport=sp, prop=prop)
                tr = role.get("team_rank")
                tn = role.get("team_n")
                team_rank_of_n = (
                    f"{tr} of {tn}" if tr != "" and tn != "" else (str(tr) if tr != "" else "")
                )
                universe.append(
                    {
                        "sport": sp,
                        "game_time": _game_time_et(play),
                        "player": str(
                            play.get("player") or play.get("player_name") or ""
                        ).strip(),
                        "prop": prop,
                        "prop_display": str(prop_raw or prop),
                        "side": side,
                        "line": line,
                        "pick": pick,
                        "five_values": ",".join(f"{v:g}" for v in values),
                        "slice_used": priced["slice"] if priced else "",
                        "hits": priced["hits"] if priced else "",
                        "n": priced["n"] if priced else "",
                        "hits_n": (
                            f"{priced['hits']}/{priced['n']}" if priced else ""
                        ),
                        "wilson_low": round(wilson, 6) if wilson is not None else "",
                        "raw_gap": round(raw_gap, 4) if raw_gap is not None else "",
                        "scaled_gap": round(gz, 4) if gz is not None else "",
                        "min_last5_avg": role.get("min_last5_avg", ""),
                        "min_season_avg": role.get("min_season_avg", ""),
                        "starter_flag": role.get("starter_flag", ""),
                        "l5_minutes_gap": role.get("l5_minutes_gap", ""),
                        "minutes_flag": role.get("minutes_flag", ""),
                        "team_rank": role.get("team_rank", ""),
                        "team_n": role.get("team_n", ""),
                        "team_rank_of_n": team_rank_of_n,
                        "top3": bool(role.get("top3")),
                        "bottom3": bool(role.get("bottom3")),
                        "flags": "|".join(fl),
                        "flag_count": n_flags,
                        "status": status,
                        "drop_reason": "|".join(drop_reasons),
                        "_wilson": wilson if wilson is not None else -1.0,
                        "_scaled": gz if gz is not None else -1e9,
                        "_cell_hr": cell_hr if cell_hr is not None else 0.0,
                        "_n_flags": n_flags,
                        "_player_key": player_key(play),
                        "_game_key": game_key(play),
                        "_cat": category_key(sp, prop, side, pick),
                        "_group": (
                            "Standard Over"
                            if pick == "Standard" and side == "OVER"
                            else "Standard Under"
                            if pick == "Standard" and side == "UNDER"
                            else "Goblin"
                        ),
                    }
                )
            if pick == "Goblin" and not any_side:
                sport_scan[sport]["not_five_of_five"] += 1

    # Sort: group, category, Wilson low, scaled gap, fewer flags (tie-break), player
    universe.sort(
        key=lambda r: (
            {"Standard Over": 0, "Standard Under": 1, "Goblin": 2}[r["_group"]],
            r["prop"],
            -r["_wilson"],
            -r["_scaled"],
            r["_n_flags"],
            r["player"],
            r["line"],
        )
    )

    fields = [
        "sport",
        "game_time",
        "player",
        "prop",
        "side",
        "line",
        "pick",
        "five_values",
        "slice_used",
        "hits_n",
        "wilson_low",
        "raw_gap",
        "scaled_gap",
        "min_last5_avg",
        "min_season_avg",
        "starter_flag",
        "l5_minutes_gap",
        "minutes_flag",
        "team_rank",
        "team_n",
        "team_rank_of_n",
        "top3",
        "bottom3",
        "flag_count",
        "flags",
        "status",
        "drop_reason",
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in universe:
            w.writerow(r)

    total = len(universe)
    kept = [r for r in universe if r["status"] == "kept"]
    dropped = [r for r in universe if r["status"] == "dropped"]
    drop_by = Counter()
    for r in dropped:
        for reason in (r["drop_reason"] or "unknown").split("|"):
            drop_by[reason] += 1

    # Walks from kept only. Flags (thin_slice / line_under_8 / stale_gap / …)
    # stay on the row — they do not block. Hard: one cat/player, max 3;
    # same-game allowed (flagged); reject all-same-team slips at score time.
    def walk_pool(group: str, pick: str) -> list[dict]:
        from utils.slip_correlation import all_same_team, pick_legs_without_hard_blocks

        pool = []
        for r in kept:
            if r["_group"] != group:
                continue
            # Walks stay on full-game boards; period boards stay in the CSV.
            if str(r.get("sport") or "").upper() in {"NBA1H", "NBA1Q", "WNBA1H", "WNBA1Q"}:
                continue
            pool.append(r)
        pool.sort(
            key=lambda r: (-r["_wilson"], -r["_scaled"], r["_n_flags"], r["player"])
        )
        used_cat: set[str] = set()
        used_player: set[str] = set()
        ranked: list[dict] = []
        for r in pool:
            if r["_cat"] in used_cat:
                continue
            if r["_player_key"] in used_player:
                continue
            used_cat.add(r["_cat"])
            used_player.add(r["_player_key"])
            ranked.append(r)
        out = pick_legs_without_hard_blocks(ranked, min(3, len(ranked))) or []
        if len(out) >= 2 and all_same_team(out):
            return []
        return out

    def score_walk(name: str, legs: list[dict]) -> dict:
        """Print quote + EV (cell−2pts) on every slip; place if floor+EV clear."""
        from utils.slip_correlation import annotate_slip, hard_block_reason

        n = len(legs)
        quote = SCRAPED_QUOTES.get((name, n))
        hrs = [float(r["_cell_hr"]) for r in legs]
        slip_flags = sorted(
            {
                f
                for r in legs
                for f in str(r.get("flags") or "").split("|")
                if f
            }
        )
        meta = annotate_slip({"legs": legs})
        if meta.get("same_game_pair"):
            slip_flags.append("same_game_pair")
        block = hard_block_reason(legs)
        if block:
            slip_flags.append(block)
        n_flag_legs = sum(1 for r in legs if r["_n_flags"] > 0)
        if block:
            return {
                "name": name,
                "n": n,
                "quote": quote,
                "placeable": False,
                "note": f"blocked: {block}",
                "slip_flags": sorted(set(slip_flags)),
                "n_flag_legs": n_flag_legs,
                "same_game_pair": bool(meta.get("same_game_pair")),
                "p_win": 0.0,
                "ev": -1.0,
            }
        if n < 2:
            return {
                "name": name,
                "n": n,
                "quote": quote,
                "placeable": False,
                "note": f"need >=2 legs; have {n}",
                "slip_flags": sorted(set(slip_flags)),
                "n_flag_legs": n_flag_legs,
                "same_game_pair": bool(meta.get("same_game_pair")),
                "p_win": 0.0,
                "ev": -1.0,
            }
        if quote is None:
            return {
                "name": name,
                "n": n,
                "quote": None,
                "placeable": False,
                "note": f"no scraped N-correct quote for {name} {n}-leg (floor {MIN_PAYOUT_X}x)",
                "slip_flags": sorted(set(slip_flags)),
                "n_flag_legs": n_flag_legs,
                "same_game_pair": bool(meta.get("same_game_pair")),
                "p_win": 0.0,
                "ev": -1.0,
            }
        ok, scored = placeable_at_quote(hrs, quote)
        if not ok and quote < MIN_PAYOUT_X:
            note = f"fails {MIN_PAYOUT_X}x floor at {quote}x"
        elif not ok:
            note = (
                f"clears {MIN_PAYOUT_X}x floor at {quote}x but EV "
                f"{scored['ev']:+.3f} at cell−2pts (p_win={scored['p_win']:.3f}) — blocked"
            )
        else:
            note = (
                f"placeable: {quote}x clears {MIN_PAYOUT_X}x floor; "
                f"EV {scored['ev']:+.3f} at cell−2pts (p_win={scored['p_win']:.3f})"
            )
        return {
            "name": name,
            "n": n,
            "quote": quote,
            "placeable": ok,
            "note": note,
            "slip_flags": sorted(set(slip_flags)),
            "n_flag_legs": n_flag_legs,
            "same_game_pair": bool(meta.get("same_game_pair")),
            "p_win": scored["p_win"],
            "ev": scored["ev"],
        }

    std_over_walk = walk_pool("Standard Over", "Standard")
    std_under_walk = walk_pool("Standard Under", "Standard")
    gob_walk = walk_pool("Goblin", "Goblin")
    scores = {
        "Standard Over": score_walk("Standard Over", std_over_walk),
        "Standard Under": score_walk("Standard Under", std_under_walk),
        "Goblin": score_walk("Goblin", gob_walk),
    }

    lines: list[str] = []
    lines.append(f"FIT LIST {SLATE}")
    lines.append(f"generated_utc={now.isoformat()}")
    lines.append("")
    lines.append("=== RECONCILIATION ===")
    lines.append(f"total_5of5_legs_found = {total}")
    lines.append(f"kept = {len(kept)}")
    lines.append(f"dropped = {len(dropped)}")
    lines.append(f"kept + dropped = {len(kept) + len(dropped)}")
    if len(kept) + len(dropped) != total:
        lines.append("MISMATCH: kept+dropped != total")
    else:
        lines.append("CHECK: kept + dropped == total_5of5  OK")
    lines.append("dropped_by_reason (a row can have multiple reasons; primary tallies below):")
    primary = Counter(r["drop_reason"] for r in dropped)
    for reason, n in primary.most_common():
        lines.append(f"  {reason}: {n}")
    lines.append("dropped_reason_token_counts:")
    for reason, n in drop_by.most_common():
        lines.append(f"  {reason}: {n}")
    lines.append("")
    lines.append("=== PER SPORT (one row each) ===")
    for sp in ALL_SPORTS:
        path = sources.get(sp)
        true_n = int(sport_scan[sp]["true_5of5"])
        k = sum(1 for r in kept if r["sport"] == sp)
        d = sum(1 for r in dropped if r["sport"] == sp)
        if not path:
            reason = "no five-game step8 for this slate"
            lines.append(
                f"{sp}: true_5of5=0 kept=0 dropped=0 | file=NO | reason={reason}"
            )
            continue
        reason_bits = []
        for key, n in sport_scan[sp].most_common():
            if key in {"rows_in_file", "true_5of5", "no_five_game_file"}:
                continue
            reason_bits.append(f"{key}={n}")
        reason = "; ".join(reason_bits) if reason_bits else "cleared pool filters"
        lines.append(
            f"{sp}: true_5of5={true_n} kept={k} dropped={d} | "
            f"file=YES ({path.name}) rows={sport_scan[sp]['rows_in_file']} | "
            f"filters={reason}"
        )
        if k + d != true_n:
            lines.append(f"  MISMATCH for {sp}: kept+dropped != true_5of5")
    lines.append("")

    for group in ("Standard Over", "Standard Under", "Goblin"):
        rows = [r for r in universe if r["_group"] == group]
        krows = [r for r in rows if r["status"] == "kept"]
        lines.append(f"========== {group}  (universe={len(rows)} kept={len(krows)}) ==========")
        # print EVERY universe row in file order (kept and dropped)
        for i, r in enumerate(rows, 1):
            lines.append(
                f"{i:4d}. [{r['status']}] {r['sport']} | {r['game_time']} | {r['player']} | "
                f"{r['prop']} {r['side']} {r['line']} | {r['pick']} | "
                f"L5=[{r['five_values']}] | slice={r['slice_used']} {r['hits_n']} "
                f"w={r['wilson_low']} | gap={r['raw_gap']} gz={r['scaled_gap']} | "
                f"minL5={r.get('min_last5_avg') or '-'} starter={r.get('starter_flag') or '-'} "
                f"min_gap={r.get('l5_minutes_gap') if r.get('l5_minutes_gap') != '' else '-'} "
                f"min_flag={r.get('minutes_flag') or '-'} "
                f"team_rank={r.get('team_rank_of_n') or '-'} "
                f"top3={r.get('top3')} | "
                f"n_flags={r['flag_count']} flags={r['flags'] or '-'} | "
                f"drop={r['drop_reason'] or '-'}"
            )
        lines.append("")

    lines.append(
        f"=== WALKS (kept; 1/cat/player/game; max 3; flags stay; "
        f"floor {MIN_PAYOUT_X}x + EV at cell−2pts) ==="
    )

    def print_walk(title: str, legs: list[dict], scored: dict) -> None:
        lines.append(f"-- {title} --")
        if not legs:
            lines.append("  (empty)")
            return
        for r in legs:
            lines.append(
                f"  {r['sport']} {r['player']} | {r['prop']} {r['side']} {r['line']} | "
                f"{r['hits_n']} w={r['wilson_low']} gz={r['scaled_gap']} | "
                f"{r['five_values']} | {r['game_time']} | "
                f"starter={r.get('starter_flag') or '-'} min_gap={r.get('l5_minutes_gap') if r.get('l5_minutes_gap') != '' else '-'} "
                f"team_rank={r.get('team_rank_of_n') or '-'} | "
                f"n_flags={r['flag_count']} flags={r['flags'] or '-'}"
            )
        q = scored.get("quote")
        q_s = f"{q}x" if q is not None else "n/a"
        lines.append(
            f"  SLIP CARD: quote={q_s}  EV={scored['ev']:+.3f}  "
            f"p_win(cell−2pts)={scored['p_win']:.3f}  "
            f"n_flag_legs={scored['n_flag_legs']}/{scored['n']}  "
            f"flags={','.join(scored['slip_flags']) or '-'}  "
            f"placeable={scored['placeable']}"
        )
        lines.append(f"  {scored['note']}")

    print_walk("Standard Over", std_over_walk, scores["Standard Over"])
    print_walk("Standard Under", std_under_walk, scores["Standard Under"])
    print_walk("Goblin", gob_walk, scores["Goblin"])
    lines.append("")

    lines.append("=== SLIP CLEAR? ===")
    for name in ("Standard Over", "Standard Under", "Goblin"):
        s = scores[name]
        lines.append(f"{name}: {s['note']}")
    placeable = [n for n, s in scores.items() if s["placeable"]]
    if placeable:
        lines.append(
            "BOTTOM LINE: placeable on floor+EV — "
            + ", ".join(placeable)
            + ". Flags stay on the card; R7 = no price change from flags."
        )
    else:
        lines.append(
            f"BOTTOM LINE: no slip clears {MIN_PAYOUT_X}x floor and EV>0 at cell−2pts."
        )
    lines.append("")
    lines.append(
        "GRADING: after the slate, grade every kept leg with its flags "
        "(including voids). Report hit rate by flag vs unflagged in the same "
        "slice (n). Review at ~100 flagged legs per flag type."
    )
    lines.append("")
    lines.append(f"CSV: {OUT_CSV}")
    lines.append(f"TXT: {OUT_TXT}")

    text = "\n".join(lines) + "\n"
    OUT_TXT.write_text(text, encoding="utf-8")
    # also print reconciliation to stdout (full text is in file)
    print(text[:4000])
    print(f"\n... full text written ({len(lines)} lines) -> {OUT_TXT}")
    print(f"csv rows (excl header) = {total} -> {OUT_CSV}")


if __name__ == "__main__":
    main()
