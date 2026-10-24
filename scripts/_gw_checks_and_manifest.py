"""Games Won hand-check, gate branches, same-window compare, slip manifest, soccer split."""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts"), str(ROOT / "Sports" / "Tennis" / "scripts")]

import build_ticket_eval as te  # noqa: E402
from report_gated_construction_efficiency import discover_g70_archives, payload_from_g70  # noqa: E402
from utils.gated_construction_tracks import GATED_GOBLIN, construction_track  # noqa: E402
from utils.leg_start_window import (  # noqa: E402
    grade_dates_for_start,
    is_placeholder_start,
    usable_start_date,
)
from utils.prop_norm import canon_prop  # noqa: E402
from utils.tennis_keep_gates import (  # noqa: E402
    GAMES_WON_L5_SECOND_WON_MIN,
    GAMES_WON_STD_DISCOUNT_MIN,
    tennis_games_won_big_favorite_fade,
    tennis_games_won_factor_keep,
    tennis_goblin_keep_eligible,
    tennis_l5_second_won_pct,
    tennis_opp_lefty,
    tennis_rank_gap,
    tennis_std_discount,
)
from utils.ticket_70_pool import norm_sport  # noqa: E402

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
ET = ZoneInfo("America/New_York")
MANIFEST_PATH = ROOT / "data" / "reports" / "gated_goblin_slip_manifest_20261007.json"


def _starts_for(day: str) -> dict[str, str]:
    """Prefer step1 CSV (PrizePicks source), then step8."""
    by_player: dict[str, Counter] = defaultdict(Counter)
    root = MAIN / "outputs" / day
    files = (
        list(root.glob("tennis/step1_tennis_props.csv"))
        + list(root.glob("step1_tennis_props.csv"))
        + list(root.glob("step8_*tennis*clean*.parquet"))
        + list(root.glob("tennis/*clean*.parquet"))
        + list(root.glob("step8_*tennis*clean*.xlsx"))
    )
    for path in files:
        try:
            if path.suffix.lower() == ".parquet":
                df = pd.read_parquet(path)
            elif path.suffix.lower() == ".csv":
                df = pd.read_csv(path, dtype=str, encoding_errors="replace")
            else:
                df = pd.read_excel(path)
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        player_c = cols.get("player") or cols.get("player_name")
        start_c = cols.get("start_time") or cols.get("game_time") or cols.get("game time")
        if not (player_c and start_c):
            continue
        for player, start in zip(df[player_c], df[start_c], strict=False):
            if player is None or (isinstance(player, float) and pd.isna(player)):
                continue
            if start is None or (isinstance(start, float) and pd.isna(start)):
                continue
            name = str(player).strip().lower()
            stamp = str(start).strip()
            if name and stamp and stamp.lower() != "nan":
                by_player[name][stamp] += 1
        if by_player and "step1" in path.name:
            break
    return {n: c.most_common(1)[0][0] for n, c in by_player.items() if c}


def _one(leg, indices, injury):
    use = dict(leg)
    if not use.get("direction") and use.get("side"):
        use["direction"] = use["side"]
    if not use.get("prop_type") and use.get("prop"):
        use["prop_type"] = use["prop"]
    matched = te._match_leg_to_row_multi(use, indices)
    try:
        line = float(use.get("line"))
    except (TypeError, ValueError):
        line = None
    direction = str(use.get("direction") or use.get("side") or "").strip().upper()
    actual = matched["actual"] if matched else None
    grade_raw = matched["grade_raw"] if matched else ""
    void_note = matched.get("void_note", "") if matched else ""
    g = te._leg_grade(actual, line, direction, grade_raw, void_note)
    g = te._resolve_void_pending_if_injury_dnp(g, use, actual, line, grade_raw, void_note, injury)
    return g, actual, matched


def _slip_id(date_s: str, ticket: dict, legs: list[dict]) -> str:
    tid = str(ticket.get("ticket_id") or ticket.get("id") or "").strip()
    if tid:
        return f"{date_s}|{tid}"
    parts = sorted(
        f"{leg.get('player')}|{leg.get('prop') or leg.get('prop_type')}|{leg.get('line')}"
        for leg in legs
    )
    return f"{date_s}|legs|" + ";".join(parts)


def _load_step8_row(day: str, player: str, prop: str) -> dict:
    root = MAIN / "outputs" / day
    files = (
        list(root.glob("step8_*tennis*clean*.parquet"))
        + list(root.glob("tennis/*clean*.parquet"))
        + list(root.glob("step8_*tennis*clean*.xlsx"))
        + list(root.glob("step8_*tennis*.parquet"))
    )
    want_p = player.strip().lower()
    want_prop = canon_prop("TENNIS", prop)
    for path in files:
        try:
            df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_excel(path)
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        pc = cols.get("player") or cols.get("player_name")
        prop_c = cols.get("prop_type") or cols.get("prop")
        if not (pc and prop_c):
            continue
        for _, r in df.iterrows():
            if str(r.get(pc) or "").strip().lower() != want_p:
                continue
            if canon_prop("TENNIS", str(r.get(prop_c) or "")) != want_prop:
                continue
            return {str(c): r.get(c) for c in df.columns}
    return {}


def _branch(row: dict) -> dict:
    disc = tennis_std_discount(row)
    lefty = tennis_opp_lefty(row)
    sw = tennis_l5_second_won_pct(row)
    gap = tennis_rank_gap(row)
    fade = tennis_games_won_big_favorite_fade(row)
    factor = tennis_games_won_factor_keep(row)
    keep = tennis_goblin_keep_eligible(row)
    branch = "none"
    if fade:
        branch = "fade_50plus"
    elif disc is not None and disc >= GAMES_WON_STD_DISCOUNT_MIN:
        branch = "std_discount"
    elif factor:
        if lefty is True:
            branch = "l10_lefty"
        elif sw is not None and sw >= GAMES_WON_L5_SECOND_WON_MIN:
            branch = "l10_2nd_won"
        else:
            branch = "factor_other"
    return {
        "keep": keep,
        "branch": branch,
        "disc": disc,
        "lefty": lefty,
        "second_won": sw,
        "rank_gap": gap,
        "fade": fade,
        "l5": row.get("l5_over") or row.get("L5 Over") or row.get("l5"),
        "l10": row.get("l10_over") or row.get("L10 Over") or row.get("l10"),
        "standard_line": row.get("standard_line") or row.get("Standard Line"),
        "opp_lefty_raw": row.get("opp_lefty") or row.get("vs_lefty") or row.get("opponent_hand"),
        "second_won_raw": row.get("l5_second_won_pct") or row.get("second_won_l5") or row.get("L5 2nd Won %"),
    }


def _espn_get(url: str):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=25) as resp:
        return json.loads(resp.read().decode())


def _hand_score_from_espn(player: str, start: str) -> dict | None:
    """Recompute games_won from linescores on the live/historical scoreboard."""
    day = usable_start_date(start) or str(start)[:10]
    if len(day) != 10:
        return None
    want = " ".join(player.casefold().split())
    for tour in ("atp", "wta"):
        for ymd in (day, (date.fromisoformat(day) + timedelta(days=1)).isoformat()):
            url = (
                f"https://site.api.espn.com/apis/site/v2/sports/tennis/{tour}/scoreboard"
                f"?dates={ymd.replace('-', '')}"
            )
            try:
                payload = _espn_get(url)
            except Exception:
                continue
            for ev in payload.get("events") or []:
                for comp in ev.get("competitions") or []:
                    comps = list(comp.get("competitors") or [])
                    names = [
                        str(((c.get("athlete") or {}).get("displayName") or ""))
                        for c in comps
                    ]
                    if not any(want in " ".join(n.casefold().split()) for n in names):
                        continue
                    status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "")
                    detail = str(((comp.get("status") or {}).get("type") or {}).get("detail") or "")
                    mine = next(
                        (
                            c
                            for c in comps
                            if want
                            in " ".join(
                                str(((c.get("athlete") or {}).get("displayName") or "")).casefold().split()
                            )
                        ),
                        None,
                    )
                    if mine is None:
                        continue
                    lines = mine.get("linescores") or []
                    games = sum(float((x or {}).get("value") or 0) for x in lines)
                    set_bits = [str(int(float((x or {}).get("value") or 0))) for x in lines]
                    other = next((c for c in comps if c is not mine), None)
                    other_games = 0.0
                    other_bits = []
                    if other is not None:
                        ols = other.get("linescores") or []
                        other_games = sum(float((x or {}).get("value") or 0) for x in ols)
                        other_bits = [str(int(float((x or {}).get("value") or 0))) for x in ols]
                    score_line = "-".join(
                        f"{a}-{b}" for a, b in zip(set_bits, other_bits or ["?"] * len(set_bits))
                    )
                    return {
                        "player": player,
                        "tour": tour,
                        "event": ev.get("id"),
                        "date": ev.get("date"),
                        "status": status,
                        "detail": detail,
                        "games_won_hand": games,
                        "opp_games": other_games,
                        "score_line": score_line,
                        "retired": any(
                            tok in (status + " " + detail).upper()
                            for tok in ("RET", "ABANDON", "WALKOVER", "W/O")
                        ),
                    }
    return None


def main() -> None:
    te.PROP_ALIAS_INDEX = True
    cache = {}

    def indices(day):
        if day not in cache:
            cache[day] = (
                te._load_actuals_indices({}, [day], slate_date=day, payload={}),
                te._injury_dnp_by_sport([day]),
            )
        return cache[day]

    # ---- gather slips with grades (start-date aware) ----
    slips: dict[str, dict] = {}
    gw_legs: list[dict] = []  # gated Games Won decided
    tg_legs: list[dict] = []
    starts_cache: dict[str, dict[str, str]] = {}

    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        starts = starts_cache.setdefault(date_s, _starts_for(date_s))
        base_idx, base_inj = indices(date_s)
        for group in payload.get("groups") or []:
            gname = str(group.get("group_name") or "")
            for ticket in group.get("tickets") or []:
                if not isinstance(ticket, dict):
                    continue
                if construction_track(ticket, gname) != GATED_GOBLIN:
                    continue
                legs = [leg for leg in (ticket.get("legs") or []) if isinstance(leg, dict)]
                if not legs:
                    continue
                sid = _slip_id(date_s, ticket, legs)
                if sid in slips:
                    continue
                grades = []
                unresolved_reason = ""
                for leg in legs:
                    player = str(leg.get("player") or "")
                    prop = str(leg.get("prop") or leg.get("prop_type") or "")
                    sport = norm_sport(str(leg.get("sport") or ""))
                    start = starts.get(player.lower(), "")
                    gb, actual, matched = _one(leg, base_idx, base_inj)
                    ga = gb
                    source = "slate"
                    if gb not in {"HIT", "MISS"}:
                        if is_placeholder_start(start) or not usable_start_date(start):
                            ga = "UNRESOLVED"
                            unresolved_reason = (
                                "placeholder_23"
                                if is_placeholder_start(start)
                                else ("missing_start" if not start else "bad_start")
                            )
                        else:
                            filled = None
                            used = ""
                            for day in grade_dates_for_start(start):
                                go, act2, _ = _one(leg, *indices(day))
                                if go in {"HIT", "MISS"}:
                                    filled = go
                                    actual = act2
                                    used = day
                                    break
                            if filled:
                                ga = filled
                                source = f"fill:{used}"
                            else:
                                ga = "UNRESOLVED"
                                unresolved_reason = "not_in_window"
                    grades.append(ga)
                    if sport.lower() == "tennis" and ga in {"HIT", "MISS"}:
                        prop_k = canon_prop("TENNIS", prop)
                        rec = {
                            "date": date_s,
                            "slip": sid,
                            "player": player,
                            "prop": prop,
                            "prop_k": prop_k,
                            "line": leg.get("line"),
                            "side": str(leg.get("side") or leg.get("direction") or "OVER").upper(),
                            "grade": ga,
                            "actual": actual,
                            "start": start,
                            "source": source,
                            "pick_type": str(leg.get("pick_type") or "Goblin"),
                        }
                        if prop_k == "games_won":
                            gw_legs.append(rec)
                        elif prop_k == "match_total_games":
                            tg_legs.append(rec)
                resolved = all(g in {"HIT", "MISS"} for g in grades)
                win = te._ticket_pays_money(gname, grades) if resolved else None
                slips[sid] = {
                    "id": sid,
                    "date": date_s,
                    "ticket_id": str(ticket.get("ticket_id") or ticket.get("id") or ""),
                    "group": gname,
                    "resolved": resolved,
                    "win": win,
                    "grades": grades,
                    "unresolved_reason": unresolved_reason if not resolved else "",
                    "players": [str(l.get("player") or "") for l in legs],
                    "props": [str(l.get("prop") or l.get("prop_type") or "") for l in legs],
                    "sports": [norm_sport(str(l.get("sport") or "")) for l in legs],
                }

    # ---- manifest ----
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(
            {
                "written_at": datetime.now(ET).isoformat(timespec="seconds"),
                "n": len(slips),
                "resolved": sum(1 for s in slips.values() if s["resolved"]),
                "unresolved": sum(1 for s in slips.values() if not s["resolved"]),
                "slips": list(slips.values()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    resolved_n = sum(1 for s in slips.values() if s["resolved"])
    unresolved_n = len(slips) - resolved_n
    wins = sum(1 for s in slips.values() if s["resolved"] and s["win"])
    print(f"MANIFEST {MANIFEST_PATH}")
    print(f"SET n={len(slips)} resolved={resolved_n} unresolved={unresolved_n} wins={wins}/{resolved_n}")

    # Prior set from last report numbers for reconciliation note
    # Load previous manifest if any; else reconstruct note from counts.
    prev = ROOT / "data" / "reports" / "gated_goblin_slip_manifest_prev.json"
    if prev.is_file():
        old = json.loads(prev.read_text(encoding="utf-8"))
        old_ids = {s["id"] for s in old.get("slips") or []}
        new_ids = set(slips)
        print(f"ADDED {len(new_ids - old_ids)}")
        for sid in sorted(new_ids - old_ids)[:40]:
            print(f"  + {sid}")
        print(f"REMOVED {len(old_ids - new_ids)}")
        for sid in sorted(old_ids - new_ids)[:40]:
            print(f"  - {sid}")
    else:
        print("NO_PREV_MANIFEST (first write; next run will reconcile)")

    # Save as prev for next time after also copying current
    # Keep both: current manifest is the source of truth.

    # unresolved reasons
    ur = Counter(s["unresolved_reason"] for s in slips.values() if not s["resolved"])
    print("UNRESOLVED_REASONS", dict(ur))

    # ---- Games Won summary ----
    print(f"\nGATED_GAMES_WON n={len(gw_legs)} hits={sum(1 for r in gw_legs if r['grade']=='HIT')}")
    print(f"GATED_TOTAL_GAMES n={len(tg_legs)} hits={sum(1 for r in tg_legs if r['grade']=='HIT')}")

    # ---- gate branches for each GW leg ----
    print("\n=== GATE BRANCHES (47 Games Won) ===")
    branch_hits = defaultdict(Counter)
    for rec in gw_legs:
        row = _load_step8_row(rec["date"], rec["player"], rec["prop"])
        # overlay leg fields
        row.setdefault("sport", "TENNIS")
        row.setdefault("pick_type", rec["pick_type"])
        row.setdefault("side", rec["side"])
        row.setdefault("line", rec["line"])
        row.setdefault("player", rec["player"])
        row.setdefault("prop", rec["prop"])
        # directional L5/L10 from archive leg if step8 blank
        if rec.get("line") is not None:
            row.setdefault("l5", None)
        info = _branch(row)
        branch_hits[info["branch"]][rec["grade"]] += 1
        print(
            f"  {rec['date']} {rec['player']} line={rec['line']} grade={rec['grade']} "
            f"branch={info['branch']} disc={info['disc']} lefty={info['lefty']} "
            f"2nd={info['second_won']} l10={info['l10']} fade={info['fade']} "
            f"lefty_raw={info['opp_lefty_raw']!r} 2nd_raw={info['second_won_raw']!r}"
        )
    print("BRANCH_SUMMARY")
    for b, c in sorted(branch_hits.items()):
        n = c["HIT"] + c["MISS"]
        print(f"  {b}: {c['HIT']}/{n}")

    # ---- hand verify 10 GW legs ----
    print("\n=== HAND VERIFY 10 Games Won ===")
    sample = gw_legs[:10] if len(gw_legs) >= 10 else gw_legs
    # prefer mix of HIT/MISS and a fill if any
    hits = [r for r in gw_legs if r["grade"] == "HIT"]
    misses = [r for r in gw_legs if r["grade"] == "MISS"]
    fills = [r for r in gw_legs if r["source"].startswith("fill")]
    sample = []
    for bucket in (fills, hits, misses):
        for r in bucket:
            if r not in sample:
                sample.append(r)
            if len(sample) >= 10:
                break
        if len(sample) >= 10:
            break
    for rec in sample:
        hand = _hand_score_from_espn(rec["player"], rec["start"] or rec["date"])
        stored = rec["actual"]
        try:
            line = float(rec["line"])
        except (TypeError, ValueError):
            line = None
        if hand is None:
            print(f"  {rec['date']} {rec['player']} stored={stored} ESPN=NOT_FOUND start={rec['start']}")
            continue
        hand_g = hand["games_won_hand"]
        hand_grade = None
        if line is not None:
            hand_grade = "HIT" if hand_g > line else "MISS"
        match = (
            stored is not None
            and abs(float(stored) - hand_g) < 0.01
            and (hand_grade is None or hand_grade == rec["grade"])
        )
        print(
            f"  {rec['date']} {rec['player']} line={rec['line']} stored={stored} "
            f"hand={hand_g} score={hand['score_line']} status={hand['status']} "
            f"retired={hand['retired']} grade={rec['grade']} hand_grade={hand_grade} "
            f"MATCH={match}"
        )

    # ---- same window / same line gated vs ungated ----
    print("\n=== SAME WINDOW SAME LINE (Games Won Goblin OVER) ===")
    dates = sorted({r["date"] for r in gw_legs})
    # catalog from graded_props on those dates
    gated_keys = {
        (r["date"], r["player"].lower(), float(r["line"]))
        for r in gw_legs
        if r["line"] is not None
    }
    # also by line band
    by_line_gated = defaultdict(Counter)
    by_line_ungated = defaultdict(Counter)
    ungated_all = Counter()
    gated_all = Counter()
    for day in dates:
        path = MAIN / "ui_runner" / "templates" / f"graded_props_{day}.json"
        if not path.is_file():
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if not str(row.get("sport") or "").upper().startswith("TENNIS"):
                continue
            if "goblin" not in str(row.get("pick_type") or "").lower():
                continue
            if str(row.get("direction") or "").upper() != "OVER":
                continue
            if canon_prop("TENNIS", str(row.get("prop") or "")) != "games_won":
                continue
            res = str(row.get("result") or "").upper()
            if res not in {"HIT", "MISS"}:
                continue
            try:
                line = float(row.get("line"))
            except (TypeError, ValueError):
                continue
            key = (day, str(row.get("player") or "").lower(), line)
            bucket = by_line_gated if key in gated_keys else by_line_ungated
            # wait - gated_keys is slip legs; catalog row matching player+line+date
            is_gated = key in gated_keys
            target = gated_all if is_gated else ungated_all
            target[res] += 1
            line_bucket = by_line_gated if is_gated else by_line_ungated
            line_bucket[line][res] += 1
    gh, gn = gated_all["HIT"], gated_all["HIT"] + gated_all["MISS"]
    uh, un = ungated_all["HIT"], ungated_all["HIT"] + ungated_all["MISS"]
    print(f"  gated on those dates: {gh}/{gn} = {gh/gn if gn else None}")
    print(f"  ungated on those dates: {uh}/{un} = {uh/un if un else None}")
    # lines that appear in both
    shared_lines = sorted(set(by_line_gated) & set(by_line_ungated))
    print(f"  shared line levels: {len(shared_lines)}")
    for line in shared_lines[:12]:
        g = by_line_gated[line]
        u = by_line_ungated[line]
        gn = g["HIT"] + g["MISS"]
        un = u["HIT"] + u["MISS"]
        print(
            f"    line {line}: gated {g['HIT']}/{gn} ungated {u['HIT']}/{un}"
        )

    # full catalog window check
    print("\n=== CATALOG WINDOW ===")
    cat_dates = []
    templates = MAIN / "ui_runner" / "templates"
    for path in sorted(templates.glob("graded_props_2026-0[89]*.json")):
        day = path.stem.replace("graded_props_", "")[:10]
        if "2026-08-26" <= day <= "2026-09-30":
            cat_dates.append(day)
    print(f"  catalog dates {cat_dates[0]}..{cat_dates[-1]} n={len(cat_dates)}")
    print(f"  gated slip dates {dates[0]}..{dates[-1]} n={len(dates)}")
    print(f"  overlap {len(set(dates) & set(cat_dates))}")

    # ---- 11pm stamp source ----
    print("\n=== 11PM STAMP SOURCE ===")
    ph_slips = [s for s in slips.values() if s["unresolved_reason"] == "placeholder_23"]
    print(f"placeholder slips {len(ph_slips)}")
    # compare step1 vs step8 for a few
    for day in ("2026-09-20", "2026-09-27"):
        step1 = MAIN / "outputs" / day / "tennis" / "step1_tennis_props.csv"
        if not step1.is_file():
            continue
        df = pd.read_csv(step1, dtype=str)
        n23 = df["start_time"].fillna("").str.contains("T23:00:00").sum()
        print(f"  step1 {day}: {n23}/{len(df)} rows are 23:00 ET (PrizePicks start_time)")

    # Can step1 backfill non-23 starts for unresolved?
    print("\n=== STEP1 BACKFILL POTENTIAL ===")
    miss_start = [s for s in slips.values() if s["unresolved_reason"] in {"missing_start", "bad_start", "placeholder_23"}]
    recoverable = 0
    for s in miss_start:
        day = s["date"]
        starts = starts_cache.get(day) or _starts_for(day)
        for player in s["players"]:
            st = starts.get(player.lower(), "")
            if st and not is_placeholder_start(st) and usable_start_date(st):
                recoverable += 1
                print(f"  recoverable start {day} {player} -> {st}")
                break
    print(f"slips with a usable step1 start available: {recoverable}/{len(miss_start)}")

    # ---- soccer coverage split ----
    print("\n=== SOCCER MISSES Sep 27 ===")
    day = "2026-09-27"
    day_dir = MAIN / "outputs" / day
    actuals = pd.read_csv(day_dir / f"actuals_soccer_{day}.csv", dtype=str)
    have = set(" ".join(str(p).casefold().split()) for p in actuals["player"] if str(p).strip())
    # load soccer step8
    slate_files = list(day_dir.glob("**/step8*soccer*")) + list(day_dir.glob("step8*soccer*"))
    rows = []
    for path in slate_files:
        if path.suffix.lower() not in {".parquet", ".csv", ".xlsx"}:
            continue
        try:
            df = pd.read_parquet(path) if path.suffix == ".parquet" else (
                pd.read_csv(path, dtype=str) if path.suffix == ".csv" else pd.read_excel(path)
            )
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        for _, r in df.iterrows():
            player = str(r.get(cols.get("player") or cols.get("player_name")) or "").strip()
            if not player or " + " in player:
                continue
            start = r.get(cols.get("start_time") or cols.get("game_time") or "")
            from utils.eastern_slate_date import eastern_slate_date
            from utils.soccer_board_coverage import covered_events, event_matches_sides, match_sides

            if eastern_slate_date(start) != day:
                continue
            # started?
            try:
                dt = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                dt = dt.astimezone(ET)
            except Exception:
                continue
            if day >= datetime.now(ET).date().isoformat() and dt > datetime.now(ET):
                continue
            sides = match_sides(
                {
                    "sport": "SOCCER",
                    "matchup": r.get(cols.get("matchup") or "") if cols.get("matchup") else "",
                    "team": r.get(cols.get("team") or "") if cols.get("team") else "",
                    "opp_team": r.get(cols.get("opp_team") or cols.get("opponent") or "")
                    if (cols.get("opp_team") or cols.get("opponent"))
                    else "",
                    "start_time": start,
                }
            )
            events = covered_events(day) or ()
            if sides is None or not any(event_matches_sides(sides[0], sides[1], ev) for ev in events):
                continue
            name = " ".join(player.casefold().split())
            prop = str(r.get(cols.get("prop_type") or cols.get("prop") or "") or "")
            league = str(r.get(cols.get("league") or cols.get("competition") or "") or "")
            starter = str(
                r.get(cols.get("minutes_tier") or "")
                or r.get(cols.get("role_tier") or "")
                or r.get(cols.get("usage_tier") or "")
                or r.get(cols.get("off_tier") or "")
                or r.get(cols.get("starter") or "")
                or ""
            )
            # common soccer starter flags
            for k, c in cols.items():
                if any(tok in k for tok in ("starter", "lineup", "minutes", "role", "usage", "off")):
                    val = str(r.get(c) or "").strip()
                    if val:
                        starter = f"{starter}|{k}={val}".strip("|")
            rows.append(
                {
                    "player": player,
                    "name": name,
                    "prop": prop,
                    "league": league,
                    "starter": starter,
                    "missing": name not in have,
                    "matchup": sides,
                }
            )
        if rows:
            break
    # dedupe by player
    by_player = {}
    for r in rows:
        by_player.setdefault(r["name"], r)
    missing = [r for r in by_player.values() if r["missing"]]
    present = [r for r in by_player.values() if not r["missing"]]
    print(f"started covered players {len(by_player)} missing {len(missing)}")
    # starter flag split
    def is_starter(s: str) -> str:
        u = s.upper()
        if any(tok in u for tok in ("STARTER", "START", "HIGH", "ELITE")):
            return "starterish"
        if any(tok in u for tok in ("BENCH", "LOW", "DNP", "SIT")):
            return "benchish"
        if not s:
            return "blank"
        return "other"

    print("by starter flag (missing):")
    print(Counter(is_starter(r["starter"]) for r in missing))
    print("by starter flag (present):")
    print(Counter(is_starter(r["starter"]) for r in present))
    print("missing by league:")
    print(Counter(r["league"] or "?" for r in missing))
    print("missing by prop:")
    print(Counter(canon_prop("SOCCER", r["prop"]) or r["prop"] or "?" for r in missing))
    print("sample missing:")
    for r in missing[:15]:
        print(f"  {r['player']} {r['prop']} league={r['league']!r} starter={r['starter'][:80]!r}")


if __name__ == "__main__":
    main()
