#!/usr/bin/env python3
"""Stack player / opponent ATP-WTA ranks onto tennis best keep gates.

For each live keep path (Goblin GW, Goblin TG, Std OVER GW), report hit rate
by player_tier, opp_tier, and rank_gap, plus stacked filters (keep + favorable
rank bands) so we can see whether ranks add lift.

  py -3.14 scripts/tennis_keep_gates_rank_stack.py
"""
from __future__ import annotations

import json
import math
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO), str(_REPO / "Sports" / "Tennis" / "scripts"), str(_REPO / "scripts")]

from tennis_shared import (  # noqa: E402
    build_sackmann_player_index,
    ensure_sackmann_matches,
    norm_key,
    norm_tennis_prop,
    parse_sackmann_tourney_date,
)
from tennis_ta_matchlog import load_cached_ta_player_index, parse_ta_date  # noqa: E402
from utils.tennis_keep_gates import (  # noqa: E402
    GAMES_WON_L5_SECOND_WON_MIN,
    GAMES_WON_STD_DISCOUNT_MIN,
    GAMES_WON_STD_OVER_L5_GAP_MIN,
)
from utils.tennis_rank_d import (  # noqa: E402
    TennisOppRankIndex,
    parse_int_rank,
    rank_to_tier,
    row_rank_fields,
)

OUT = _REPO / "data" / "reports" / "tennis_keep_gates_rank_stack.json"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]

TIER_ORDER = ("Elite", "Above Avg", "Avg", "Below Avg", "Weak", "Unknown")
# Lower ATP/WTA number = better. gap = opp_rank - player_rank (>0 means player is higher-ranked).
GAP_BANDS = (
    ("player_better_50plus", lambda g: g is not None and g >= 50),
    ("player_better_1_49", lambda g: g is not None and 1 <= g < 50),
    ("near_even_pm20", lambda g: g is not None and -20 <= g <= 20),
    ("opp_better_1_49", lambda g: g is not None and -49 <= g <= -1),
    ("opp_better_50plus", lambda g: g is not None and g <= -50),
    ("unknown_gap", lambda g: g is None),
)


def _finite(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def _mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def _pct(h: int, n: int) -> float | None:
    return round(100.0 * h / n, 1) if n else None


def _wilson(h: int, n: int, z: float = 1.96) -> list[float | None]:
    if n <= 0:
        return [None, None]
    p = h / n
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    half = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / den
    return [round(100.0 * (center - half), 1), round(100.0 * (center + half), 1)]


def _book(row: dict) -> str | None:
    pt = str(row.get("pick_type") or row.get("book") or "").strip().lower()
    d = str(row.get("direction") or row.get("over_under") or "").strip().upper()
    if "demon" in pt:
        return None
    if "goblin" in pt:
        return f"Goblin {d if d in ('OVER', 'UNDER') else 'OVER'}"
    if d in ("OVER", "UNDER"):
        return f"Standard {d}"
    return None


def _iter_props(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("props", "legs", "rows", "graded", "items"):
        v = data.get(key)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    for v in data.values():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            if "player" in v[0] or "prop" in v[0] or "sport" in v[0]:
                return v
    return []


def _l5(row: dict, side: str) -> float | None:
    keys = (
        ("l5_over", "last5_over", "L5 Over", "line_hits_over_5")
        if side == "OVER"
        else ("l5_under", "last5_under", "L5 Under", "line_hits_under_5")
    )
    for k in keys:
        v = _finite(row.get(k))
        if v is not None and v <= 10:
            return v
    return None


def _l10(row: dict, side: str) -> float | None:
    keys = (
        ("l10_over", "last10_over", "L10 Over", "line_hits_over_10")
        if side == "OVER"
        else ("l10_under", "last10_under", "L10 Under", "line_hits_under_10")
    )
    for k in keys:
        v = _finite(row.get(k))
        if v is not None and v <= 10:
            return v
    return None


def _std_discount(row: dict, side: str) -> float | None:
    line = _finite(row.get("line"))
    std = None
    for k in ("standard_line", "Standard Line", "std_line"):
        std = _finite(row.get(k))
        if std is not None:
            break
    if line is None or std is None:
        return None
    return (line - std) if side == "UNDER" else (std - line)


def _l5_gap(row: dict) -> float | None:
    for k in ("dist_l5", "Dist_L5"):
        v = _finite(row.get(k))
        if v is not None:
            return v
    avg = None
    for k in ("stat_last5_avg", "Last 5 Avg", "last5_avg", "l5_avg"):
        avg = _finite(row.get(k))
        if avg is not None:
            break
    line = _finite(row.get("line"))
    if avg is None or line is None:
        return None
    return avg - line


def _rate(label: str, subset: list[dict]) -> dict:
    n = len(subset)
    h = sum(int(l["hit"]) for l in subset)
    lo, hi = _wilson(h, n)
    return {"label": label, "n": n, "hits": h, "hit_pct": _pct(h, n), "ci95": [lo, hi]}


def _by_field(subset: list[dict], field: str, order: tuple[str, ...] | None = None) -> list[dict]:
    buckets: dict[str, list[dict]] = {}
    for l in subset:
        key = str(l.get(field) or "Unknown") or "Unknown"
        buckets.setdefault(key, []).append(l)
    keys = list(order) if order else sorted(buckets.keys())
    for k in buckets:
        if k not in keys:
            keys.append(k)
    out = []
    for k in keys:
        rows = buckets.get(k) or []
        if not rows and order:
            continue
        r = _rate(k, rows)
        r["bucket"] = k
        out.append(r)
    return out


def _by_gap(subset: list[dict]) -> list[dict]:
    out = []
    for label, pred in GAP_BANDS:
        rows = [l for l in subset if pred(l.get("rank_gap"))]
        r = _rate(label, rows)
        r["bucket"] = label
        out.append(r)
    return out


def _ta_match_ranks(
    ta_index: dict[str, list[dict]],
    player: str,
    slate: date,
    *,
    max_abs_days: int = 3,
) -> tuple[int | None, int | None, str | None]:
    """Ranks from the TA match nearest to slate (true matchup orank)."""
    rows = ta_index.get(norm_key(player)) or []
    best = None
    best_abs = None
    for rec in rows:
        dt = parse_ta_date(rec.get("date"))
        if dt is None:
            continue
        abs_d = abs((dt - slate).days)
        if abs_d > max_abs_days:
            continue
        if best_abs is None or abs_d < best_abs or (abs_d == best_abs and dt > best[0]):
            best = (dt, rec)
            best_abs = abs_d
    if best is None:
        return None, None, None
    rec = best[1]
    pr = parse_int_rank(rec.get("rank"))
    ork = parse_int_rank(rec.get("opp_rank"))
    return pr, ork, str(rec.get("opp") or "") or None


def main() -> int:
    print("Loading Sackmann + rank index + TA…")
    matches = ensure_sackmann_matches()
    index = build_sackmann_player_index(matches)
    rank_idx = TennisOppRankIndex.from_repo(_REPO)
    ta_index = load_cached_ta_player_index()
    print(f"  hands… rank_players={len(rank_idx.by_pk)} ta_players={len(ta_index)}")

    hand_by: dict[str, str] = {}
    for _, rd in matches.iterrows():
        for name_col, hand_col in (("winner_name", "winner_hand"), ("loser_name", "loser_hand")):
            nm = norm_key(str(rd.get(name_col) or ""))
            hd = str(rd.get(hand_col) or "").strip().upper()
            if nm and hd in {"L", "R", "U"}:
                hand_by[nm] = hd
    print(f"  hands={len(hand_by)}")

    seen: set[str] = set()
    legs: list[dict] = []
    seen_names: set[str] = set()
    paths: list[Path] = []
    for d in GRADED_DIRS:
        if not d.exists():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if p.name in seen_names:
                continue
            seen_names.add(p.name)
            paths.append(p)

    for path in paths:
        rows = _iter_props(path)
        m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        if not m:
            continue
        slate = date.fromisoformat(m.group(1))
        for row in rows:
            if str(row.get("sport") or "").strip().lower() != "tennis":
                continue
            result = str(row.get("result") or "").strip().upper()
            if result not in ("HIT", "MISS"):
                continue
            book = _book(row)
            if not book:
                continue
            player = str(row.get("player") or "").strip()
            prop_raw = str(row.get("prop") or row.get("prop_type") or "").strip()
            if not player or not prop_raw:
                continue
            pnorm = norm_tennis_prop(prop_raw)
            side = "OVER" if "OVER" in book else "UNDER"
            line = _finite(row.get("line"))
            pk = norm_key(player)
            dkey = f"{slate}|{pk}|{pnorm}|{book}|{line}|{result}"
            if dkey in seen:
                continue
            seen.add(dkey)

            prior: list[dict] = []
            for rec in index.get(pk) or []:
                dt = parse_sackmann_tourney_date(rec.get("date"))
                if dt and dt < slate:
                    prior.append(rec)
            sw_vals = [_finite(r.get("second_won_pct")) for r in prior[:5]]
            sw = _mean([v for v in sw_vals if v is not None])

            opp_name = norm_key(str(row.get("opp_team") or row.get("opponent") or row.get("opp") or ""))
            today_hand = hand_by.get(opp_name) if opp_name else None
            last_hand = str((prior[0].get("opp_hand") if prior else "") or "").strip().upper() or None
            if last_hand and last_hand not in {"L", "R", "U"}:
                last_hand = None
            opp_hand = today_hand or last_hand
            lefty = True if opp_hand == "L" else (False if opp_hand in {"R", "U"} else None)

            row_pr, row_or = row_rank_fields(row)
            player_rank, opp_rank = row_pr, row_or
            rank_src = "row" if (player_rank is not None or opp_rank is not None) else None
            opp_display = str(row.get("opp_team") or row.get("opponent") or row.get("opp") or "").strip()

            # TA same-match ranks (covers summer keep legs; Sackmann ends ~May).
            if player_rank is None or opp_rank is None:
                ta_pr, ta_or, ta_opp = _ta_match_ranks(ta_index, player, slate)
                if player_rank is None and ta_pr is not None:
                    player_rank = ta_pr
                    rank_src = "ta_match"
                if opp_rank is None and ta_or is not None:
                    opp_rank = ta_or
                    rank_src = "ta_match"
                if not opp_display and ta_opp:
                    opp_display = ta_opp

            if player_rank is None or opp_rank is None:
                rec = rank_idx.lookup_entering_ranks(player, opp_display, slate, max_age_days=180)
                if player_rank is None and rec.player_rank is not None:
                    player_rank = rec.player_rank
                    rank_src = rank_src or "sackmann_last"
                if opp_rank is None and rec.opp_rank is not None:
                    opp_rank = rec.opp_rank
                    rank_src = rank_src or "sackmann_last"

            rank_gap = None
            if player_rank is not None and opp_rank is not None:
                rank_gap = opp_rank - player_rank  # >0 = player higher-ranked

            legs.append(
                {
                    "book": book,
                    "prop": pnorm,
                    "hit": 1 if result == "HIT" else 0,
                    "l5": _l5(row, side),
                    "l10": _l10(row, side),
                    "std_disc": _std_discount(row, side),
                    "l5_gap": _l5_gap(row),
                    "sw": sw,
                    "lefty": lefty,
                    "player_rank": player_rank,
                    "opp_rank": opp_rank,
                    "player_tier": rank_to_tier(player_rank) or "Unknown",
                    "opp_tier": rank_to_tier(opp_rank) or "Unknown",
                    "rank_gap": rank_gap,
                    "rank_src": rank_src,
                }
            )

    print(f"legs={len(legs)}")
    sw_thr = GAMES_WON_L5_SECOND_WON_MIN

    def gw_keep(l: dict) -> bool:
        if l["book"] != "Goblin OVER" or l["prop"] != "games_won":
            return False
        if l["std_disc"] is not None and l["std_disc"] >= GAMES_WON_STD_DISCOUNT_MIN:
            return True
        if l["l10"] is None or l["l10"] < 8:
            return False
        return l["lefty"] is True or (l["sw"] is not None and l["sw"] >= sw_thr)

    def tg_keep(l: dict) -> bool:
        if l["book"] != "Goblin OVER" or l["prop"] != "match_total_games":
            return False
        if l["l10"] is None or l["l10"] < 8:
            return False
        return (l["l5"] is not None and l["l5"] >= 4) or l["lefty"] is True

    def so_gw_keep(l: dict) -> bool:
        if l["book"] != "Standard OVER" or l["prop"] != "games_won":
            return False
        if l["l10"] is None or l["l10"] < 8:
            return False
        if l["l5_gap"] is not None and l["l5_gap"] >= GAMES_WON_STD_OVER_L5_GAP_MIN:
            return True
        return l["sw"] is not None and l["sw"] >= sw_thr

    gates: list[tuple[str, list[dict]]] = [
        ("Goblin OVER Games Won · KEEP union", [l for l in legs if gw_keep(l)]),
        (
            "Goblin OVER Games Won · L10≥8 + (lefty|2nd-won)",
            [
                l
                for l in legs
                if l["book"] == "Goblin OVER"
                and l["prop"] == "games_won"
                and l["l10"] is not None
                and l["l10"] >= 8
                and (l["lefty"] is True or (l["sw"] is not None and l["sw"] >= sw_thr))
            ],
        ),
        (
            "Goblin OVER Games Won · L10≥8 + 2nd-won",
            [
                l
                for l in legs
                if l["book"] == "Goblin OVER"
                and l["prop"] == "games_won"
                and l["l10"] is not None
                and l["l10"] >= 8
                and l["sw"] is not None
                and l["sw"] >= sw_thr
            ],
        ),
        (
            "Goblin OVER Games Won · L10≥8 + lefty",
            [
                l
                for l in legs
                if l["book"] == "Goblin OVER"
                and l["prop"] == "games_won"
                and l["l10"] is not None
                and l["l10"] >= 8
                and l["lefty"] is True
            ],
        ),
        ("Goblin OVER Total Games · KEEP union", [l for l in legs if tg_keep(l)]),
        (
            "Goblin OVER Total Games · L5≥4 + L10≥8",
            [
                l
                for l in legs
                if l["book"] == "Goblin OVER"
                and l["prop"] == "match_total_games"
                and l["l10"] is not None
                and l["l10"] >= 8
                and l["l5"] is not None
                and l["l5"] >= 4
            ],
        ),
        ("Standard OVER Games Won · KEEP", [l for l in legs if so_gw_keep(l)]),
        (
            "Goblin OVER Games Won · ungated",
            [l for l in legs if l["book"] == "Goblin OVER" and l["prop"] == "games_won"],
        ),
        (
            "Goblin OVER Total Games · ungated",
            [l for l in legs if l["book"] == "Goblin OVER" and l["prop"] == "match_total_games"],
        ),
    ]

    # Favorable / fade rank stacks on top of each gate
    stack_preds = (
        ("+ player Elite|Above", lambda l: l["player_tier"] in ("Elite", "Above Avg")),
        ("+ player Below|Weak", lambda l: l["player_tier"] in ("Below Avg", "Weak")),
        ("+ opp Weak|Below (D-style OVER)", lambda l: l["opp_tier"] in ("Weak", "Below Avg")),
        ("+ opp Elite|Above (hard D)", lambda l: l["opp_tier"] in ("Elite", "Above Avg")),
        ("+ player better (gap≥1)", lambda l: l.get("rank_gap") is not None and l["rank_gap"] >= 1),
        ("+ player better 50+", lambda l: l.get("rank_gap") is not None and l["rank_gap"] >= 50),
        ("+ opp better (gap≤-1)", lambda l: l.get("rank_gap") is not None and l["rank_gap"] <= -1),
        ("+ both ranks known", lambda l: l.get("player_rank") is not None and l.get("opp_rank") is not None),
    )

    gate_reports = []
    for label, subset in gates:
        base = _rate(label, subset)
        known_p = sum(1 for l in subset if l.get("player_rank") is not None)
        known_o = sum(1 for l in subset if l.get("opp_rank") is not None)
        known_both = sum(1 for l in subset if l.get("player_rank") is not None and l.get("opp_rank") is not None)
        stacks = []
        for s_label, pred in stack_preds:
            sub = [l for l in subset if pred(l)]
            r = _rate(f"{label} {s_label}", sub)
            r["stack"] = s_label
            r["lift_pp"] = (
                round((r["hit_pct"] or 0) - (base["hit_pct"] or 0), 1) if r["n"] and base["hit_pct"] is not None else None
            )
            stacks.append(r)
        gate_reports.append(
            {
                "gate": label,
                "base": base,
                "rank_coverage": {
                    "player_rank_n": known_p,
                    "opp_rank_n": known_o,
                    "both_n": known_both,
                    "pct_both": _pct(known_both, len(subset)),
                },
                "by_player_tier": _by_field(subset, "player_tier", TIER_ORDER),
                "by_opp_tier": _by_field(subset, "opp_tier", TIER_ORDER),
                "by_rank_gap": _by_gap(subset),
                "stacks": stacks,
            }
        )
        print(f"\n{label}: {base['hit_pct']}% n={base['n']} (ranks both={known_both})")
        for row in _by_field(subset, "player_tier", TIER_ORDER):
            if row["n"]:
                print(f"  player {row['bucket']:12} {row['hit_pct']:5}% n={row['n']:4}")
        for row in _by_field(subset, "opp_tier", TIER_ORDER):
            if row["n"]:
                print(f"  opp    {row['bucket']:12} {row['hit_pct']:5}% n={row['n']:4}")
        for row in stacks:
            if row["n"] >= 15:
                print(f"  stack  {row['stack']:32} {row['hit_pct']:5}% n={row['n']:4} lift={row['lift_pp']:+}pp" if row["lift_pp"] is not None else f"  stack  {row['stack']}")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "legs": len(legs),
        "sw_thr": sw_thr,
        "rank_map": {
            "Elite": "1-10",
            "Above Avg": "11-25",
            "Avg": "26-50",
            "Below Avg": "51-100",
            "Weak": "101+",
            "rank_gap": "opp_rank - player_rank (>0 = player higher-ranked / better)",
        },
        "notes": [
            "Ranks: graded row fields → TA same-match (rank/orank within ±3d of slate) → "
            "Sackmann lookup_entering_ranks (match window or last-known, max 180d).",
            "Graded tennis often blanks opp_team; TA orank is the summer opp-rank source.",
            "Tiers match tennis_shared.atp_wta_def_tier / D mapping.",
            "Stacks are exploratory — not wired into tennis_keep_gates.py yet.",
        ],
        "gates": gate_reports,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nWrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
