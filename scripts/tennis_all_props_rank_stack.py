#!/usr/bin/env python3
"""All tennis props × Goblin / Std OVER / Std UNDER with L5/L10/factor gates + ranks.

For every book×prop cell: ungated base, universal recency gates, factor gates,
prop-specific keep paths, and player/opp rank stacks on each gated subset.

  py -3.14 scripts/tennis_all_props_rank_stack.py
"""
from __future__ import annotations

import json
import math
import re
import sys
from collections import Counter
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

OUT = _REPO / "data" / "reports" / "tennis_all_props_rank_stack.json"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]
TIER_ORDER = ("Elite", "Above Avg", "Avg", "Below Avg", "Weak", "Unknown")
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
SW_THR = GAMES_WON_L5_SECOND_WON_MIN
MIN_GATE_N = 10
MIN_PRINT_N = 15

GAP_BANDS = (
    ("player_better_50plus", lambda g: g is not None and g >= 50),
    ("player_better_1_49", lambda g: g is not None and 1 <= g < 50),
    ("near_even_pm20", lambda g: g is not None and -20 <= g <= 20),
    ("opp_better_1_49", lambda g: g is not None and -49 <= g <= -1),
    ("opp_better_50plus", lambda g: g is not None and g <= -50),
    ("unknown_gap", lambda g: g is None),
)
RANK_STACKS = (
    ("player Elite|Above", lambda l: l["player_tier"] in ("Elite", "Above Avg")),
    ("player Below|Weak", lambda l: l["player_tier"] in ("Below Avg", "Weak")),
    ("opp Weak|Below", lambda l: l["opp_tier"] in ("Weak", "Below Avg")),
    ("opp Elite|Above", lambda l: l["opp_tier"] in ("Elite", "Above Avg")),
    ("player better gap>=1", lambda l: l.get("rank_gap") is not None and l["rank_gap"] >= 1),
    ("player better 50+", lambda l: l.get("rank_gap") is not None and l["rank_gap"] >= 50),
    ("opp better gap<=-1", lambda l: l.get("rank_gap") is not None and l["rank_gap"] <= -1),
    ("both ranks known", lambda l: l.get("player_rank") is not None and l.get("opp_rank") is not None),
)


def finite(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def pct(h: int, n: int) -> float | None:
    return round(100.0 * h / n, 1) if n else None


def wilson(h: int, n: int, z: float = 1.96) -> list[float | None]:
    if n <= 0:
        return [None, None]
    p = h / n
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    half = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / den
    return [round(100.0 * (center - half), 1), round(100.0 * (center + half), 1)]


def book(row: dict) -> str | None:
    pt = str(row.get("pick_type") or row.get("book") or "").strip().lower()
    d = str(row.get("direction") or row.get("over_under") or "").strip().upper()
    if "demon" in pt:
        return None
    if "goblin" in pt:
        return f"Goblin {d if d in ('OVER', 'UNDER') else 'OVER'}"
    if d in ("OVER", "UNDER"):
        return f"Standard {d}"
    return None


def iter_props(path: Path) -> list[dict]:
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


def l5(row: dict, side: str) -> float | None:
    keys = (
        ("l5_over", "last5_over", "L5 Over", "line_hits_over_5")
        if side == "OVER"
        else ("l5_under", "last5_under", "L5 Under", "line_hits_under_5")
    )
    for k in keys:
        v = finite(row.get(k))
        if v is not None and v <= 10:
            return v
    return None


def l10(row: dict, side: str) -> float | None:
    keys = (
        ("l10_over", "last10_over", "L10 Over", "line_hits_over_10")
        if side == "OVER"
        else ("l10_under", "last10_under", "L10 Under", "line_hits_under_10")
    )
    for k in keys:
        v = finite(row.get(k))
        if v is not None and v <= 10:
            return v
    return None


def std_discount(row: dict, side: str) -> float | None:
    line = finite(row.get("line"))
    std = None
    for k in ("standard_line", "Standard Line", "std_line"):
        std = finite(row.get(k))
        if std is not None:
            break
    if line is None or std is None:
        return None
    return (line - std) if side == "UNDER" else (std - line)


def l5_gap(row: dict) -> float | None:
    for k in ("dist_l5", "Dist_L5"):
        v = finite(row.get(k))
        if v is not None:
            return v
    avg = None
    for k in ("stat_last5_avg", "Last 5 Avg", "last5_avg", "l5_avg"):
        avg = finite(row.get(k))
        if avg is not None:
            break
    line = finite(row.get("line"))
    if avg is None or line is None:
        return None
    return avg - line


def rate(label: str, subset: list[dict]) -> dict:
    n = len(subset)
    h = sum(int(l["hit"]) for l in subset)
    lo, hi = wilson(h, n)
    return {"label": label, "n": n, "hits": h, "hit_pct": pct(h, n), "ci95": [lo, hi]}


def by_field(subset: list[dict], field: str, order: tuple[str, ...] | None = None) -> list[dict]:
    buckets: dict[str, list[dict]] = {}
    for l in subset:
        key = str(l.get(field) or "Unknown") or "Unknown"
        buckets.setdefault(key, []).append(l)
    keys = list(order) if order else sorted(buckets)
    for k in buckets:
        if k not in keys:
            keys.append(k)
    out = []
    for k in keys:
        rows = buckets.get(k) or []
        if not rows:
            continue
        r = rate(k, rows)
        r["bucket"] = k
        out.append(r)
    return out


def by_gap(subset: list[dict]) -> list[dict]:
    out = []
    for label, pred in GAP_BANDS:
        rows = [l for l in subset if pred(l.get("rank_gap"))]
        r = rate(label, rows)
        r["bucket"] = label
        out.append(r)
    return out


def rank_stacks(subset: list[dict], base_pct: float | None) -> list[dict]:
    out = []
    for s_label, pred in RANK_STACKS:
        sub = [l for l in subset if pred(l)]
        r = rate(s_label, sub)
        r["stack"] = s_label
        r["lift_pp"] = (
            round((r["hit_pct"] or 0) - (base_pct or 0), 1) if r["n"] and base_pct is not None else None
        )
        out.append(r)
    return out


def ta_match_ranks(ta_index: dict, player: str, slate: date, max_abs_days: int = 3):
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
    return parse_int_rank(rec.get("rank")), parse_int_rank(rec.get("opp_rank")), str(rec.get("opp") or "") or None


def gate_defs_for(prop: str, bk: str) -> list[tuple[str, object]]:
    """Universal + factor + prop-specific keep gates."""
    gates: list[tuple[str, object]] = [
        ("ungated", lambda l: True),
        ("L5>=4", lambda l: l["l5"] is not None and l["l5"] >= 4),
        ("L5=5", lambda l: l["l5"] is not None and l["l5"] == 5),
        ("L10>=8", lambda l: l["l10"] is not None and l["l10"] >= 8),
        ("L10=10", lambda l: l["l10"] is not None and l["l10"] == 10),
        (
            "L5>=4 + L10>=8",
            lambda l: l["l5"] is not None and l["l5"] >= 4 and l["l10"] is not None and l["l10"] >= 8,
        ),
        (
            "L5=5 + L10>=8",
            lambda l: l["l5"] is not None and l["l5"] == 5 and l["l10"] is not None and l["l10"] >= 8,
        ),
        ("vs lefty", lambda l: l["lefty"] is True),
        (f"L5 2nd-won%≥{SW_THR:g}", lambda l: l["sw"] is not None and l["sw"] >= SW_THR),
        (
            f"L10≥8 + lefty",
            lambda l: l["l10"] is not None and l["l10"] >= 8 and l["lefty"] is True,
        ),
        (
            f"L10≥8 + 2nd-won≥{SW_THR:g}",
            lambda l: l["l10"] is not None
            and l["l10"] >= 8
            and l["sw"] is not None
            and l["sw"] >= SW_THR,
        ),
        (
            f"L10≥8 + (lefty|2nd-won≥{SW_THR:g})",
            lambda l: l["l10"] is not None
            and l["l10"] >= 8
            and (l["lefty"] is True or (l["sw"] is not None and l["sw"] >= SW_THR)),
        ),
    ]

    if prop == "games_won" and bk == "Goblin OVER":
        gates.extend(
            [
                (
                    f"KEEP Std−Goblin≥{GAMES_WON_STD_DISCOUNT_MIN:g}",
                    lambda l: l["std_disc"] is not None and l["std_disc"] >= GAMES_WON_STD_DISCOUNT_MIN,
                ),
                (
                    f"KEEP union (gap≥{GAMES_WON_STD_DISCOUNT_MIN:g}|L10≥8+(lefty|2nd-won))",
                    lambda l: (l["std_disc"] is not None and l["std_disc"] >= GAMES_WON_STD_DISCOUNT_MIN)
                    or (
                        l["l10"] is not None
                        and l["l10"] >= 8
                        and (l["lefty"] is True or (l["sw"] is not None and l["sw"] >= SW_THR))
                    ),
                ),
            ]
        )
    if prop == "match_total_games" and bk == "Goblin OVER":
        gates.append(
            (
                "KEEP union (L5≥4+L10≥8 | L10≥8+lefty)",
                lambda l: l["l10"] is not None
                and l["l10"] >= 8
                and ((l["l5"] is not None and l["l5"] >= 4) or l["lefty"] is True),
            )
        )
    if prop == "games_won" and bk == "Standard OVER":
        gates.append(
            (
                f"KEEP L10≥8+(gap≥{GAMES_WON_STD_OVER_L5_GAP_MIN:g}|2nd-won)",
                lambda l: l["l10"] is not None
                and l["l10"] >= 8
                and (
                    (l["l5_gap"] is not None and l["l5_gap"] >= GAMES_WON_STD_OVER_L5_GAP_MIN)
                    or (l["sw"] is not None and l["sw"] >= SW_THR)
                ),
            )
        )
        gates.append(
            (
                f"L10≥8 + L5gap≥{GAMES_WON_STD_OVER_L5_GAP_MIN:g}",
                lambda l: l["l10"] is not None
                and l["l10"] >= 8
                and l["l5_gap"] is not None
                and l["l5_gap"] >= GAMES_WON_STD_OVER_L5_GAP_MIN,
            )
        )
    return gates


def annotate_gate(subset: list[dict], gate_label: str, ungated_pct: float | None) -> dict:
    base = rate(gate_label, subset)
    base["gate"] = gate_label
    base["lift_vs_ungated_pp"] = (
        round((base["hit_pct"] or 0) - (ungated_pct or 0), 1)
        if base["n"] and ungated_pct is not None
        else None
    )
    both = sum(1 for l in subset if l.get("player_rank") is not None and l.get("opp_rank") is not None)
    return {
        "gate": gate_label,
        "base": base,
        "rank_coverage": {
            "player_n": sum(1 for l in subset if l.get("player_rank") is not None),
            "opp_n": sum(1 for l in subset if l.get("opp_rank") is not None),
            "both_n": both,
            "pct_both": pct(both, len(subset)),
        },
        "by_player_tier": by_field(subset, "player_tier", TIER_ORDER),
        "by_opp_tier": by_field(subset, "opp_tier", TIER_ORDER),
        "by_rank_gap": by_gap(subset),
        "rank_stacks": rank_stacks(subset, base["hit_pct"]),
    }


def main() -> int:
    print("Loading Sackmann + ranks + TA…")
    matches = ensure_sackmann_matches()
    index = build_sackmann_player_index(matches)
    rank_idx = TennisOppRankIndex.from_repo(_REPO)
    ta_index = load_cached_ta_player_index()

    hand_by: dict[str, str] = {}
    for _, rd in matches.iterrows():
        for name_col, hand_col in (("winner_name", "winner_hand"), ("loser_name", "loser_hand")):
            nm = norm_key(str(rd.get(name_col) or ""))
            hd = str(rd.get(hand_col) or "").strip().upper()
            if nm and hd in {"L", "R", "U"}:
                hand_by[nm] = hd
    print(f"  hands={len(hand_by)} sackmann={len(rank_idx.by_pk)} ta={len(ta_index)}")

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
        rows = iter_props(path)
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
            bk = book(row)
            if not bk:
                continue
            player = str(row.get("player") or "").strip()
            prop_raw = str(row.get("prop") or row.get("prop_type") or "").strip()
            if not player or not prop_raw:
                continue
            pnorm = norm_tennis_prop(prop_raw)
            side = "OVER" if "OVER" in bk else "UNDER"
            line = finite(row.get("line"))
            pk = norm_key(player)
            dkey = f"{slate}|{pk}|{pnorm}|{bk}|{line}|{result}"
            if dkey in seen:
                continue
            seen.add(dkey)

            prior: list[dict] = []
            for rec in index.get(pk) or []:
                dt = parse_sackmann_tourney_date(rec.get("date"))
                if dt and dt < slate:
                    prior.append(rec)
            sw_vals = [finite(r.get("second_won_pct")) for r in prior[:5]]
            sw = mean([v for v in sw_vals if v is not None])

            opp_name = norm_key(str(row.get("opp_team") or row.get("opponent") or row.get("opp") or ""))
            today_hand = hand_by.get(opp_name) if opp_name else None
            last_hand = str((prior[0].get("opp_hand") if prior else "") or "").strip().upper() or None
            if last_hand and last_hand not in {"L", "R", "U"}:
                last_hand = None
            opp_hand = today_hand or last_hand
            lefty = True if opp_hand == "L" else (False if opp_hand in {"R", "U"} else None)

            row_pr, row_or = row_rank_fields(row)
            player_rank, opp_rank = row_pr, row_or
            opp_display = str(row.get("opp_team") or row.get("opponent") or row.get("opp") or "").strip()
            if player_rank is None or opp_rank is None:
                ta_pr, ta_or, ta_opp = ta_match_ranks(ta_index, player, slate)
                if player_rank is None and ta_pr is not None:
                    player_rank = ta_pr
                if opp_rank is None and ta_or is not None:
                    opp_rank = ta_or
                if not opp_display and ta_opp:
                    opp_display = ta_opp
            if player_rank is None or opp_rank is None:
                rec = rank_idx.lookup_entering_ranks(player, opp_display, slate, max_age_days=180)
                if player_rank is None:
                    player_rank = rec.player_rank
                if opp_rank is None:
                    opp_rank = rec.opp_rank
            rank_gap = None
            if player_rank is not None and opp_rank is not None:
                rank_gap = opp_rank - player_rank

            legs.append(
                {
                    "book": bk,
                    "prop": pnorm,
                    "hit": 1 if result == "HIT" else 0,
                    "l5": l5(row, side),
                    "l10": l10(row, side),
                    "std_disc": std_discount(row, side),
                    "l5_gap": l5_gap(row),
                    "sw": sw,
                    "lefty": lefty,
                    "player_rank": player_rank,
                    "opp_rank": opp_rank,
                    "player_tier": rank_to_tier(player_rank) or "Unknown",
                    "opp_tier": rank_to_tier(opp_rank) or "Unknown",
                    "rank_gap": rank_gap,
                }
            )

    print(f"legs={len(legs)}")
    prop_n = Counter(l["prop"] for l in legs)
    props = [p for p, n in prop_n.most_common() if n >= 10]
    print("props", [(p, prop_n[p]) for p in props])

    cells = []
    for prop in props:
        for bk in BOOKS:
            subset = [l for l in legs if l["prop"] == prop and l["book"] == bk]
            if len(subset) < MIN_GATE_N:
                continue
            ungated = rate("ungated", subset)
            gate_rows = []
            for g_label, pred in gate_defs_for(prop, bk):
                g_sub = [l for l in subset if pred(l)]
                if len(g_sub) < MIN_GATE_N and g_label != "ungated":
                    gate_rows.append(
                        {
                            "gate": g_label,
                            "base": rate(g_label, g_sub),
                            "skipped_thin": True,
                        }
                    )
                    continue
                gate_rows.append(annotate_gate(g_sub, g_label, ungated["hit_pct"]))

            cells.append(
                {
                    "prop": prop,
                    "book": bk,
                    "ungated": ungated,
                    "gates": gate_rows,
                }
            )

    rollups = [rate(bk, [l for l in legs if l["book"] == bk]) for bk in BOOKS]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "legs": len(legs),
        "sw_thr": SW_THR,
        "method": (
            "graded_props HIT/MISS; Demons excluded; "
            "gates = L5/L10 + lefty/2nd-won + prop KEEP paths; "
            "ranks = row → TA ±3d → Sackmann entering"
        ),
        "rank_map": {
            "Elite": "1-10",
            "Above Avg": "11-25",
            "Avg": "26-50",
            "Below Avg": "51-100",
            "Weak": "101+",
            "rank_gap": "opp_rank - player_rank (>0 = player higher-ranked)",
        },
        "book_rollups": rollups,
        "cells": cells,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {OUT} cells={len(cells)}")

    print("\n=== BOOK ROLLUPS ===")
    for r in rollups:
        print(f"  {r['label']:16} {r['hit_pct']:5}% n={r['n']:6}")

    print("\n=== GATES (n>=15) + notable rank stacks (|lift|>=5 n>=20) ===")
    for c in cells:
        print(f"\n## {c['book']} · {c['prop']}  ungated {c['ungated']['hit_pct']}% n={c['ungated']['n']}")
        for g in c["gates"]:
            if g.get("skipped_thin"):
                b = g["base"]
                if b["n"]:
                    print(f"  {g['gate']:48} {b['hit_pct']}% n={b['n']} (thin)")
                continue
            b = g["base"]
            if b["n"] < MIN_PRINT_N:
                continue
            lift = b.get("lift_vs_ungated_pp")
            lift_s = f" lift={lift:+}pp" if lift is not None else ""
            print(f"  {g['gate']:48} {b['hit_pct']:5}% n={b['n']:4}{lift_s}")
            for s in g.get("rank_stacks") or []:
                if s["n"] >= 20 and s.get("lift_pp") is not None and abs(s["lift_pp"]) >= 5:
                    print(
                        f"      rank {s['stack']:28} {s['hit_pct']:5}% n={s['n']:4} "
                        f"vs_gate={s['lift_pp']:+}pp"
                    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
