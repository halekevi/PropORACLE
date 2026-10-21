#!/usr/bin/env python3
"""Cross-sport Combo prop VOID audit — same failure as MLB strikeouts_combo?"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO / "scripts"), str(_REPO)]
import prop_hit_tiers as T  # noqa: E402

dirs = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]
by_date: dict[str, Path] = {}
for d in dirs:
    if not d.is_dir():
        continue
    for p in sorted(d.glob("graded_props_20*.json")):
        if ".bak_" in p.name:
            continue
        date = p.stem.replace("graded_props_", "")[:10]
        if date not in by_date:
            by_date[date] = p

# (sport, raw_prop) -> result counts
res_ctr: dict[tuple[str, str], Counter] = defaultdict(Counter)
void_reason: dict[tuple[str, str], Counter] = defaultdict(Counter)
player_shape: dict[tuple[str, str], Counter] = defaultdict(Counter)
actual_shape: dict[tuple[str, str], Counter] = defaultdict(Counter)
samples: dict[tuple[str, str], list] = defaultdict(list)

PLUS_NAME = re.compile(r".+\s\+\s.+")


def act_bucket(v) -> str:
    if v is None or v == "":
        return "null"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "non_numeric"
    if f != f:
        return "nan"
    return "zero" if f == 0 else "nonzero"


n_combo_rows = 0
for path in by_date.values():
    raw = json.loads(path.read_text(encoding="utf-8"))
    for r in raw.get("props") or raw.get("rows") or []:
        if not isinstance(r, dict):
            continue
        prop = str(r.get("prop") or r.get("prop_type") or "").strip()
        if "(combo)" not in prop.lower() and not prop.lower().endswith(" combo"):
            # also catch "Points Combo" style without parens
            if "combo" not in prop.lower():
                continue
        sport = T.norm_sport(str(r.get("sport") or "")) or "?"
        res = str(r.get("result") or "").strip().upper() or "?"
        key = (sport, prop)
        res_ctr[key][res] += 1
        n_combo_rows += 1
        vr = str(r.get("void_reason") or r.get("void_note") or "").strip() or "(none)"
        if res == "VOID":
            void_reason[key][vr] += 1
        player = str(r.get("player") or r.get("player_name") or "")
        if PLUS_NAME.match(player):
            player_shape[key]["two_name_plus"] += 1
        elif player:
            player_shape[key]["single_name"] += 1
        else:
            player_shape[key]["blank_player"] += 1
        act = r.get("actual")
        if act is None:
            act = r.get("actual_value")
        actual_shape[key][act_bucket(act)] += 1
        if res == "VOID" and len(samples[key]) < 3:
            samples[key].append(
                {
                    "date": path.stem[-10:],
                    "player": player,
                    "void_reason": vr,
                    "actual": act,
                    "line": r.get("line") or r.get("line_score"),
                    "pick_type": r.get("pick_type"),
                }
            )

# Summarize
rows = []
for (sport, prop), ctr in res_ctr.items():
    total = sum(ctr.values())
    void = ctr.get("VOID", 0)
    hit = ctr.get("HIT", 0)
    miss = ctr.get("MISS", 0)
    decided = hit + miss
    two = player_shape[(sport, prop)].get("two_name_plus", 0)
    null_act = actual_shape[(sport, prop)].get("null", 0) + actual_shape[
        (sport, prop)
    ].get("nan", 0)
    top_void = void_reason[(sport, prop)].most_common(2)
    rows.append(
        {
            "sport": sport,
            "prop": prop,
            "n": total,
            "void": void,
            "void_pct": 100.0 * void / total if total else 0,
            "decided": decided,
            "hr": 100.0 * hit / decided if decided else None,
            "two_name_pct": 100.0 * two / total if total else 0,
            "null_actual_pct": 100.0 * null_act / total if total else 0,
            "top_void_reasons": top_void,
            "player_shape": dict(player_shape[(sport, prop)]),
            "actual_shape": dict(actual_shape[(sport, prop)]),
            "samples": samples[(sport, prop)],
        }
    )

rows.sort(key=lambda x: (-x["void_pct"], -x["n"]))

print(f"Combo-ish rows scanned: {n_combo_rows} across {len(rows)} sport×prop cells\n")
print("=== Ranked by VOID% ===")
for r in rows:
    flag = ""
    if r["void_pct"] >= 90 and r["two_name_pct"] >= 50:
        flag = " *** SAME_SHAPE_AS_KS_COMBO ***"
    elif r["void_pct"] >= 90 and r["null_actual_pct"] >= 90:
        flag = " *** HIGH_VOID_NULL_ACTUAL ***"
    elif r["decided"] >= 20 and r["void_pct"] < 30:
        flag = " [works]"
    print(
        f"{r['sport']:8} {r['prop']!r:42} n={r['n']:5} "
        f"void={r['void_pct']:5.1f}% decided={r['decided']:5} "
        f"two_name={r['two_name_pct']:5.1f}% null_act={r['null_actual_pct']:5.1f}% "
        f"void_reasons={r['top_void_reasons']}{flag}"
    )

same = [r for r in rows if r["void_pct"] >= 90 and r["two_name_pct"] >= 50]
works = [r for r in rows if r["decided"] >= 20 and r["void_pct"] < 30]
print(f"\nSame shape as pitcher Ks combo (VOID≥90% + two-name≥50%): {len(same)}")
for r in same:
    print(f"  {r['sport']} {r['prop']} n={r['n']}")
    for s in r["samples"][:2]:
        print(f"    sample: {s}")

print(f"\nCombo props that mostly WORK (decided≥20, VOID<30%): {len(works)}")
for r in works:
    print(
        f"  {r['sport']} {r['prop']} n={r['n']} void={r['void_pct']:.1f}% "
        f"hr={r['hr']:.1f}% two_name={r['two_name_pct']:.1f}%"
    )

out = {
    "audited": "2026-09-18",
    "question": "Do other Combo props share MLB strikeouts_combo NO_ACTUAL + A+B failure?",
    "n_combo_rows": n_combo_rows,
    "n_cells": len(rows),
    "same_shape_as_ks_combo": [
        {"sport": r["sport"], "prop": r["prop"], "n": r["n"], "void_pct": r["void_pct"]}
        for r in same
    ],
    "mostly_working": [
        {
            "sport": r["sport"],
            "prop": r["prop"],
            "n": r["n"],
            "void_pct": r["void_pct"],
            "hr": r["hr"],
            "two_name_pct": r["two_name_pct"],
        }
        for r in works
    ],
    "all_cells": rows,
}
Path(r"H:/PropORACLE/data/reports/audit_combo_props_void_cross_sport.json").write_text(
    json.dumps(out, indent=2) + "\n", encoding="utf-8"
)
print("\nwrote data/reports/audit_combo_props_void_cross_sport.json")
