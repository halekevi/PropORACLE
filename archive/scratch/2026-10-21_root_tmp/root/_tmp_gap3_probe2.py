#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO / "scripts"), str(_REPO)]
import prop_hit_tiers as T  # noqa: E402
from utils.fantasy_prop_filter import is_fantasy_prop_label  # noqa: E402

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

ctr: Counter = Counter()
for path in by_date.values():
    raw = json.loads(path.read_text(encoding="utf-8"))
    for r in raw.get("props") or raw.get("rows") or []:
        if not isinstance(r, dict):
            continue
        sport = T.norm_sport(str(r.get("sport") or ""))
        prop = str(r.get("prop") or r.get("prop_type") or "")
        low = prop.lower()
        res = str(r.get("result") or "").strip().upper() or "?"
        keep = False
        if sport == "Soccer" and "fantasy" in low:
            keep = True
        if sport == "Tennis" and (
            "break" in low or "fantasy" in low or "set" in low or "tie" in low
        ):
            keep = True
        if sport == "WNBA" and "fantasy" in low:
            keep = True
        if "break point" in low or "breakpoint" in low:
            keep = True
        if keep:
            ctr[(sport, prop, res)] += 1

print("Fantasy / Sets / Tie / Break result breakdown:")
for (sport, prop, res), n in sorted(ctr.items(), key=lambda x: (-x[1], x[0])):
    drop = "DROP" if is_fantasy_prop_label(prop) else ""
    print(f"  {sport:8} {prop!r:42} {res:6} n={n:5} {drop}")

cat = json.loads(
    (_REPO / "data/reports/all_ranked_prop_hit_rates.json").read_text(encoding="utf-8")
)
print("\nLive-book catalog cells (Goblin/Std) for gap props:")
for r in cat["rows"]:
    if r["sport"] in ("Soccer", "Tennis") and r["prop"] in (
        "tackles",
        "crosses",
        "total_sets",
        "total_tie_breaks",
        "break_points_won",
    ):
        if r["book"] in ("Goblin OVER", "Standard OVER", "Standard UNDER"):
            hr = 100.0 * float(r["hr"]) if r["hr"] is not None else 0
            # hr in file might already be 0-1 or percent - check
            if float(r["hr"]) <= 1.0:
                hr = 100.0 * float(r["hr"])
            else:
                hr = float(r["hr"])
            print(
                f"  {r['sport']:8} {r['book']:18} {r['prop_display']:25} "
                f"n={r['n']:4} hr={hr:.1f}%"
            )

print("\nDemon-only catalog cells for same props:")
for r in cat["rows"]:
    if r["sport"] in ("Soccer", "Tennis") and r["prop"] in (
        "tackles",
        "crosses",
        "total_sets",
        "total_tie_breaks",
        "break_points_won",
    ):
        if "Demon" in r["book"]:
            hr = float(r["hr"])
            if hr <= 1.0:
                hr *= 100
            print(
                f"  {r['sport']:8} {r['book']:18} {r['prop_display']:25} "
                f"n={r['n']:4} hr={hr:.1f}%"
            )
