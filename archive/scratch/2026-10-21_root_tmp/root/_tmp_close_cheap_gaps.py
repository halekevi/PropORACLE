#!/usr/bin/env python3
"""Pull cheap Soccer/Tennis catalog cells + MLB board vs catalog coverage."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pandas as pd

_REPO = Path(r"H:/PropORACLE")
MAIN = _REPO.parent / "PropORACLE_main_cp"
cat = json.loads(
    (_REPO / "data/reports/all_ranked_prop_hit_rates.json").read_text(encoding="utf-8")
)


def hr_pct(r: dict) -> float:
    h = float(r["hr"])
    return h * 100 if h <= 1.0 else h


def fmt(r: dict) -> str:
    return f"{hr_pct(r):.1f}% ({r['hits']}/{r['n']})"


print("=== CHEAP ADDITIONS (catalog) ===")
for sport, props in [
    ("Soccer", ("tackles", "crosses")),
    ("Tennis", ("total_sets", "total_tie_breaks")),
]:
    for r in sorted(
        [x for x in cat["rows"] if x["sport"] == sport and x["prop"] in props],
        key=lambda x: (x["prop"], -x["n"]),
    ):
        print(
            f"  {sport:8} {r['book']:18} {r['prop_display']:22} "
            f"{fmt(r):20} listed={r.get('listed_hr')} l5eq5={r.get('l5eq5_hr')}"
        )

# MLB board from freshest step1
candidates = [
    MAIN / "Sports/MLB/step1_mlb_props.csv",
    _REPO / "Sports/MLB/step1_mlb_props.csv",
]
# also outputs today
for root in (MAIN, _REPO):
    for p in sorted((root / "outputs").glob("*/mlb/step1*.csv") if (root / "outputs").exists() else []):
        candidates.append(p)
    for p in sorted((root / "outputs").glob("*/MLB/step1*.csv") if (root / "outputs").exists() else []):
        candidates.append(p)

step1 = None
best_mtime = -1.0
for p in candidates:
    if p.is_file():
        m = p.stat().st_mtime
        if m > best_mtime:
            best_mtime = m
            step1 = p

print(f"\n=== MLB BOARD SOURCE ===\n  {step1}")
if step1 is None:
    raise SystemExit("no step1")

df = pd.read_csv(step1, low_memory=False)
# find prop column
prop_col = None
for c in ("prop_type", "prop", "Prop", "stat_type", "prop_name"):
    if c in df.columns:
        prop_col = c
        break
print("  cols sample:", list(df.columns)[:20])
print("  prop_col:", prop_col, "rows:", len(df))
if "fetched_at" in df.columns:
    print("  fetched_at sample:", df["fetched_at"].dropna().astype(str).head(3).tolist())

board_props = sorted({str(x).strip() for x in df[prop_col].dropna().unique() if str(x).strip()})
print(f"  unique board props: {len(board_props)}")
for p in board_props:
    print(f"    - {p}")

# Catalog MLB live-book props (Goblin/Std)
mlb_cat = [
    r
    for r in cat["rows"]
    if r["sport"] == "MLB" and r["book"] in ("Goblin OVER", "Standard OVER", "Standard UNDER")
]
cat_displays = sorted({r["prop_display"] for r in mlb_cat})
cat_by_disp: dict[str, list] = {}
for r in mlb_cat:
    cat_by_disp.setdefault(r["prop_display"], []).append(r)

# normalize for match
import re


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())


cat_norm = {norm(d): d for d in cat_displays}
board_norm = {norm(p): p for p in board_props}

matched = []
board_only = []
for bn, bp in sorted(board_norm.items()):
    if bn in cat_norm:
        matched.append((bp, cat_norm[bn]))
    else:
        # fuzzy: substring
        hits = [cd for cn, cd in cat_norm.items() if bn in cn or cn in bn]
        if hits:
            matched.append((bp, hits[0] + " (fuzzy)"))
        else:
            board_only.append(bp)

cat_only = []
for cn, cd in sorted(cat_norm.items()):
    if cn not in board_norm and not any(cn in bn or bn in cn for bn in board_norm):
        # only list if live book has n>=20
        cells = cat_by_disp[cd]
        nmax = max(c["n"] for c in cells)
        if nmax >= 40:
            cat_only.append((cd, nmax))

print(f"\n=== MLB BOARD vs CATALOG ===")
print(f"  matched: {len(matched)} / {len(board_props)}")
print(f"  board-only (no catalog rate): {len(board_only)}")
for p in board_only:
    print(f"    BOARD ONLY: {p}")
print(f"  catalog live props not on this board (n>=40): {len(cat_only)}")
for cd, n in sorted(cat_only, key=lambda x: -x[1])[:25]:
    print(f"    CAT ONLY: {cd} (max n={n})")

print("\n=== MATCHED with Goblin OVER ungated when present ===")
gob = {(r["prop_display"]): r for r in mlb_cat if r["book"] == "Goblin OVER"}
for bp, cd in matched:
    base = cd.replace(" (fuzzy)", "")
    r = gob.get(base)
    if r:
        print(f"  {bp:35} -> Goblin {fmt(r)}")
    else:
        cells = [x for x in mlb_cat if x["prop_display"] == base]
        if cells:
            best = max(cells, key=lambda x: x["n"])
            print(f"  {bp:35} -> {best['book']} {fmt(best)}")
        else:
            print(f"  {bp:35} -> matched {cd} (no live cell?)")
