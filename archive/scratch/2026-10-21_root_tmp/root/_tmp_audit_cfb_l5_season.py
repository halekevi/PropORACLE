#!/usr/bin/env python3
"""Audit CFB L5 cross-season padding on 2026-09-19 board."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-19/cfb")
# Prefer step5b if present (has stat_g*); else step8
p5 = ROOT / "step5b_with_stats_cfb.csv"
p8 = ROOT / "step8_cfb_direction_clean.xlsx"
open_p5 = Path(r"H:/PropORACLE/outputs/2026-09-19/cfb/step5b_with_stats_cfb.csv")

for p in (p5, open_p5, p8):
    print("exists", p.exists(), p)

if open_p5.exists():
    df = pd.read_csv(open_p5, low_memory=False)
elif p5.exists():
    df = pd.read_csv(p5, low_memory=False)
else:
    df = pd.read_excel(p8)
    df.columns = [str(c).strip() for c in df.columns]

print("cols sample", [c for c in df.columns if any(x in str(c).lower() for x in ("stat_g", "l5", "player", "prop", "line", "game"))][:40])
print("rows", len(df))

# Jones
pc = next(c for c in df.columns if str(c).lower() in ("player", "Player"))
jones = df[df[pc].astype(str).str.contains("Tavorus Jones|Jones", case=False, na=False)]
jones = jones[jones[pc].astype(str).str.contains("Tavorus|Jones", case=False, na=False)]
gcols = [c for c in df.columns if str(c).lower().startswith("stat_g")]
print("\nJONES rows", len(jones))
if len(jones):
    prop_c = next((c for c in df.columns if "prop" in str(c).lower()), None)
    sub = jones
    if prop_c:
        sub = jones[jones[prop_c].astype(str).str.contains("Rush", case=False, na=False)]
    cols = [pc] + ([prop_c] if prop_c else []) + ["line" if "line" in df.columns else "Line"] + gcols[:10]
    cols = [c for c in cols if c in df.columns]
    print(sub[cols].head(8).to_string(index=False))
