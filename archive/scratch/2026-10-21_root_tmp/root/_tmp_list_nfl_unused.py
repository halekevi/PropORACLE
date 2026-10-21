#!/usr/bin/env python3
import json
from pathlib import Path
import pandas as pd
import sys

sys.path[:0] = [r"H:\PropORACLE", r"H:\PropORACLE\scripts"]
from utils.prop_norm import canon_prop

used = set()
for p in Path(r"H:\PropORACLE\data\reports").glob("nfl_pp_tabs_2026-09-27*.json"):
    d = json.loads(p.read_text(encoding="utf-8"))
    blocks = list(d.get("results") or [])
    if "result" in d:
        blocks.append(d["result"])
    for block in blocks:
        if not block or not block.get("ready"):
            continue
        for row in block.get("legs") or []:
            used.add(row[0] if isinstance(row, (list, tuple)) else row.get("player"))
print("used", sorted(used))

STEP8 = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-27/nfl/step8_nfl_direction_clean.xlsx")
if not STEP8.exists():
    STEP8 = Path(r"H:/PropORACLE/outputs/2026-09-27/nfl/step8_nfl_direction_clean.xlsx")
df = pd.read_excel(STEP8)
LABEL = {
    "passing_yards": "Pass Yards",
    "rushing_yards": "Rush Yards",
    "receiving_yards": "Rec Yards",
    "receptions": "Recs",
}
l5 = pd.to_numeric(df["L5 Over"], errors="coerce")
l10 = pd.to_numeric(df["L10 Over"], errors="coerce")
n = 0
for i, r in df.iterrows():
    if str(r.get("Direction") or "").upper() != "OVER":
        continue
    if "goblin" not in str(r.get("Pick Type") or "").lower():
        continue
    a = float(l5.loc[i]) if pd.notna(l5.loc[i]) else 0.0
    b = float(l10.loc[i]) if pd.notna(l10.loc[i]) else 0.0
    if a < 5 or b < 8:
        continue
    prop = canon_prop("NFL", str(r.get("Prop") or ""))
    if prop not in LABEL:
        continue
    od = str(r.get("Def Tier") or "").lower()
    if "weak" not in od and "below" not in od:
        continue
    pl = str(r["Player"])
    if "+" in pl or pl in used:
        continue
    print(
        f"{pl:22} {LABEL[prop]:12} {float(r['Line'])} L10={b:.0f} "
        f"D={r.get('Def Tier')} ({str(r.get('Team') or '').upper()})"
    )
    n += 1
    if n >= 30:
        break
print("extra", n)
