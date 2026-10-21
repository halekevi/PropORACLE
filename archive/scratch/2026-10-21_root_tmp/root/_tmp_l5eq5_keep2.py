import pandas as pd
from pathlib import Path
import sys
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO)]
from utils.mlb_keep_gates import mlb_goblin_keep_eligible, KEEP_PROPS
from utils.prop_norm import canon_prop

p = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/mlb/step8_mlb_direction_clean.xlsx")
df = pd.read_excel(p)
print("cols sample:", [c for c in df.columns if any(x in str(c).lower() for x in ("pitch","bat","def","own","opp","ba","k%","tier","l5","l10"))])
# try one row
sub=df[(df["Pick Type"].astype(str).str.contains("Goblin",case=False)) & (df["Direction"].astype(str).str.upper()=="OVER")]
sub=sub[pd.to_numeric(sub["L5 Over"], errors="coerce")>=5]
print("L5=5 goblin over", len(sub))
ok=0
miss_reasons={}
for _, r in sub.iterrows():
    rec={
        "sport":"MLB",
        "side":"OVER",
        "pick_type":"Goblin",
        "player":r["Player"],
        "prop":r["Prop"],
        "line":r["Line"],
        "l5_over":float(r["L5 Over"]),
        "l10_over":float(pd.to_numeric(r.get("L10 Over"), errors="coerce") or 0),
        "def_tier": r.get("Def Tier"),
        "team": r.get("Team"),
        "opp": r.get("Opp"),
    }
    # attach all cols lower
    for c in df.columns:
        rec[str(c)] = r[c]
        rec[str(c).lower().replace(" ","_")] = r[c]
    prop=canon_prop("MLB", str(r["Prop"]))
    if prop not in KEEP_PROPS:
        miss_reasons[f"prop:{prop}"]=miss_reasons.get(f"prop:{prop}",0)+1
        continue
    if mlb_goblin_keep_eligible(rec):
        ok+=1
        if ok<=25:
            print(f"KEEP L5=5 {r['Player']:22} {r['Prop']:22} {r['Line']} L10={rec['l10_over']}")
    else:
        miss_reasons[f"gate:{prop}"]=miss_reasons.get(f"gate:{prop}",0)+1
print("ok", ok, "miss", miss_reasons)
