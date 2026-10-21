import json
from pathlib import Path
import pandas as pd

# G70 pool from dated build
g=json.loads(Path(r"H:/PropORACLE/data/reports/goblin70_tickets_2026-09-25.json").read_text(encoding="utf-8"))
pool=g.get("goblin_pool") or []
print("g70 pool", len(pool))
l5eq5=[]
for r in pool:
    l5=r.get("l5_over") if str(r.get("direction") or "OVER").upper()=="OVER" else r.get("l5_under")
    try: l5=int(float(l5))
    except: l5=0
    if l5>=5:
        l5eq5.append(r)
print("L5=5 in g70 pool", len(l5eq5))
for r in l5eq5:
    print(f"  {r.get('sport'):7} {r.get('player'):22} {r.get('prop'):22} O{r.get('line')} L5={r.get('l5_over')} L10={r.get('l10_over')} D={r.get('def_tier') or r.get('Def Tier')}")

# Also check step8 MLB for goblin over L5=5 keep props
p=Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/mlb/step8_mlb_direction_clean.xlsx")
if not p.exists():
    p=Path(r"H:/PropORACLE/outputs/2026-09-25/mlb/step8_mlb_direction_clean.xlsx")
df=pd.read_excel(p)
# normalize cols
cols={c.lower().replace(" ", "_"): c for c in df.columns}
def col(*names):
    for n in names:
        if n in df.columns: return n
        if n.lower() in cols: return cols[n.lower()]
    return None
c_pt=col("Pick Type","pick_type")
c_dir=col("Direction","direction")
c_l5o=col("L5 Over","l5_over","last5_over")
c_l5u=col("L5 Under","l5_under")
c_player=col("Player","player")
c_prop=col("Prop","prop")
c_line=col("Line","line")
c_team=col("Team","team")
sub=df[df[c_pt].astype(str).str.contains("Goblin", case=False, na=False)].copy()
sub=sub[sub[c_dir].astype(str).str.upper()=="OVER"]
sub["_l5"]=pd.to_numeric(sub[c_l5o], errors="coerce").fillna(0)
eq5=sub[sub["_l5"]>=5].copy()
print("\nstep8 MLB Goblin OVER L5>=5:", len(eq5))
# show unique player/prop/line
show=eq5[[c_player,c_prop,c_line,"_l5"]].drop_duplicates()
print(show.to_string(index=False))
