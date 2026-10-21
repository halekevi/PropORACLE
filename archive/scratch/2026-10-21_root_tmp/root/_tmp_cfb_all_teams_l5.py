import pandas as pd
from pathlib import Path
import sys
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO)]
from utils.cfb_keep_gates import cfb_list_eligible

df = pd.read_excel(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx")
print("rows", len(df), "teams", df["Team"].nunique(), "games times", df["Game Time"].astype(str).str.slice(0,10).value_counts().to_dict())
print("unique teams", sorted(df["Team"].astype(str).str.upper().unique())[:80], "... total", df["Team"].nunique())

df["_l5o"]=pd.to_numeric(df["L5 Over"], errors="coerce").fillna(0)
df["_l5u"]=pd.to_numeric(df["L5 Under"], errors="coerce").fillna(0)
df["_l10o"]=pd.to_numeric(df["L10 Over"], errors="coerce").fillna(0)
df["_l10u"]=pd.to_numeric(df["L10 Under"], errors="coerce").fillna(0)
df["_dir"]=df["Direction"].astype(str).str.upper()
df["_dl5"]=df.apply(lambda r: r["_l5o"] if r["_dir"]=="OVER" else r["_l5u"], axis=1)
df["_dl10"]=df.apply(lambda r: r["_l10o"] if r["_dir"]=="OVER" else r["_l10u"], axis=1)
df["_pt"]=df["Pick Type"].astype(str)

print("\ndl5 dist", df["_dl5"].value_counts().sort_index().to_dict())
print("L5=5", int((df["_dl5"]>=5).sum()), "L5>=4", int((df["_dl5"]>=4).sum()), "L5>=3", int((df["_dl5"]>=3).sum()))

# L5>=4 by team
eq4 = df[df["_dl5"]>=4].copy()
print("\nL5>=4 by team:")
print(eq4.groupby(eq4["Team"].astype(str).str.upper()).size().sort_values(ascending=False).to_string())

# list-eligible L5>=4 with team diversity view
rows=[]
for _, r in eq4.iterrows():
    rec={
        "sport":"CFB","player":r["Player"],"prop":r["Prop"],"prop_type":r["Prop"],
        "pick_type":str(r["Pick Type"]),"direction":r["_dir"],"side":r["_dir"],
        "line":r["Line"],"l5_over":r["_l5o"],"l5_under":r["_l5u"],
        "l10_over":r["_l10o"],"l10_under":r["_l10u"],"def_tier":r.get("Def Tier"),
        "team":r.get("Team"),"opp":r.get("Opp"),
    }
    for c in df.columns: rec[str(c)]=r[c]
    ok=False
    try: ok=bool(cfb_list_eligible(rec))
    except Exception: pass
    if ok:
        rows.append(r)

print(f"\nlist-eligible L5>=4: {len(rows)}")
for r in sorted(rows, key=lambda x: (-x["_dl5"], str(x["Team"]), str(x["Player"]))):
    print(f"  L5={int(r['_dl5'])} L10={int(r['_dl10'])} {str(r['Pick Type'])[:8]:8} {r['_dir']:5} {str(r['Team']):5} {str(r['Player'])[:22]:22} {str(r['Prop'])[:18]:18} {r['Line']} vs {r.get('Opp')} D={r.get('Def Tier')}")

# Also show best non-USC L5=3 goblin overs (nearest for 2-team mix)
print("\n=== Best non-USC Goblin OVER by dl5 (top 40) ===")
gob = df[(df["_pt"].str.contains("Goblin", case=False)) & (df["_dir"]=="OVER")].copy()
gob = gob[~gob["Team"].astype(str).str.upper().isin(["USC"])]
gob = gob.sort_values(["_dl5","_dl10"], ascending=False)
seen=set()
n=0
for _, r in gob.iterrows():
    key=(str(r["Player"]), str(r["Prop"]), float(r["Line"]))
    if key in seen: continue
    seen.add(key)
    if r["_dl5"] < 3: break
    print(f"  L5={int(r['_dl5'])} L10={int(r['_dl10'])} {str(r['Team']):5} {str(r['Player'])[:22]:22} {str(r['Prop'])[:18]:18} O{r['Line']} vs {r.get('Opp')} D={r.get('Def Tier')} flag={r.get('Season L5 Flag')}")
    n+=1
    if n>=40: break

# Oregon specifically
print("\n=== ORE / Oregon faces L5>=3 ===")
ore = df[df["Team"].astype(str).str.upper().isin(["ORE","OREGON"]) | df["Opp"].astype(str).str.upper().isin(["ORE","OREGON","USC"])]
ore3 = ore[ore["_dl5"]>=3]
print("ORE team rows L5>=3", len(ore[ore["Team"].astype(str).str.upper().isin(["ORE","OREGON"]) & (ore["_dl5"]>=3)]))
for _, r in ore[(ore["Team"].astype(str).str.upper().isin(["ORE","OREGON"])) & (ore["_dl5"]>=3)].sort_values("_dl5", ascending=False).head(25).iterrows():
    print(f"  L5={int(r['_dl5'])} {r['Pick Type']} {r['_dir']} {r['Player']} {r['Prop']} {r['Line']}")
