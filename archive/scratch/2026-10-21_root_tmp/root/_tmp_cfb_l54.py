import pandas as pd
from pathlib import Path
import sys
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO)]
from utils.cfb_keep_gates import cfb_list_eligible

df = pd.read_excel(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx")
df["_l5o"]=pd.to_numeric(df["L5 Over"], errors="coerce").fillna(0)
df["_l5u"]=pd.to_numeric(df["L5 Under"], errors="coerce").fillna(0)
df["_l10o"]=pd.to_numeric(df["L10 Over"], errors="coerce").fillna(0)
df["_l10u"]=pd.to_numeric(df["L10 Under"], errors="coerce").fillna(0)
df["_dir"]=df["Direction"].astype(str).str.upper()
df["_dl5"]=df.apply(lambda r: r["_l5o"] if r["_dir"]=="OVER" else r["_l5u"], axis=1)
df["_dl10"]=df.apply(lambda r: r["_l10o"] if r["_dir"]=="OVER" else r["_l10u"], axis=1)
eq4 = df[df["_dl5"]>=4].copy()
print(f"Nearest: L5>=4 faces ({len(eq4)}) — none reach L5=5\n")
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
    rows.append((ok,r))

rows.sort(key=lambda x: (-x[1]["_dl5"], -x[1]["_dl10"], str(x[1]["Player"])))
print(f"{'list':4} {'type':8} {'dir':5} {'player':22} {'prop':18} {'line':6} L5 L10 D  matchup")
for ok,r in rows:
    print(f"{'Y' if ok else 'n':4} {str(r['Pick Type'])[:8]:8} {r['_dir']:5} {str(r['Player'])[:22]:22} {str(r['Prop'])[:18]:18} {r['Line']!s:6} {int(r['_dl5']):2} {int(r['_dl10']):3}  {str(r.get('Def Tier') or '-'):10} {r.get('Team')} vs {r.get('Opp')}")
