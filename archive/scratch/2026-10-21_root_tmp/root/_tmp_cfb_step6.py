import pandas as pd
from pathlib import Path

# step6 ranked may have saturday
p=Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/cfb/step6_ranked_cfb.xlsx")
xl=pd.ExcelFile(p)
print("sheets", xl.sheet_names)
df=pd.read_excel(p, sheet_name=0)
print("step6 rows", len(df), "cols", len(df.columns))
# find date/time and L5 cols
cols=[c for c in df.columns if any(x in str(c).lower() for x in ("start","time","l5","over","under","player","prop","pick","line","dir","flag","sample","def"))]
print("useful", cols[:40])
sc=next((c for c in df.columns if "start" in str(c).lower()), None)
if sc:
    days=df[sc].astype(str).str.slice(0,10)
    print("by day", days.value_counts().to_dict())
    sat=df[days=="2026-09-26"].copy()
else:
    sat=df.copy()
print("sat rows", len(sat))

# try compute directional L5 from common names
def pick(*names):
    for n in names:
        if n in sat.columns: return n
    for c in sat.columns:
        for n in names:
            if str(c).lower().replace(" ","_")==n.lower().replace(" ","_"):
                return c
    return None

c_dir=pick("Direction","direction","side")
c_l5o=pick("L5 Over","l5_over","last5_over","Hit Rate L5")
c_l5u=pick("L5 Under","l5_under","last5_under")
c_pt=pick("Pick Type","pick_type","odds_type")
c_player=pick("Player","player")
c_prop=pick("Prop","prop","stat_type")
c_line=pick("Line","line")
c_team=pick("Team","team")
c_flag=pick("Season L5 Flag","season_l5_flag")
c_gn=pick("L5 Sample N","l5_sample_n","games_used")
print("mapped", dict(dir=c_dir,l5o=c_l5o,l5u=c_l5u,pt=c_pt,player=c_player,prop=c_prop,line=c_line,flag=c_flag,gn=c_gn))

if c_l5o and c_dir:
    sat=sat.copy()
    sat["_l5o"]=pd.to_numeric(sat[c_l5o], errors="coerce").fillna(0)
    sat["_l5u"]=pd.to_numeric(sat[c_l5u], errors="coerce").fillna(0) if c_l5u else 0
    sat["_dir"]=sat[c_dir].astype(str).str.upper()
    # if Hit Rate L5 is rate 0-1 not count, detect
    if sat["_l5o"].max()<=1.0 and sat["_l5o"].mean()<1:
        print("L5 looks like hit-rate fraction, not count — check step5b")
    sat["_dl5"]=sat.apply(lambda r: r["_l5o"] if "OVER" in r["_dir"] else r["_l5u"], axis=1)
    print("sat dl5>=4", int((sat["_dl5"]>=4).sum()), "dl5=5", int((sat["_dl5"]>=5).sum()), "max", sat["_dl5"].max())
    if c_gn:
        print("l5n", pd.to_numeric(sat[c_gn], errors="coerce").value_counts().sort_index().head(10).to_dict())
    if c_flag:
        print("flags", sat[c_flag].astype(str).value_counts().head(8).to_dict())
