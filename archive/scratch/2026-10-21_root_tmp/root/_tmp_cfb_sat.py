import pandas as pd
from pathlib import Path
d1=pd.read_csv(r"H:/PropORACLE_main_cp/outputs/2026-09-25/cfb/step1_cfb.csv", low_memory=False)
d1["day"]=d1["start_time"].astype(str).str.slice(0,10)
print("step1 by start day", d1["day"].value_counts().head(10).to_dict())
# saturday sample teams
sat=d1[d1["day"]=="2026-09-26"]
print("sat rows", len(sat), "players", sat["player"].nunique() if len(sat) else 0)
if len(sat):
    print("sat teams", sorted(sat["team"].astype(str).str.upper().unique())[:30])
    print("sat games sample", sat[["player","team","stat_type","line","start_time"]].head(8).to_string(index=False))
# also check 9/27
for day in ["2026-09-26","2026-09-27","2026-09-28"]:
    print(day, (d1["day"]==day).sum())

# the one L5>=4 face on friday step8
s8=pd.read_excel(r"H:/PropORACLE_main_cp/outputs/2026-09-25/cfb/step8_cfb_direction_clean.xlsx")
s8["l5o"]=pd.to_numeric(s8.get("L5 Over"), errors="coerce").fillna(0)
s8["l5u"]=pd.to_numeric(s8.get("L5 Under"), errors="coerce").fillna(0)
s8["dir"]=s8["Direction"].astype(str).str.upper()
s8["dl5"]=s8.apply(lambda r: r["l5o"] if r["dir"]=="OVER" else r["l5u"], axis=1)
top=s8.nlargest(15, "dl5")[["Player","Team","Opp","Prop","Pick Type","Direction","Line","dl5","L5 Sample N","Season L5 Flag","Def Tier"]]
print(top.to_string(index=False))
