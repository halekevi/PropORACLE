import pandas as pd
from pathlib import Path
from collections import Counter

p = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/cfb/step8_cfb_direction_clean.xlsx")
df = pd.read_excel(p)
print("step8 rows", len(df))
# game times / dates
gt = df.get("Game Time")
if gt is not None:
    s = gt.astype(str)
    print("game time sample", s.head(8).tolist())
    # extract date part
    dates = s.str.slice(0,10)
    print("by date", dates.value_counts().to_dict())

# teams
print("teams", sorted(df["Team"].astype(str).str.upper().unique())[:40], "n", df["Team"].nunique())

df["l5o"]=pd.to_numeric(df.get("L5 Over"), errors="coerce").fillna(0)
df["l5u"]=pd.to_numeric(df.get("L5 Under"), errors="coerce").fillna(0)
df["dir"]=df["Direction"].astype(str).str.upper()
df["dl5"]=df.apply(lambda r: r["l5o"] if r["dir"]=="OVER" else r["l5u"], axis=1)
df["gn"]=pd.to_numeric(df.get("L5 Sample N"), errors="coerce")
df["flag"]=df.get("Season L5 Flag").astype(str)
print("dl5>=4", int((df["dl5"]>=4).sum()), "dl5=5", int((df["dl5"]>=5).sum()))
print("l5n dist", df["gn"].value_counts().sort_index().to_dict())
print("flags", df["flag"].value_counts().head(10).to_dict())

# also step1 fetched_at and game dates for tomorrow
s1=Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/cfb/step1_cfb.csv")
d1=pd.read_csv(s1, nrows=5)
print("step1 cols", list(d1.columns)[:25])
d1full=pd.read_csv(s1, low_memory=False)
print("step1 rows", len(d1full))
for c in d1full.columns:
    cl=c.lower()
    if "fetch" in cl or "start" in cl or "time" in cl or "date" in cl or "game" in cl:
        if d1full[c].notna().any():
            print(c, "sample", d1full[c].astype(str).head(3).tolist())
