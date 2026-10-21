import pandas as pd
from pathlib import Path
base=Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/cfb")
for name in ["step2_cfb.csv","step5b_cfb.csv"]:
    p=base/name
    df=pd.read_csv(p, low_memory=False, nrows=0)
    cols=list(df.columns)
    time_cols=[c for c in cols if any(x in c.lower() for x in ("start","time","date","game"))]
    print(name, "timecols", time_cols[:8])
    full=pd.read_csv(p, low_memory=False, usecols=lambda c: c in set(time_cols+["player","Player","team","Team"]+[c for c in cols if "start" in c.lower()][:1]))
    # find start col
    sc=next((c for c in full.columns if "start" in c.lower()), None)
    if sc:
        days=full[sc].astype(str).str.slice(0,10)
        print(name, "rows", len(full), "by day", days.value_counts().head(5).to_dict())
