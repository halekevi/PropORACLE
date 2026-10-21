#!/usr/bin/env python3
import pandas as pd

cfb = pd.read_excel(
    r"H:/PropORACLE_main_cp/outputs/2026-09-19/cfb/step8_cfb_direction_clean.xlsx"
)
cfb.columns = [str(c).strip() for c in cfb.columns]
d = cfb[cfb["Pick Type"].astype(str).str.lower().eq("standard")].copy()
d = d[
    ~d["Prop"]
    .astype(str)
    .str.contains("Longest|Completion %|First |YPC", case=False, na=False)
]
fade = d["Prop"].astype(str).str.contains("Receiv", case=False) & d[
    "Direction"
].astype(str).str.upper().str.startswith("U")
d = d[~fade]
d["tier"] = d["Def Tier"].astype(str)
d["side"] = d["Direction"].astype(str).str.upper()
d["l5"] = d.apply(
    lambda r: r["L5 Over"] if r["side"].startswith("O") else r["L5 Under"], axis=1
)
d["l5n"] = pd.to_numeric(d["l5"], errors="coerce")
d = d[d["l5n"] >= 4]


def dok(r):
    t = str(r["tier"]).lower()
    if r["side"].startswith("O"):
        return "weak" in t or "below" in t
    return "elite" in t or "above" in t


ok = d[d.apply(dok, axis=1)].copy()
if "Pos" in ok.columns:
    ok = ok[~ok["Pos"].astype(str).str.upper().isin(["QB"])]
ok = ok[
    ~ok["Player"]
    .astype(str)
    .str.contains("Kukuk|Dampier|Fifita|Jones|Overton|Hiedeman", case=False, na=False)
]
print("--- non-QB D-pass L5>=4 (excl current ticket) ---")
for _, r in ok.sort_values("l5n", ascending=False).head(18).iterrows():
    print(
        f"{str(r.get('Pos','?')):3} {r['Player']:24} {r['Prop']:22} "
        f"{r['side'][0]}{r['Line']}  L5 {r['L5 Over']}/{r['L5 Under']}  "
        f"Def {r['tier']}  proj {r.get('Projection')}"
    )
