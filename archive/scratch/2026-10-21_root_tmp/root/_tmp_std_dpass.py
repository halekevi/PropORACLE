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


def d_ok(r):
    t = str(r["tier"]).lower()
    if r["side"].startswith("O"):
        return "weak" in t or "below" in t
    if r["side"].startswith("U"):
        return "elite" in t or "above" in t
    return False


ok = d[d.apply(d_ok, axis=1)].sort_values("l5n", ascending=False)
print("D-pass Standard L5>=4 n=", len(ok))
for _, r in ok.head(30).iterrows():
    print(
        f"{r['Player']:24} {r['Prop']:22} {r['side'][0]}{r['Line']}  "
        f"L5 {r['L5 Over']}/{r['L5 Under']}  L10 {r['L10 Over']}/{r['L10 Under']}  "
        f"Def {r['tier']}  proj {r.get('Projection')}  {r['Team']} vs {r['Opp']}"
    )

# WNBA D
wnba = pd.read_excel(
    r"H:/PropORACLE_main_cp/outputs/2026-09-19/wnba/step8_wnba_direction_clean.xlsx"
)
wnba.columns = [str(c).strip() for c in wnba.columns]
print("\nWNBA Def Tier cols", [c for c in wnba.columns if "Def" in c or "Tier" in c][:15])
w = wnba[wnba["Pick Type"].astype(str).str.lower().eq("standard")].copy()
w["side"] = w["Direction"].astype(str).str.upper()
if "L5 Over" in w.columns:
    w["l5"] = w.apply(
        lambda r: r["L5 Over"] if r["side"].startswith("O") else r["L5 Under"], axis=1
    )
    w["l5n"] = pd.to_numeric(w["l5"], errors="coerce")
    w = w[w["l5n"] >= 4]
tier_col = next((c for c in w.columns if c in ("Def Tier", "Opp Def Tier", "DEF_TIER")), None)
print("tier_col", tier_col, "n", len(w))
if tier_col:
    for _, r in w.head(20).iterrows():
        print(
            r["Player"],
            r["Prop"],
            r["side"][0],
            r["Line"],
            "L5",
            r["L5 Over"],
            r["L5 Under"],
            "Def",
            r.get(tier_col),
        )
