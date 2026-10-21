#!/usr/bin/env python3
from __future__ import annotations

import pandas as pd
from pathlib import Path

df = pd.read_excel(
    Path(r"H:/PropORACLE_main_cp/outputs/2026-09-20/nfl/step8_nfl_direction_clean.xlsx")
)
cols = {c.lower().replace(" ", "_"): c for c in df.columns}


def C(*names: str) -> str | None:
    for n in names:
        for k, v in cols.items():
            if n == k or n in k:
                return v
    return None


pt = C("pick_type")
prop = C("prop")
player = C("player")
line = C("line")
direc = C("direction")
l5o = C("l5_over")
l5u = C("l5_under")
dcol = C("def_tier") or C("def")
team = C("team")
opp = C("opp")
proj = C("projection")
hr = C("hit_rate_l5") or C("hit_rate_(5g)")
junk = (
    "longest|first_|per_carry|completion_percentage|yards_on_first|"
    "halves_with|combo|fantasy|punt|fg_made|field_goal|shortest"
)


def core(frame: pd.DataFrame) -> pd.DataFrame:
    return frame[~frame[prop].astype(str).str.lower().str.contains(junk, regex=True)].copy()


g = df[
    df[pt].astype(str).str.contains("Goblin", case=False)
    & df[direc].astype(str).str.upper().isin(["OVER", "MORE", "O"])
]
g = g[pd.to_numeric(g[l5o], errors="coerce").fillna(0) >= 4]
g2 = core(g)
g2["_l5"] = pd.to_numeric(g2[l5o], errors="coerce")
g2["_line"] = pd.to_numeric(g2[line], errors="coerce")
g2["_proj"] = pd.to_numeric(g2[proj], errors="coerce")
g2["_cov"] = g2["_proj"] - g2["_line"]
g2["_d"] = g2[dcol].astype(str) if dcol else ""
# D pass for overs: Weak / Below
g2["_dpass"] = g2["_d"].str.contains("Weak|Below", case=False, na=False)

print(f"Goblin OVER L5>=4 core={len(g2)} (all props {len(g)})")
print("\n=== Goblin OVER L5=5 + D pass (Weak/Below) ===")
sub = g2[(g2["_l5"] >= 5) & g2["_dpass"]].sort_values(["_cov"], ascending=False)
for _, r in sub.head(25).iterrows():
    print(
        f"{r[player]:22} {r[prop]:18} O{r[line]}  "
        f"L5 {int(r['_l5'])} cov {r['_cov']:+.1f}  D={r['_d']}  {r[team]} vs {r[opp]}"
    )
print(f"(n={len(sub)})")

print("\n=== Goblin OVER L5=5 any D (Gold-path volume) ===")
sub = g2[g2["_l5"] >= 5].sort_values(["_dpass", "_cov"], ascending=[False, False])
for _, r in sub.head(30).iterrows():
    print(
        f"{r[player]:22} {r[prop]:18} O{r[line]}  "
        f"L5 {int(r['_l5'])} cov {r['_cov']:+.1f}  D={r['_d']}  {r[team]} vs {r[opp]}"
    )

print("\n=== Goblin OVER L5=4 + D pass ===")
sub = g2[(g2["_l5"] == 4) & g2["_dpass"]].sort_values(["_cov"], ascending=False)
for _, r in sub.head(20).iterrows():
    print(
        f"{r[player]:22} {r[prop]:18} O{r[line]}  "
        f"L5 4 cov {r['_cov']:+.1f}  D={r['_d']}  {r[team]} vs {r[opp]}"
    )

# Standards
s = df[df[pt].astype(str).str.contains("Standard", case=False)]
so = s[s[direc].astype(str).str.upper().isin(["OVER", "MORE", "O"])]
su = s[s[direc].astype(str).str.upper().isin(["UNDER", "LESS", "U"])]
so = so[pd.to_numeric(so[l5o], errors="coerce").fillna(0) >= 4]
su = su[pd.to_numeric(su[l5u], errors="coerce").fillna(0) >= 4]
so2 = core(so)
su2 = core(su)
so2["_l5"] = pd.to_numeric(so2[l5o], errors="coerce")
so2["_proj"] = pd.to_numeric(so2[proj], errors="coerce")
so2["_line"] = pd.to_numeric(so2[line], errors="coerce")
so2["_cov"] = so2["_proj"] - so2["_line"]
so2["_d"] = so2[dcol].astype(str) if dcol else ""
so2["_dpass"] = so2["_d"].str.contains("Weak|Below", case=False, na=False)
su2["_l5"] = pd.to_numeric(su2[l5u], errors="coerce")
su2["_proj"] = pd.to_numeric(su2[proj], errors="coerce")
su2["_line"] = pd.to_numeric(su2[line], errors="coerce")
su2["_cov"] = su2["_line"] - su2["_proj"]
su2["_d"] = su2[dcol].astype(str) if dcol else ""
su2["_dpass"] = su2["_d"].str.contains("Elite|Above", case=False, na=False)

print("\n=== Standard OVER L5>=4 + D pass (core) ===")
sub = so2[so2["_dpass"]].sort_values(["_l5", "_cov"], ascending=[False, False])
for _, r in sub.head(25).iterrows():
    print(
        f"{r[player]:22} {r[prop]:18} O{r[line]}  "
        f"L5 {int(r['_l5'])} cov {r['_cov']:+.1f}  D={r['_d']}  {r[team]} vs {r[opp]}"
    )
print(f"(n={len(sub)})")

print("\n=== Standard UNDER L5>=4 + D pass (core) ===")
sub = su2[su2["_dpass"]].sort_values(["_l5", "_cov"], ascending=[False, False])
for _, r in sub.head(25).iterrows():
    print(
        f"{r[player]:22} {r[prop]:18} U{r[line]}  "
        f"L5u {int(r['_l5'])} cov {r['_cov']:+.1f}  D={r['_d']}  {r[team]} vs {r[opp]}"
    )
print(f"(n={len(sub)})")

# sanity Shough
print("\n=== sanity: Shough rows ===")
sh = df[df[player].astype(str).str.contains("Shough", case=False)]
for _, r in sh.head(12).iterrows():
    print(
        r[pt],
        r[prop],
        r[line],
        r[direc],
        "L5o",
        r[l5o],
        "proj",
        r[proj],
        "D",
        r[dcol] if dcol else None,
    )
