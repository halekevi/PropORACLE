"""Four L5=5 x O/D soft splits on unique-game soft-attach rows."""
from __future__ import annotations

import pandas as pd

from utils.nfl_prop_gate_ledger import rate

u = pd.read_csv(r"H:\PropORACLE\Sports\NFL\data\nfl_od_soft_attach_week1_2.csv")
u["hit"] = u["result"].astype(str).str.upper().eq("HIT")
u["wopr_tier"] = pd.Series(pd.NA, index=u.index, dtype="object")
u.loc[u["wopr"] >= 0.50, "wopr_tier"] = "high (>=0.50)"
u.loc[u["wopr"] < 0.50, "wopr_tier"] = "low (<0.50)"
u.loc[u["wopr"].isna(), "wopr_tier"] = "missing"


def split(title: str, mask: pd.Series, col: str) -> None:
    g = u.loc[mask & (u["l5"] == 5)].copy()
    print(f"\n### {title}  (L5=5 n={len(g)})")
    if g.empty:
        print("  (no rows)")
        return
    rows = []
    for tier, sg in g.groupby(col, dropna=False):
        h = int(sg["hit"].sum())
        n = len(sg)
        rows.append((str(tier), h, n, rate(h, n)))
    rows.sort(key=lambda r: (-(r[3] or -1), -r[2]))
    print(f"  {'tier':22s}  hits/n        hr")
    for tier, h, n, hr in rows:
        print(f"  {tier:22s}  {h:3d}/{n:<3d} = {hr:5.1f}%")
    # Elite/Above vs Weak/Below rollups when col is a defense tier
    if "secondary" in col or "rush" in col:
        elite = {"Elite", "Above Avg", "Above"}
        weak = {"Weak", "Below Avg", "Below"}
        for label, s in [("Elite/Above", elite), ("Weak/Below", weak), ("Avg", {"Avg"})]:
            sg = g[g[col].isin(s)]
            h = int(sg["hit"].sum())
            n = len(sg)
            if n:
                print(f"  [{label:12s}]         {h:3d}/{n:<3d} = {rate(h, n):5.1f}%")


# 1 Receptions UNDER x opp secondary
split(
    "1. Receptions UNDER x opp_secondary_tier",
    (u.prop == "receptions") & (u.side == "UNDER") & (u.pick == "Standard"),
    "opp_secondary_tier",
)

# 2 Longest rec UNDER x opp secondary
split(
    "2. Longest reception UNDER x opp_secondary_tier",
    (u.prop == "longest_reception") & (u.side == "UNDER") & (u.pick == "Standard"),
    "opp_secondary_tier",
)

# 3 Rush yards UNDER x opp rush D
split(
    "3. Rushing yards UNDER x opp_rush_d_tier",
    (u.prop == "rushing_yards") & (u.side == "UNDER") & (u.pick == "Standard"),
    "opp_rush_d_tier",
)

# 4 Goblin rec yards OVER x wopr_tier
split(
    "4. Receiving yards Goblin OVER x wopr_tier",
    (u.prop == "receiving_yards") & (u.side == "OVER") & (u.pick == "Goblin"),
    "wopr_tier",
)

# Extra context user called out: rec yards UNDER x secondary (key finding)
split(
    "EXTRA. Receiving yards UNDER x opp_secondary_tier",
    (u.prop == "receiving_yards") & (u.side == "UNDER") & (u.pick == "Standard"),
    "opp_secondary_tier",
)
