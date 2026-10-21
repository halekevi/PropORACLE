"""Reprint soft O/D crosstabs as HITS/TOTAL so % is checkable."""
from __future__ import annotations

import pandas as pd

u = pd.read_csv(r"H:\PropORACLE\Sports\NFL\data\nfl_od_soft_attach_week1_2.csv")
ELITE = {"Elite", "Above Avg", "Above"}
WEAK = {"Weak", "Below Avg", "Below"}

u["opp_sec_elite"] = u["opp_secondary_tier"].isin(ELITE)
u["opp_rush_elite"] = u["opp_rush_d_tier"].isin(ELITE)
u["opp_rush_weak"] = u["opp_rush_d_tier"].isin(WEAK)
u["opp_ol_elite"] = u["opp_ol_tier"].isin(ELITE) if "opp_ol_tier" in u.columns else False
u["own_pr_weak"] = (
    u["own_pass_rush_tier"].isin(WEAK) if "own_pass_rush_tier" in u.columns else False
)
u["wopr_high"] = u["wopr"] >= 0.50
u["wopr_low"] = u["wopr"] < 0.50
u["air_low"] = u["opp_air_yards_low"].fillna(False).astype(bool)
u["own_rush_elite"] = (
    u["own_rush_off_tier"].isin(ELITE) if "own_rush_off_tier" in u.columns else False
)


def hits_total(g: pd.DataFrame) -> tuple[int, int, float]:
    n = len(g)
    h = int((g["result"].astype(str).str.upper() == "HIT").sum())
    pct = (100.0 * h / n) if n else float("nan")
    return h, n, pct


def show(title: str, mask: pd.Series, extras: list[tuple[str, pd.Series]]) -> None:
    base = u.loc[mask & (u["l5"] == 5)]
    h, n, pct = hits_total(base)
    print(f"\n### {title}")
    print(f"  {'L5=5':55s}  {h:3d}/{n:<3d} = {pct:5.1f}%")
    for label, extra in extras:
        g = u.loc[mask & (u["l5"] == 5) & extra]
        h2, n2, pct2 = hits_total(g)
        lift = (pct2 - pct) if n2 and n else float("nan")
        print(f"  {label:55s}  {h2:3d}/{n2:<3d} = {pct2:5.1f}%  (lift {lift:+.1f}pp)")


print("Format: HITS / DECIDED unique-game rows  (= HIT rate)")
print(f"File unique-game rows: {len(u)}")

show(
    "Sacks UNDER Standard",
    (u.prop == "sacks") & (u.side == "UNDER") & (u.pick == "Standard"),
    [
        ("+ opp OL Elite/Above", u["opp_ol_elite"]),
        ("+ own pass-rush Weak/Below", u["own_pr_weak"]),
        ("+ opp OL Elite OR own PR Weak", u["opp_ol_elite"] | u["own_pr_weak"]),
    ],
)

show(
    "Receptions UNDER Standard",
    (u.prop == "receptions") & (u.side == "UNDER") & (u.pick == "Standard"),
    [
        ("+ opp Sec Elite/Above", u["opp_sec_elite"]),
        ("+ Sec Elite + WOPR Low", u["opp_sec_elite"] & u["wopr_low"]),
        ("+ WOPR Low only", u["wopr_low"]),
    ],
)

show(
    "Longest reception UNDER Standard",
    (u.prop == "longest_reception") & (u.side == "UNDER") & (u.pick == "Standard"),
    [
        ("+ opp Sec Elite/Above", u["opp_sec_elite"]),
        ("+ air_yards_allowed < median", u["air_low"]),
        ("+ Sec Elite + air low", u["opp_sec_elite"] & u["air_low"]),
        ("+ opp Sec Weak/Below (anti)", u["opp_secondary_tier"].isin(WEAK)),
    ],
)

show(
    "Rushing yards UNDER Standard",
    (u.prop == "rushing_yards") & (u.side == "UNDER") & (u.pick == "Standard"),
    [
        ("+ opp Rush D Elite/Above", u["opp_rush_elite"]),
        ("+ opp Rush D Weak/Below (anti)", u["opp_rush_weak"]),
    ],
)

show(
    "Receiving yards Goblin OVER",
    (u.prop == "receiving_yards") & (u.side == "OVER") & (u.pick == "Goblin"),
    [
        ("+ WOPR>=0.50", u["wopr_high"]),
        ("+ WOPR<0.50 (anti)", u["wopr_low"]),
    ],
)

show(
    "Rushing yards Goblin OVER",
    (u.prop == "rushing_yards") & (u.side == "OVER") & (u.pick == "Goblin"),
    [
        ("+ opp Rush D Weak/Below", u["opp_rush_weak"]),
        ("+ opp Rush D Elite/Above (anti)", u["opp_rush_elite"]),
        (
            "+ Weak rush D + Elite rush off",
            u["opp_rush_weak"] & u["own_rush_elite"],
        ),
    ],
)
