#!/usr/bin/env python3
"""Print NFL Sun 9/20 props on current-season L5 (post Sep-1 cutoff)."""
from __future__ import annotations

import pandas as pd

STEP8 = r"H:\PropORACLE_main_cp\outputs\2026-09-20\nfl\step8_nfl_direction_clean.xlsx"
CORE = {
    "passing_yards",
    "rushing_yards",
    "receiving_yards",
    "receptions",
    "rush_attempts",
    "pass_attempts",
    "pass_completions",
    "passing_touchdowns",
    "rushing_touchdowns",
    "receiving_touchdowns",
    "player_touchdowns",
    "anytime_touchdown",
    "fantasy_score",
    "kicking_points",
    "fg_made",
}


def d_pass(side: str, tier: str) -> bool:
    t = str(tier or "").strip()
    if side == "OVER":
        return t in ("Weak", "Below Avg")
    if side == "UNDER":
        return t in ("Elite", "Above Avg")
    return False


def main() -> None:
    df = pd.read_excel(STEP8)
    df = df[df["League"].astype(str).str.upper().isin(("NFL", "9", "nan")) | df["League"].isna()]
    # drop combos / junk
    prop = df["Prop"].astype(str)
    df = df[~prop.str.contains("combo|1st_|first_|2nd_|half|quarter", case=False, na=False)]
    df = df[prop.isin(CORE) | prop.str.lower().isin(CORE)]

    flag = df["Season L5 Flag"].astype(str)
    gu = pd.to_numeric(df["Games Used"], errors="coerce")
    pt = df["Pick Type"].astype(str).str.strip().str.lower()
    direction = df["Direction"].astype(str).str.upper()
    # Prefer board direction; Goblins are OVER-only for list
    l5o = pd.to_numeric(df["L5 Over"], errors="coerce")
    l5u = pd.to_numeric(df["L5 Under"], errors="coerce")

    print("=== NFL 2026-09-20 (current-season L5 only; Sep 1 cutoff) ===")
    print("Strict L5>=4 clears: 0 (Week 2 → almost everyone is n=1).")
    print("Listed below = THIN_CLEAR + D aligned. CLEAR ≠ ticket-eligible.\n")

    # Goblin OVER THIN_CLEAR_OVER + D
    g = df[
        (pt == "goblin")
        & (flag == "THIN_CLEAR_OVER")
        & (l5o >= 1)
        & (l5u.fillna(0) == 0)
    ].copy()
    g["D_ok"] = g["Def Tier"].map(lambda t: d_pass("OVER", t))
    g = g[g["D_ok"]]
    # hardest Goblin line per player+prop
    g = g.sort_values(["Line"], ascending=False)
    g = g.drop_duplicates(subset=["Player", "Prop"], keep="first")
    g["_cov"] = pd.to_numeric(g["Stat G1"], errors="coerce") - pd.to_numeric(g["Line"], errors="coerce")
    g = g.sort_values(["_cov", "Line"], ascending=[False, False])

    print("### Goblin OVER — THIN_CLEAR_OVER + D (Weak|Below) — unique player/prop")
    print(f"{'Player':28} {'Prop':18} {'Line':>6} {'G1':>6} {'Cov':>6} {'n':>3} {'D':12} Game")
    for _, r in g.head(40).iterrows():
        game = f"{r.get('Team','')} vs {r.get('Opp','')}"
        print(
            f"{str(r['Player'])[:28]:28} {str(r['Prop'])[:18]:18} "
            f"{float(r['Line']):6.1f} {float(r['Stat G1']):6.1f} {float(r['_cov']):+6.1f} "
            f"{int(r['Games Used']):3d} {str(r['Def Tier'])[:12]:12} {game}"
        )

    # Standard OVER
    so = df[
        (pt == "standard")
        & (flag == "THIN_CLEAR_OVER")
        & ((direction == "OVER") | (l5o >= 1))
        & (l5u.fillna(0) == 0)
    ].copy()
    so = so[so["Def Tier"].map(lambda t: d_pass("OVER", t))]
    so = so.sort_values("Line", ascending=False).drop_duplicates(["Player", "Prop"], keep="first")
    so["_cov"] = pd.to_numeric(so["Stat G1"], errors="coerce") - pd.to_numeric(so["Line"], errors="coerce")
    so = so.sort_values("_cov", ascending=False)
    print("\n### Standard OVER — THIN_CLEAR_OVER + D")
    print(f"{'Player':28} {'Prop':18} {'Line':>6} {'G1':>6} {'Cov':>6} {'D':12}")
    for _, r in so.head(25).iterrows():
        print(
            f"{str(r['Player'])[:28]:28} {str(r['Prop'])[:18]:18} "
            f"{float(r['Line']):6.1f} {float(r['Stat G1']):6.1f} {float(r['_cov']):+6.1f} "
            f"{str(r['Def Tier'])[:12]:12}"
        )

    # Standard UNDER
    su = df[
        (pt == "standard")
        & (flag == "THIN_CLEAR_UNDER")
        & ((direction == "UNDER") | (l5u >= 1))
        & (l5o.fillna(0) == 0)
    ].copy()
    # fade fake zero completions junk? keep fantasy/yards
    su = su[~su["Prop"].astype(str).str.contains("completion", case=False, na=False)]
    su = su[su["Def Tier"].map(lambda t: d_pass("UNDER", t))]
    su = su.sort_values("Line", ascending=False).drop_duplicates(["Player", "Prop"], keep="first")
    su["_cov"] = pd.to_numeric(su["Line"], errors="coerce") - pd.to_numeric(su["Stat G1"], errors="coerce")
    su = su.sort_values("_cov", ascending=False)
    print("\n### Standard UNDER — THIN_CLEAR_UNDER + D (Elite|Above) — no completions")
    print(f"{'Player':28} {'Prop':18} {'Line':>6} {'G1':>6} {'Cov':>6} {'D':12}")
    for _, r in su.head(25).iterrows():
        print(
            f"{str(r['Player'])[:28]:28} {str(r['Prop'])[:18]:18} "
            f"{float(r['Line']):6.1f} {float(r['Stat G1']):6.1f} {float(r['_cov']):+6.1f} "
            f"{str(r['Def Tier'])[:12]:12}"
        )

    print("\n### Notes")
    print("- n=1 everywhere that prints — THIN_CLEAR means Week 1 alone cleared the line.")
    print("- Tyler Shough: one OT shootout (410) — more caution than flat THIN_CLEAR_OVER.")
    print("- No L5=5 ticket stack until the season has real depth.")


if __name__ == "__main__":
    main()
