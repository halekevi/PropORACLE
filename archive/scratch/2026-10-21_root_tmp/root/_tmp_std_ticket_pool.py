#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-19")


def load(sport: str, f: str) -> pd.DataFrame:
    df = pd.read_excel(ROOT / sport / f)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def main() -> None:
    cfb = load("cfb", "step8_cfb_direction_clean.xlsx")
    wnba = load("wnba", "step8_wnba_direction_clean.xlsx")
    soccer = load("soccer", "step8_soccer_direction_clean.xlsx")

    print("=== CFB Standard L5>=4 core (no longest) ===")
    d = cfb[cfb["Pick Type"].astype(str).str.lower().eq("standard")].copy()
    d["_side"] = d["Direction"].astype(str).str.upper()
    prop = d["Prop"].astype(str)
    junk = prop.str.contains(
        "Longest|Completion %|First |YPC|Comp %", case=False, na=False
    )
    d = d[~junk]
    d["_l5"] = d.apply(
        lambda r: r["L5 Over"]
        if str(r["_side"]).startswith("O")
        else r["L5 Under"],
        axis=1,
    )
    d["_l5n"] = pd.to_numeric(d["_l5"], errors="coerce")
    d = d[d["_l5n"] >= 4].sort_values("_l5n", ascending=False)

    for side in ("OVER", "UNDER"):
        s = d[d["_side"].str.startswith(side[0])]
        print(f"-- {side} n={len(s)}")
        for _, r in s.head(18).iterrows():
            print(
                f"  {str(r['Player']):24} {str(r['Prop']):22} "
                f"{r['_side'][0]}{r['Line']}  L5 {r['L5 Over']}/{r['L5 Under']}  "
                f"L10 {r.get('L10 Over')}/{r.get('L10 Under')}  "
                f"proj {r.get('Projection')}  {r.get('Team')} vs {r.get('Opp')}"
            )

    print("\n=== WNBA Standard cols / L5>=4 ===")
    print([c for c in wnba.columns if "L5" in c or "Hit" in c][:25])
    w = wnba[wnba["Pick Type"].astype(str).str.lower().eq("standard")].copy()
    if "L5 Over" in w.columns:
        w["_side"] = w["Direction"].astype(str).str.upper()
        w["_l5"] = w.apply(
            lambda r: r["L5 Over"]
            if str(r["_side"]).startswith("O")
            else r["L5 Under"],
            axis=1,
        )
        w["_l5n"] = pd.to_numeric(w["_l5"], errors="coerce")
        w = w[w["_l5n"] >= 4].sort_values("_l5n", ascending=False)
        for _, r in w.head(15).iterrows():
            print(
                f"  {r['Player']:24} {r['Prop']:22} {r['_side'][0]}{r['Line']}  "
                f"L5 {r['L5 Over']}/{r['L5 Under']}  proj {r.get('Projection')}"
            )
    elif "Hit Rate L5" in w.columns:
        for _, r in w.sort_values("Hit Rate L5", ascending=False).head(15).iterrows():
            print(
                f"  {r['Player']:24} {r['Prop']:22} {str(r['Direction'])[0]}{r['Line']}  "
                f"HR5 {r.get('Hit Rate L5')}  proj {r.get('Projection')}"
            )

    print("\n=== SOCCER Standard Saves/Shots/SOT ===")
    s = soccer[soccer["Pick Type"].astype(str).str.lower().eq("standard")].copy()
    s = s[
        s["Prop"]
        .astype(str)
        .str.contains("Save|Shot|SOT|On Target", case=False, na=False)
    ]
    cols = [
        c
        for c in [
            "Player",
            "Prop",
            "Line",
            "Direction",
            "L5 Over",
            "L5 Under",
            "Hit Rate L5",
            "Projection",
            "Team",
            "Opp",
        ]
        if c in s.columns
    ]
    print(s[cols].head(30).to_string(index=False))


if __name__ == "__main__":
    main()
