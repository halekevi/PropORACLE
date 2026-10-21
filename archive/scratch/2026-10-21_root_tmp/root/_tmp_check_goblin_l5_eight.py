#!/usr/bin/env python3
"""Direct check: rebuilt CFB L5 for Goblin ticket surnames."""
from __future__ import annotations

import pandas as pd

s5 = pd.read_csv(
    r"H:/PropORACLE_main_cp/outputs/2026-09-19/cfb/step5b_cfb.csv",
    low_memory=False,
)

# Likely full names from board (disambiguate Jordan / Hawkins / Freeman)
TARGETS = [
    ("DeSean Bishop", "Rush Yards"),
    ("Michael Hawkins Jr.", "Pass+Rush Yds"),
    ("Michael Hawkins Jr.", "Rush Yards"),
    ("Nate Frazier", "Rush Yards"),
    ("Devon Dampier", "Pass Yards"),
    ("Devon Dampier", "Pass+Rush Yds"),
    ("Devon Dampier", "Rush Yards"),
    ("Jayden Maiava", "Pass Yards"),
    ("Wayshawn Parker", "Rush Yards"),
    ("Wayshawn Parker", "Rush+Rec Yds"),
    ("Waymond Jordan", "Rush Yards"),
    ("Jordan Faison", "Receiving Yards"),
    ("Jordan Shipp", "Receiving Yards"),
    ("Gavin Freeman", "Receiving Yards"),
    ("Josiah Freeman", "Receiving Yards"),
]

print(
    f"{'player':28} {'prop':16} {'line':6} {'status':16} {'n':>3} "
    f"{'g1':>6} {'g2':>6} {'g3':>6} {'g4':>6} {'g5':>6} "
    f"{'avg':>6} {'L5o':>3} {'L5u':>3}"
)
print("-" * 130)

seen = set()
for player, prop in TARGETS:
    m = s5[
        s5["player"].astype(str).str.fullmatch(player, case=False)
        & s5["prop_type"].astype(str).str.fullmatch(prop, case=False)
        & s5["pick_type"].astype(str).str.lower().eq("goblin")
    ]
    if m.empty:
        m = s5[
            s5["player"].astype(str).str.contains(player.split()[0], case=False)
            & s5["player"].astype(str).str.contains(player.split()[-1].rstrip("."), case=False)
            & s5["prop_type"].astype(str).str.contains(prop.split()[0], case=False)
            & s5["pick_type"].astype(str).str.lower().eq("goblin")
        ]
    if m.empty:
        print(f"{player:28} {prop:16} NO ROW")
        continue
    # one row per distinct line — take lowest Goblin line (often the played one)
    m = m.sort_values("line")
    for _, r in m.iterrows():
        key = (str(r["player"]), str(r["prop_type"]), float(r["line"]))
        if key in seen:
            continue
        seen.add(key)
        gs = [r.get(f"stat_g{i}") for i in range(1, 6)]
        def fmt(v):
            try:
                if pd.isna(v):
                    return "  —"
                return f"{float(v):6.1f}"
            except Exception:
                return "  —"
        avg = r.get("stat_last5_avg")
        avg_s = fmt(avg)
        print(
            f"{str(r['player'])[:28]:28} {str(r['prop_type'])[:16]:16} "
            f"{float(r['line']):6.1f} {str(r.get('stat_status',''))[:16]:16} "
            f"{int(float(r['l5_sample_n'])) if pd.notna(r.get('l5_sample_n')) else 0:3d} "
            f"{fmt(gs[0])}{fmt(gs[1])}{fmt(gs[2])}{fmt(gs[3])}{fmt(gs[4])} "
            f"{avg_s} "
            f"{int(float(r['line_hits_over_5'])) if pd.notna(r.get('line_hits_over_5')) else 0:3d} "
            f"{int(float(r['line_hits_under_5'])) if pd.notna(r.get('line_hits_under_5')) else 0:3d}"
        )

# Summary: any of these with L5 over >=4 or >=5 after fix?
print("\n=== Goblin OVER L5 clear after fix? ===")
gob = s5[s5["pick_type"].astype(str).str.lower().eq("goblin")].copy()
players = [
    "DeSean Bishop",
    "Michael Hawkins Jr.",
    "Nate Frazier",
    "Devon Dampier",
    "Jayden Maiava",
    "Wayshawn Parker",
    "Waymond Jordan",
    "Gavin Freeman",
    "Josiah Freeman",
    "Jordan Faison",
    "Jordan Shipp",
]
for p in players:
    m = gob[gob["player"].astype(str).str.fullmatch(p, case=False)]
    if m.empty:
        continue
    o = pd.to_numeric(m["line_hits_over_5"], errors="coerce")
    n = pd.to_numeric(m["l5_sample_n"], errors="coerce")
    st = m["stat_status"].astype(str).iloc[0]
    print(
        f"{p:28} status={st:16} max_L5_over={o.max()} max_sample={n.max()} "
        f"any_L5>=4={bool((o>=4).any())} any_L5=5={bool((o>=5).any())}"
    )
