#!/usr/bin/env python3
import pandas as pd

s5 = pd.read_csv(
    r"H:/PropORACLE_main_cp/outputs/2026-09-19/cfb/step5b_cfb.csv",
    low_memory=False,
)

played = [
    ("DeSean Bishop", "Rush Yards", 69.5),
    ("Marcellous Hawkins Jr.", "Rush Yards", 39.5),
    ("Michael Hawkins Jr.", "Rush Yards", 39.5),
    ("Nate Frazier", "Rush Yards", 49.5),
    ("Wayshawn Parker", "Rush Yards", 49.5),
    ("Waymond Jordan", "Rush Yards", 29.5),
    ("Devon Dampier", "Pass Yards", 159.5),
    ("Jayden Maiava", "Pass Yards", 229.5),
    ("Josiah Freeman", "Receiving Yards", 19.5),
    ("Gavin Freeman", "Receiving Yards", 69.5),
]

print(f"{'player':28} {'prop':18} {'line':6} {'n':>2} {'L5o':>3} {'status':14} games")
for p, prop, line in played:
    r = s5[
        (s5["player"] == p)
        & (s5["prop_type"] == prop)
        & (s5["pick_type"].astype(str).str.lower() == "goblin")
        & (pd.to_numeric(s5["line"], errors="coerce") == line)
    ]
    if r.empty:
        print(f"{p:28} {prop:18} {line:6.1f} MISSING")
        continue
    x = r.iloc[0]
    games = [x.get(f"stat_g{i}") for i in range(1, 6)]
    games = [g for g in games if pd.notna(g)]
    print(
        f"{p:28} {prop:18} {line:6.1f} {int(float(x.l5_sample_n)):2d} "
        f"{int(float(x.line_hits_over_5)):3d} {str(x.stat_status):14} {games}"
    )
