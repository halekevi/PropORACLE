#!/usr/bin/env python3
import pandas as pd

df = pd.read_csv(r"H:/PropORACLE_main_cp/outputs/2026-09-19/cfb/step5b_cfb.csv", low_memory=False)
print("status\n", df["stat_status"].value_counts().head(12).to_string())
j = df[
    df.player.astype(str).str.contains("Tavorus", case=False)
    & (df.prop_type.astype(str) == "Rush Yards")
]
cols = [
    c
    for c in [
        "pick_type",
        "line",
        "stat_g1",
        "stat_g2",
        "stat_g3",
        "stat_last5_avg",
        "line_hits_over_5",
        "line_hits_under_5",
        "stat_status",
        "games_used",
        "l5_sample_n",
        "season_l5_flag",
    ]
    if c in df.columns
]
print(j[cols].to_string())
std = df[df.pick_type.astype(str).str.lower().eq("standard")].copy()
u = pd.to_numeric(std["line_hits_under_5"], errors="coerce")
o = pd.to_numeric(std["line_hits_over_5"], errors="coerce")
n = pd.to_numeric(std["l5_sample_n"], errors="coerce")
print(
    "std L5>=4 under",
    int((u >= 4).sum()),
    "over",
    int((o >= 4).sum()),
    "sample>=5",
    int((n >= 5).sum()),
    "sample 1-4",
    int(((n >= 1) & (n < 5)).sum()),
    "THIN",
    int(std.stat_status.astype(str).str.contains("THIN").sum()),
)
# ticket players
for name in [
    "Tavorus Jones",
    "Dawson Pendergrass",
    "Jeffrey Overton",
    "Carson Bruhn",
    "George Pettaway",
]:
    m = std[std.player.astype(str).str.contains(name.split()[-1], case=False) & std.player.astype(str).str.contains(name.split()[0], case=False)]
    if len(m) == 0:
        print(name, "no std row")
        continue
    r = m.iloc[0]
    print(
        name,
        "line",
        r.get("line"),
        "g",
        [r.get("stat_g1"), r.get("stat_g2"), r.get("stat_g3")],
        "L5u",
        r.get("line_hits_under_5"),
        "L5o",
        r.get("line_hits_over_5"),
        "n",
        r.get("l5_sample_n"),
        r.get("stat_status"),
    )
