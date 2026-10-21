from __future__ import annotations

from pathlib import Path

import pandas as pd

root = Path(r"H:\PropORACLE_main_cp\outputs\2026-09-24")


def start_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "start" in c.lower():
            return c
    raise KeyError("no start col")


def hhmm(v: object) -> str:
    return str(v)[11:16]


print("WNBA: no Friday faces")
print("MLB: no Friday faces")
print()

t = pd.read_csv(root / "tennis" / "step1_tennis_props.csv", low_memory=False)
sc = start_col(t)
fri = t[t[sc].astype(str).str.contains("2026-09-25")].copy()
sub = fri[fri["pick_type"].astype(str).str.lower().isin(["goblin", "standard"])]
keep = sub[sub["prop_type"].astype(str).str.lower().isin(["total games", "games won", "total games won"])]

print("TENNIS keep — deepest Goblin Total Games (lowest line)")
g = keep[
    (keep["prop_type"].astype(str).str.lower() == "total games")
    & (keep["pick_type"].astype(str).str.lower() == "goblin")
]
if len(g):
    best = g.loc[g.groupby("player")["line"].idxmin()]
    for _, r in best.sort_values(sc).iterrows():
        print(f"  {hhmm(r[sc])}  {r['player']:28} Total Games Goblin {r['line']}")

print()
print("TENNIS keep — deepest Goblin Games Won")
gw = keep[
    keep["prop_type"].astype(str).str.lower().str.contains("games won")
    & (keep["pick_type"].astype(str).str.lower() == "goblin")
]
if len(gw):
    best = gw.loc[gw.groupby("player")["line"].idxmin()]
    for _, r in best.sort_values(sc).iterrows():
        print(f"  {hhmm(r[sc])}  {r['player']:28} {r['prop_type']} Goblin {r['line']}")

print()
print("TENNIS Standard Games Won OVER")
sgw = keep[
    keep["prop_type"].astype(str).str.lower().str.contains("games won")
    & (keep["pick_type"].astype(str).str.lower() == "standard")
]
for _, r in sgw.sort_values([sc, "player", "line"]).iterrows():
    print(f"  {hhmm(r[sc])}  {r['player']:28} {r['prop_type']} Standard {r['line']}")

s = pd.read_csv(root / "soccer" / "step1_soccer_props.csv", low_memory=False)
scs = start_col(s)
gd = s["game_date"].astype(str) if "game_date" in s.columns else pd.Series([""] * len(s))
fri_s = s[s[scs].astype(str).str.contains("2026-09-25") | gd.eq("2026-09-25")].copy()
sub_s = fri_s[fri_s["pick_type"].astype(str).str.lower().isin(["goblin", "standard"])]
propn = sub_s["prop_type"].astype(str)

print()
print("SOCCER teams")
seen: set[tuple[str, str]] = set()
for _, r in fri_s.sort_values(scs).iterrows():
    key = (hhmm(r[scs]), str(r.get("team") or ""))
    if key in seen:
        continue
    seen.add(key)
    print(f"  {key[0]}  {key[1]}")

print()
print("SOCCER Saves Goblin/Standard")
saves = sub_s[propn.str.lower() == "saves"]
for _, r in saves.sort_values([scs, "player", "line"]).iterrows():
    print(
        f"  {hhmm(r[scs])}  {r['player']:28} {str(r.get('team') or ''):16} "
        f"Saves {r['pick_type']} {r['line']}"
    )

print()
print("SOCCER Shots Goblin (deepest, line >= 1.5)")
sh = sub_s[(propn.str.lower() == "shots") & (sub_s["pick_type"].str.lower() == "goblin")]
sh = sh[pd.to_numeric(sh["line"], errors="coerce") >= 1.5]
if len(sh):
    best = sh.loc[sh.groupby("player")["line"].idxmin()]
    for _, r in best.sort_values([scs, "player"]).iterrows():
        print(
            f"  {hhmm(r[scs])}  {r['player']:28} {str(r.get('team') or ''):16} "
            f"Shots Goblin {r['line']}"
        )

print()
print("SOCCER SOT Goblin (deepest)")
sot = sub_s[
    propn.str.lower().isin(["shots on target", "shotsontarget", "sot"])
    & (sub_s["pick_type"].str.lower() == "goblin")
]
if len(sot):
    best = sot.loc[sot.groupby("player")["line"].idxmin()]
    for _, r in best.sort_values([scs, "player"]).iterrows():
        print(
            f"  {hhmm(r[scs])}  {r['player']:28} {str(r.get('team') or ''):16} "
            f"SOT Goblin {r['line']}"
        )
