#!/usr/bin/env python3
"""Count CFB board legs whose L5 pads with prior-season games (2026 slate)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

CACHE = Path(r"H:/PropORACLE_main_cp/Sports/CFB/data/cache/cfb_boxscore_cache.csv")
STEP8 = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-19/cfb/step8_cfb_direction_clean.xlsx")
# Optional id map
IDMAP = Path(r"H:/PropORACLE/Sports/CFB/data/reference/pp_to_espn_id_map_cfb.csv")
SEASON_START = "2026-08-01"

cache = pd.read_csv(CACHE, low_memory=False, dtype=str).fillna("")


def to_iso(s: str) -> str:
    s = str(s or "").strip().replace("-", "")
    if len(s) >= 8 and s[:8].isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return ""


cache["_d"] = cache["game_date"].map(to_iso)
cache = cache[cache["_d"] != ""]

step8 = pd.read_excel(STEP8)
step8.columns = [str(c).strip() for c in step8.columns]
std = step8[step8["Pick Type"].astype(str).str.lower().eq("standard")].copy()
std["_side"] = std["Direction"].astype(str).str.upper()
std["_l5"] = std.apply(
    lambda r: r["L5 Over"] if str(r["_side"]).startswith("O") else r["L5 Under"],
    axis=1,
)
std["_l5n"] = pd.to_numeric(std["_l5"], errors="coerce")
gated = std[std["_l5n"] >= 4].copy()
print("Standard L5>=4 legs", len(gated), "unique players", gated["Player"].nunique())

# Map PP player -> espn id when possible
idmap = {}
if IDMAP.exists():
    m = pd.read_csv(IDMAP, dtype=str).fillna("")
    # flexible cols
    pc = next((c for c in m.columns if "player" in c.lower() or "name" in c.lower()), None)
    ic = next((c for c in m.columns if "espn" in c.lower() or c.endswith("_id")), None)
    print("idmap", pc, ic, "rows", len(m))
    if pc and ic:
        for _, r in m.iterrows():
            idmap[str(r[pc]).strip().lower()] = str(r[ic]).strip()

# Also try step8 ESPN ID if present
espn_col = next((c for c in gated.columns if "espn" in c.lower() and "id" in c.lower()), None)
print("step8 espn col", espn_col)


def dates_for_player(player: str, espn_id: str = "") -> list[str]:
    if espn_id and espn_id not in ("", "nan", "None"):
        m = cache[cache["espn_athlete_id"].astype(str).str.replace(r"\.0$", "", regex=True) == str(espn_id).replace(".0", "")]
        if len(m):
            return sorted(m["_d"].unique(), reverse=True)
    # name via player_norm-ish: lowercase collapse
    key = "".join(ch for ch in player.lower() if ch.isalnum() or ch == " ")
    key = " ".join(key.split())
    m = cache[cache["player_norm"].astype(str) == key]
    if not len(m) and " " in key:
        # last-resort: first+last tokens
        parts = key.split()
        m = cache[
            cache["player_norm"].astype(str).str.contains(parts[0], regex=False)
            & cache["player_norm"].astype(str).str.contains(parts[-1], regex=False)
        ]
    return sorted(m["_d"].unique(), reverse=True) if len(m) else []


contam = []
clean = 0
no_cache = 0
for player, grp in gated.groupby("Player"):
    espn_id = ""
    if espn_col:
        espn_id = str(grp[espn_col].iloc[0] or "")
    if not espn_id:
        espn_id = idmap.get(str(player).strip().lower(), "")
    dates = dates_for_player(str(player), espn_id)
    if not dates:
        no_cache += 1
        continue
    cur = [d for d in dates if d >= SEASON_START]
    prior = [d for d in dates if d < SEASON_START]
    n_legs = len(grp)
    if len(cur) >= 5:
        clean += 1
    elif len(cur) == 0:
        contam.append((str(player), 0, len(prior), "all_prior", n_legs))
    else:
        contam.append((str(player), len(cur), len(prior), "padded", n_legs))

print(f"\nClean players (>=5 current-season): {clean}")
print(f"No cache match: {no_cache}")
print(f"Contaminated players: {len(contam)}")
print(f"  padded (1-4 current + prior fill): {sum(1 for x in contam if x[3]=='padded')}")
print(f"  all-prior L5: {sum(1 for x in contam if x[3]=='all_prior')}")
print(f"Contaminated Standard L5>=4 legs: {sum(x[4] for x in contam)} / {len(gated)}")

print("\nTicket-relevant:")
for label in (
    "Tavorus Jones",
    "Dawson Pendergrass",
    "Jeffrey Overton Jr.",
    "Carson Bruhn",
    "George Pettaway",
    "Trey Kukuk",
):
    dates = dates_for_player(label)
    cur = [d for d in dates if d >= SEASON_START]
    prior = [d for d in dates if d < SEASON_START]
    print(f"  {label:24} cur={cur[:6]} n_cur={len(cur)} n_prior={len(prior)}")

print("\nWorst padded (fewest current games, most gated legs):")
for row in sorted([x for x in contam if x[3] == "padded"], key=lambda x: (x[1], -x[4]))[:30]:
    print(f"  {row[0]:28} cur={row[1]} prior={row[2]} gated_legs={row[4]}")
