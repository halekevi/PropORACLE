"""NFL ticket pools from mid-board n=1 gaps — Goblin / Standard / mixed."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from utils.nfl_keep_gates import nfl_standard_ticket_eligible

STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-20/nfl/step8_nfl_direction_clean.xlsx"
)
# Already on C, or hard fades / outlier tops
BAN = {
    "woody marks",
    "mack hollins",
    "devaughn vele",
    "rashod bateman",
    "kyle monangai",
    "tyler shough",
    "colston loveland",
    "kenneth walker iii",  # +126 outlier top
    "christian watson",  # +122 outlier
    "jacoby brissett",  # +127 outlier
    "c.j. stroud",  # +119 outlier
    "jalen coker",  # +117 outlier
    "malik willis",
    "deshaun watson",
}

df = pd.read_excel(STEP8)
df = df[~df["Player"].astype(str).str.contains(r"\+", regex=True)].copy()
df = df[
    ~df["Prop"]
    .astype(str)
    .str.contains("combo|per_carry|longest|fantasy|anytime|td", case=False, na=False)
].copy()
gt = pd.to_datetime(df["Game Time"], utc=True, errors="coerce")
df["game_et"] = gt.dt.tz_convert("America/New_York")
df = df[df["game_et"].dt.date.astype(str) == "2026-09-20"].copy()
df["line"] = pd.to_numeric(df["Line"], errors="coerce")
df["avg"] = pd.to_numeric(df["Projection"], errors="coerce")
df["gap"] = df["avg"] - df["line"]
df = df.dropna(subset=["line", "avg"])
df["prop_l"] = df["Prop"].astype(str).str.lower()
df["pick"] = df["Pick Type"].astype(str)
df["side"] = df["Direction"].astype(str).str.upper()
df["player"] = df["Player"].astype(str)
df["def"] = df["Def Tier"].astype(str)


def mid_over(play_gap: float, prop: str) -> bool:
    if "yard" in prop or "rush" in prop or "rec" in prop or "pass" in prop:
        return 10.0 <= play_gap <= 45.0
    return 0.5 <= play_gap <= 6.0


def mid_under(play_gap: float, prop: str, avg: float) -> bool:
    if "yard" in prop or "rush" in prop or "rec" in prop:
        if avg < 8:
            return False  # DNP/zero mirage
        return 8.0 <= play_gap <= 40.0
    return 0.5 <= play_gap <= 8.0


goblins = []
standards = []
for _, r in df.iterrows():
    if r.player.lower() in BAN:
        continue
    gap = float(r.gap)
    prop = r.prop_l
    avg = float(r.avg)
    row = {
        "player": r.player,
        "team": str(r.Team),
        "opp": str(r.Opp),
        "prop": str(r.Prop).replace("_", " "),
        "prop_key": prop,
        "pick": r.pick,
        "side": r.side,
        "line": float(r.line),
        "avg": round(avg, 1),
        "gap": round(gap, 1),
        "def": r["def"],
        "kick": r.game_et.strftime("%H:%M"),
        "matchup": f"{r.Team} vs {r.Opp}",
    }
    if r.pick == "Goblin" and r.side == "OVER" and gap > 0:
        play = gap
        if not mid_over(play, prop):
            continue
        # skill overs prefer Weak/Below; kickers OK
        row["play_gap"] = round(play, 1)
        row["d_ok"] = r["def"] in ("Weak", "Below Avg") or "kick" in prop
        goblins.append(row)
    elif r.pick == "Standard":
        d = r.to_dict()
        if not nfl_standard_ticket_eligible(d):
            continue
        if r.side == "OVER" and gap > 0:
            play = gap
            if not mid_over(play, prop):
                continue
            row["play_gap"] = round(play, 1)
            row["d_ok"] = r["def"] in ("Weak", "Below Avg")
            standards.append(row)
        elif r.side == "UNDER" and gap < 0:
            play = -gap
            if not mid_under(play, prop, avg):
                continue
            row["play_gap"] = round(play, 1)
            row["d_ok"] = r["def"] in ("Elite", "Above Avg")
            standards.append(row)

# one face per player+prop+pick (closest to ~20 yd gap / mid)
goblins.sort(key=lambda x: (0 if x["d_ok"] else 1, abs(x["play_gap"] - 22)))
standards.sort(key=lambda x: (0 if x["d_ok"] else 1, abs(x["play_gap"] - 18)))


def dedupe(rows, key=("player", "prop", "pick")):
    seen = set()
    out = []
    for r in rows:
        k = tuple(r[k] for k in key)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


goblins = dedupe(goblins)
standards = dedupe(standards)

# also one per player for packing
def one_per_player(rows):
    seen = set()
    out = []
    for r in rows:
        if r["player"] in seen:
            continue
        seen.add(r["player"])
        out.append(r)
    return out


G = one_per_player(goblins)
S = one_per_player(standards)

print(f"Goblin mid OVER pool: {len(G)}")
for i, r in enumerate(G[:20], 1):
    print(
        f"  G{i:02} {'D' if r['d_ok'] else '.'} +{r['play_gap']:<5} "
        f"{r['player']:<22} {r['prop']:<16} L{r['line']:<6} "
        f"avg={r['avg']:<6} {r['matchup']} {r['def']}"
    )
print(f"\nStandard mid keep pool: {len(S)}")
for i, r in enumerate(S[:20], 1):
    print(
        f"  S{i:02} {'D' if r['d_ok'] else '.'} {r['side']:<5} +{r['play_gap']:<5} "
        f"{r['player']:<22} {r['prop']:<16} L{r['line']:<6} "
        f"avg={r['avg']:<6} {r['matchup']} {r['def']}"
    )

# Proposed tickets (unique players across card; C already used Marks/Hollins/Vele)
# Prefer different games within a ticket (max 2 same game soft)
used = set()


def take(pool, n, prefer_d=True):
    out = []
    games = {}
    for r in pool:
        if r["player"] in used:
            continue
        if prefer_d and not r["d_ok"] and len(out) < n - 1:
            # save non-D for fill only after D exhausted in remaining
            pass
        g = r["matchup"]
        if games.get(g, 0) >= 1:  # prefer 1 per game first pass
            continue
        out.append(r)
        used.add(r["player"])
        games[g] = games.get(g, 0) + 1
        if len(out) >= n:
            return out
    # second pass relax game
    for r in pool:
        if r["player"] in used:
            continue
        out.append(r)
        used.add(r["player"])
        if len(out) >= n:
            return out
    return out


# Rebuild with smarter packing
used.clear()
tickets = {}

# E: Goblin Power 3 — mid overs, prefer D
tickets["E_goblin_p3"] = {
    "mode": "power",
    "kind": "goblin_only",
    "legs": take([r for r in G if r["d_ok"]] or G, 3),
}
# F: Standard Flex 4 — mid unders/overs keep
tickets["F_std_f4"] = {
    "mode": "flex",
    "kind": "standard_only",
    "legs": take(S, 4),
}
# G: Mixed Flex 4 — 2 Goblin OVER + 2 Standard
g_mix = take([r for r in G if r["player"] not in used], 2)
s_mix = take([r for r in S if r["player"] not in used], 2)
tickets["G_mixed_f4"] = {
    "mode": "flex",
    "kind": "mixed",
    "legs": g_mix + s_mix,
}
# H: Goblin Power 3 #2
tickets["H_goblin_p3"] = {
    "mode": "power",
    "kind": "goblin_only",
    "legs": take(G, 3),
}
# I: Standard Power 3
tickets["I_std_p3"] = {
    "mode": "power",
    "kind": "standard_only",
    "legs": take(S, 3),
}
# J: Mixed Power 3 — 2G + 1S
g2 = take(G, 2)
s1 = take(S, 1)
tickets["J_mixed_p3"] = {
    "mode": "power",
    "kind": "mixed",
    "legs": g2 + s1,
}

print("\n======== PROPOSED NFL CARD ========")
for tid, t in tickets.items():
    print(f"\n{tid} [{t['kind']}] {t['mode'].upper()} {len(t['legs'])}")
    for leg in t["legs"]:
        side = leg["side"][0]
        print(
            f"  {leg['pick'][0]} {side} {leg['player']} {leg['prop']} "
            f"{leg['line']} (avg {leg['avg']} gap {leg['play_gap']:+} D={leg['def']})"
        )

Path(r"H:/PropORACLE/_tmp_nfl_ticket_card_920.json").write_text(
    json.dumps(tickets, indent=2), encoding="utf-8"
)
print("\nwrote card json")
