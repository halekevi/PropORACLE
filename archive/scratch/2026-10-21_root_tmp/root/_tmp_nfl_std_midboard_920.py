"""List NFL Standard keep faces with mid-board n=1 gaps (hand-check browse)."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from utils.nfl_keep_gates import nfl_standard_ticket_eligible, nfl_ticket_priority

STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-20/nfl/step8_nfl_direction_clean.xlsx"
)
SIT = {
    "rashod bateman",
    "kyle monangai",
    "tyler shough",
    "woody marks",  # already on C goblin
    "mack hollins",
    "devaughn vele",
}

df = pd.read_excel(STEP8)
df = df[~df["Player"].astype(str).str.contains(r"\+", regex=True)].copy()
gt = pd.to_datetime(df["Game Time"], utc=True, errors="coerce")
df["game_et"] = gt.dt.tz_convert("America/New_York")
df = df[df["game_et"].dt.date.astype(str) == "2026-09-20"].copy()
df = df[df["Pick Type"] == "Standard"].copy()
df["line"] = pd.to_numeric(df["Line"], errors="coerce")
df["avg"] = pd.to_numeric(df["Projection"], errors="coerce")
df["gap"] = df["avg"] - df["line"]
df = df.dropna(subset=["line", "avg"])

rows = []
for _, r in df.iterrows():
    d = r.to_dict()
    if not nfl_standard_ticket_eligible(d):
        continue
    player = str(r.Player)
    if player.lower() in SIT:
        continue
    side = str(r.Direction).upper()
    gap = float(r.gap)
    # play-side clearance
    if side == "OVER" and gap <= 0:
        continue
    if side == "UNDER" and gap >= 0:
        continue
    play_gap = gap if side == "OVER" else -gap
    # mid-board: exclude freak outliers; prefer solid clearance
    # yards-ish: 8..55; counting (recs/sacks/kick): 0.5..8
    prop = str(r.Prop).lower()
    if any(x in prop for x in ("yard", "rush", "rec")):
        if not (8.0 <= play_gap <= 55.0):
            continue
    else:
        if not (0.5 <= play_gap <= 10.0):
            continue
    def_tier = str(r.get("Def Tier") or "")
    # D alignment soft preference for listing
    d_ok = False
    if side == "OVER" and def_tier in ("Weak", "Below Avg"):
        d_ok = True
    if side == "UNDER" and def_tier in ("Elite", "Above Avg"):
        d_ok = True
    rows.append(
        {
            "player": player,
            "team": str(r.Team),
            "opp": str(r.Opp),
            "prop": str(r.Prop).replace("player_", "").replace("_", " "),
            "side": side,
            "line": float(r.line),
            "avg": round(float(r.avg), 1),
            "play_gap": round(play_gap, 1),
            "def": def_tier,
            "d_ok": d_ok,
            "priority": nfl_ticket_priority(d),
            "kick": r.game_et.strftime("%H:%M"),
            "flag": str(r.get("Season L5 Flag") or ""),
            "games": float(r["Games Used"]) if pd.notna(r["Games Used"]) else None,
        }
    )

# prefer keep priority, then D pass, then moderate gap (not max)
rows.sort(key=lambda x: (x["priority"], 0 if x["d_ok"] else 1, abs(x["play_gap"] - 20)))
# dedupe player
seen = set()
uniq = []
for r in rows:
    if r["player"] in seen:
        continue
    seen.add(r["player"])
    uniq.append(r)

print(f"mid-board Standard keep clears: {len(uniq)}")
for i, r in enumerate(uniq[:40], 1):
    print(
        f"{i:2} prio={r['priority']} gap={r['play_gap']:+5.1f} "
        f"{'D' if r['d_ok'] else '.'} {r['side']:<5} {r['player']:<22} "
        f"{r['prop']:<18} L{r['line']:<6} avg={r['avg']:<6} "
        f"{r['team']} vs {r['opp']} {r['def']}"
    )

Path(r"H:/PropORACLE/_tmp_nfl_std_midboard_920.json").write_text(
    json.dumps(uniq[:50], indent=2), encoding="utf-8"
)
