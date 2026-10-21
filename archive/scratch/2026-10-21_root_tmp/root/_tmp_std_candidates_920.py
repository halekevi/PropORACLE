"""NFL mid-board Standard UNDER with real Week-1 usage + soccer Std Saves."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from utils.nfl_keep_gates import nfl_standard_ticket_eligible, nfl_ticket_priority
from utils.ticket_70_pool import standard_ticket_eligible

SIT = {
    "rashod bateman",
    "kyle monangai",
    "tyler shough",
    "woody marks",
    "mack hollins",
    "devaughn vele",
    "colston loveland",  # 0 avg on huge line — thin/out look
}

# --- NFL ---
df = pd.read_excel(
    r"H:/PropORACLE_main_cp/outputs/2026-09-20/nfl/step8_nfl_direction_clean.xlsx"
)
df = df[~df["Player"].astype(str).str.contains(r"\+", regex=True)].copy()
gt = pd.to_datetime(df["Game Time"], utc=True, errors="coerce")
df["game_et"] = gt.dt.tz_convert("America/New_York")
df = df[df["game_et"].dt.date.astype(str) == "2026-09-20"].copy()
df = df[df["Pick Type"] == "Standard"].copy()
df["line"] = pd.to_numeric(df["Line"], errors="coerce")
df["avg"] = pd.to_numeric(df["Projection"], errors="coerce")
df["gap"] = df["avg"] - df["line"]
df = df.dropna(subset=["line", "avg"])

nfl = []
for _, r in df.iterrows():
    d = r.to_dict()
    if not nfl_standard_ticket_eligible(d):
        continue
    player = str(r.Player)
    if player.lower() in SIT:
        continue
    side = str(r.Direction).upper()
    gap = float(r.gap)
    play_gap = gap if side == "OVER" else -gap
    if side == "OVER" and gap <= 0:
        continue
    if side == "UNDER" and gap >= 0:
        continue
    avg = float(r.avg)
    prop = str(r.Prop).lower()
    # real usage: yards props need meaningful Week-1 actual (not 0/DNP mirage)
    if "yard" in prop or "rush" in prop or "rec" in prop:
        if avg < 8 and side == "UNDER":
            continue  # zero/near-zero UNDER locks are usually DNP/injury
        if not (8.0 <= play_gap <= 40.0):
            continue
    else:
        if not (0.5 <= play_gap <= 8.0):
            continue
    def_tier = str(r.get("Def Tier") or "")
    d_ok = (side == "OVER" and def_tier in ("Weak", "Below Avg")) or (
        side == "UNDER" and def_tier in ("Elite", "Above Avg")
    )
    nfl.append(
        {
            "sport": "NFL",
            "player": player,
            "team": str(r.Team),
            "opp": str(r.Opp),
            "prop": str(r.Prop).replace("_", " "),
            "side": side,
            "line": float(r.line),
            "avg": round(avg, 1),
            "play_gap": round(play_gap, 1),
            "def": def_tier,
            "d_ok": d_ok,
            "priority": nfl_ticket_priority(d),
            "kick": r.game_et.strftime("%H:%M"),
            "gate70": False,
            "note": "n1 handcheck mid-gap",
        }
    )

nfl.sort(key=lambda x: (x["priority"], 0 if x["d_ok"] else 1, abs(x["play_gap"] - 18)))
seen = set()
nfl_u = []
for r in nfl:
    if r["player"] in seen:
        continue
    seen.add(r["player"])
    nfl_u.append(r)

# --- Soccer Gate70 Standard ---
sdf = pd.read_excel(
    r"H:/PropORACLE_main_cp/outputs/2026-09-20/soccer/step8_soccer_direction_clean.xlsx"
)
soc = []
for _, r in sdf.iterrows():
    row = {k: r[k] for k in r.index}
    row["sport"] = "SOCCER"
    row["pick_type"] = r.get("Pick Type") or r.get("pick_type")
    row["side"] = r.get("Direction") or r.get("final_bet_direction") or r.get("side")
    row["prop"] = r.get("Prop") or r.get("prop_type") or r.get("prop")
    row["player"] = r.get("Player") or r.get("player")
    row["line"] = r.get("Line") or r.get("line")
    row["l5_over"] = r.get("L5 Over") or r.get("l5_over") or r.get("last5_over")
    row["l5_under"] = r.get("L5 Under") or r.get("l5_under") or r.get("last5_under")
    row["l10_over"] = r.get("L10 Over") or r.get("l10_over") or r.get("last10_over")
    row["l10_under"] = r.get("L10 Under") or r.get("l10_under") or r.get("last10_under")
    row["def"] = r.get("Def Tier") or r.get("DEF_TIER") or r.get("def") or r.get("Opp Def")
    row["cover"] = r.get("Edge") or r.get("cover") or r.get("Abs Edge") or r.get("dist_l5")
    if str(row.get("pick_type") or "").title() != "Standard":
        continue
    try:
        ok = standard_ticket_eligible(row)
    except Exception:
        ok = False
    if not ok:
        continue
    soc.append(
        {
            "sport": "SOCCER",
            "player": str(row["player"]),
            "prop": str(row["prop"]),
            "side": str(row["side"]).upper(),
            "line": float(row["line"]),
            "l5o": row.get("l5_over"),
            "l5u": row.get("l5_under"),
            "l10o": row.get("l10_over"),
            "l10u": row.get("l10_under"),
            "def": str(row.get("def") or ""),
            "gate70": True,
            "note": "soccer Saves keep Gate70",
            "team": str(r.get("Team") or r.get("team") or ""),
            "opp": str(r.get("Opp") or r.get("opp_team") or ""),
        }
    )

print("=== NFL mid-board Standard (real W1 usage) ===")
for i, r in enumerate(nfl_u[:25], 1):
    print(
        f"{i:2} {'D' if r['d_ok'] else '.'} {r['side']:<5} gap={r['play_gap']:+5.1f} "
        f"{r['player']:<24} {r['prop']:<18} L{r['line']:<6} avg={r['avg']:<6} "
        f"{r['team']}vs{r['opp']} {r['def']}"
    )

print("\n=== Soccer Standard Gate70 ===")
for r in soc:
    print(
        f"  {r['side']:<5} {r['player']:<28} {r['prop']:<16} L{r['line']} "
        f"L5o={r['l5o']} L5u={r['l5u']} L10o={r['l10o']} L10u={r['l10u']} D={r['def']}"
    )

# Proposed Standard Flex: prefer soccer Gate70 if >=3 unique, else NFL handcheck Flex4
Path(r"H:/PropORACLE/_tmp_std_ticket_candidates_920.json").write_text(
    json.dumps({"nfl": nfl_u[:30], "soccer": soc}, indent=2), encoding="utf-8"
)
print("\nnfl", len(nfl_u), "soccer", len(soc))
