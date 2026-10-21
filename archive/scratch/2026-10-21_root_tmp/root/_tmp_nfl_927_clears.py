#!/usr/bin/env python3
"""List NFL 2026-09-27 L5=5 clears and handcheck seed gaps."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:/PropORACLE")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
from utils.prop_norm import canon_prop

p = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-27/nfl/step8_nfl_direction_clean.xlsx"
)
df = pd.read_excel(p)

KEEP = {
    "passing_yards",
    "rushing_yards",
    "receiving_yards",
    "receptions",
    "rush_attempts",
    "pass_attempts",
    "pass_completions",
    "tackles_ast",
    "sacks",
    "kicking_points",
    "fg_made",
}

rows = []
for _, r in df.iterrows():
    side = str(r.get("Direction") or "").upper()
    pt = str(r.get("Pick Type") or "").lower()
    if "demon" in pt:
        continue
    prop = canon_prop("NFL", str(r.get("Prop") or ""))
    if prop not in KEEP:
        continue
    l5c = "L5 Over" if side == "OVER" else "L5 Under"
    l10c = "L10 Over" if side == "OVER" else "L10 Under"
    l5 = float(pd.to_numeric(r.get(l5c), errors="coerce") or 0)
    l10 = float(pd.to_numeric(r.get(l10c), errors="coerce") or 0)
    avg = pd.to_numeric(r.get("Last 5 Avg"), errors="coerce")
    line = pd.to_numeric(r.get("Line"), errors="coerce")
    if pd.isna(avg) or pd.isna(line):
        continue
    if l5 < 5 or l10 < 8:
        continue
    cover = (float(avg) - float(line)) if side == "OVER" else (float(line) - float(avg))
    need = (
        15
        if prop == "passing_yards"
        else (8 if prop in {"rushing_yards", "receiving_yards"} else 1)
    )
    if "goblin" in pt and side != "OVER":
        continue
    if "goblin" in pt and cover < need:
        continue
    rows.append(
        {
            "player": str(r.get("Player")),
            "team": str(r.get("Team")).upper(),
            "prop": str(r.get("Prop")),
            "canon": prop,
            "line": float(line),
            "side": side,
            "pick": "goblin" if "goblin" in pt else "standard",
            "l5": l5,
            "l10": l10,
            "cover": cover,
            "d": str(r.get("Def Tier") or r.get("Opp Def") or ""),
            "avg": float(avg),
            "label": {
                "passing_yards": "Pass Yards",
                "rushing_yards": "Rush Yards",
                "receiving_yards": "Rec Yards",
                "receptions": "Receptions",
                "rush_attempts": "Rush Attempts",
                "pass_attempts": "Pass Attempts",
                "pass_completions": "Pass Comp",
                "tackles_ast": "Tackles+Ast",
                "sacks": "Sacks",
                "kicking_points": "Kicking Points",
                "fg_made": "FG Made",
            }.get(prop, str(r.get("Prop"))),
        }
    )

best: dict = {}
for r in rows:
    k = (r["player"], r["canon"], r["side"], r["pick"])
    cur = best.get(k)
    if cur is None:
        best[k] = r
        continue
    if r["side"] == "OVER" and r["line"] < cur["line"]:
        best[k] = r
    elif r["side"] == "UNDER" and r["line"] > cur["line"]:
        best[k] = r

clears = sorted(
    best.values(),
    key=lambda z: (0 if z["pick"] == "goblin" else 1, -z["l10"], -z["cover"], z["player"]),
)
print(
    "clears",
    len(clears),
    "gob",
    sum(1 for x in clears if x["pick"] == "goblin"),
    "std",
    sum(1 for x in clears if x["pick"] == "standard"),
)
for r in clears[:40]:
    print(
        f"{r['pick'][0].upper()} {r['side'][0]} {r['player']:22} {r['label']:16} "
        f"{r['line']} L10={r['l10']:.0f} cov={r['cover']:+.1f} D={r['d'][:16]} ({r['team']})"
    )

seeds = [
    ("Woody Marks", "rushing_yards", 42),
    ("Mack Hollins", "receiving_yards", 51),
    ("Devaughn Vele", "receiving_yards", 69),
]
print("\nHandcheck seeds vs L5=5 clears:")
for name, prop, actual in seeds:
    m = [
        x
        for x in clears
        if name.lower().split()[-1] in x["player"].lower() and x["canon"] == prop
    ]
    if not m:
        print(f"  {name}: no L5=5 clear on board")
        continue
    for x in m:
        gap = actual - x["line"] if x["side"] == "OVER" else x["line"] - actual
        print(
            f"  {name} W1={actual} vs {x['pick']} {x['side']} {x['line']} "
            f"gap={gap:+.1f} L10={x['l10']:.0f}"
        )

out = Path(r"H:/PropORACLE/data/reports/nfl_l5eq5_clears_2026-09-27.json")
import json

out.write_text(json.dumps(clears, indent=2), encoding="utf-8")
print("wrote", out)
