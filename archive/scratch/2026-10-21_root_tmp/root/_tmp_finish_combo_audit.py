#!/usr/bin/env python3
"""Finish combo audit JSON write (ascii-safe) + canvas note."""
from __future__ import annotations

import json
from pathlib import Path

# Re-load from prior run if needed — recompute summary from the failed script's logic
# by reading a minimal re-summary from the printed pattern we already have.

summary = {
    "audited": "2026-09-18",
    "question": "Do other Combo props share MLB strikeouts_combo NO_ACTUAL + A+B failure?",
    "verdict": (
        "Yes for several sports — not unique to pitcher Ks. Every Combo row is a "
        "two-name 'A + B' string. WNBA Combos mostly WORK (VOID ~8%); MLB/NHL and "
        "many Soccer Combos fail the same way (VOID 90-100%, null actual). "
        "slate_grader already has _split_combo_players + multi-player sum for some "
        "paths; MLB/NHL/Soccer combo legs are not getting actuals filled."
    ),
    "same_shape_as_ks_combo": [
        {"sport": "MLB", "prop": "Pitcher Strikeouts (Combo)", "n": 1011, "void_pct": 100.0, "void_reason": "NO_ACTUAL"},
        {"sport": "NHL", "prop": "Shots On Goal (Combo)", "n": 53, "void_pct": 100.0, "void_reason": "NO_ACTUAL"},
        {"sport": "NHL", "prop": "shots_on_goal_(combo)", "n": 49, "void_pct": 100.0},
        {"sport": "NHL", "prop": "Goals (Combo) / goals_(combo)", "n": 3, "void_pct": 100.0},
        {"sport": "Soccer", "prop": "Passes Attempted (Combo)", "n": 1302, "void_pct": 97.9, "void_reason": "NO_DATA/NO_ACTUAL"},
        {"sport": "Soccer", "prop": "Clearances (Combo)", "n": 178, "void_pct": 97.8},
        {"sport": "Soccer", "prop": "Goals (Combo)", "n": 102, "void_pct": 91.2},
        {"sport": "CBB", "prop": "points (combo)", "n": 78, "void_pct": 93.6},
    ],
    "mostly_working": [
        {"sport": "WNBA", "prop": "Points (Combo)", "n": 1735, "void_pct": 8.3, "note": "proof combo assembly CAN work"},
        {"sport": "WNBA", "prop": "Rebounds (Combo)", "n": 641, "void_pct": 7.8},
        {"sport": "WNBA", "prop": "Assists (Combo)", "n": 435, "void_pct": 7.4},
        {"sport": "WNBA", "prop": "3-PT Made (Combo)", "n": 302, "void_pct": 7.9},
        {"sport": "NFL", "prop": "player_touchdowns_combo", "n": 28, "void_pct": 28.6},
        {"sport": "CFB", "prop": "Rush Yards (Combo)", "n": 18, "void_pct": 0.0},
    ],
    "partial_nba": [
        {"sport": "NBA", "prop": "Points (Combo)", "n": 2430, "void_pct": 39.5, "note": "split logic exists; still high NO_PROJECTION_OR_LINE / NO_ACTUAL"},
        {"sport": "NBA", "prop": "Rebounds (Combo)", "n": 926, "void_pct": 38.3},
        {"sport": "NBA", "prop": "Assists (Combo)", "n": 897, "void_pct": 36.2},
        {"sport": "NBA", "prop": "3-PT Made (Combo)", "n": 381, "void_pct": 45.9},
    ],
    "fix_hint": (
        "scripts/grading/slate_grader.py already has _split_combo_players and "
        "multi-player sum (~L1210). MLB/NHL/Soccer combo grading paths need to "
        "hit that resolver with per-leg actuals lookups. WNBA is the working reference."
    ),
}

out = Path(r"H:/PropORACLE/data/reports/audit_combo_props_void_cross_sport.json")
out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
print("wrote", out)

# Canvas patch
canvas = Path(
    r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/active-sport-prop-hit-rates.canvas.tsx"
)
t = canvas.read_text(encoding="utf-8")
needle = 'title="Pitcher Ks (Combo) = computation failure (audited)"'
if needle not in t:
    raise SystemExit("ks combo callout missing")
# Expand that callout's body — find Callout block
start = t.rfind("<Callout", 0, t.find(needle))
end = t.find("</Callout>", t.find(needle)) + len("</Callout>")
old = t[start:end]
new = """<Callout tone="danger" title="Combo VOID family — not just pitcher Ks">
            Pitcher Strikeouts (Combo) is one instance of a cross-sport pattern:
            every Combo row is a two-name &quot;A + B&quot; string. Same failure
            (VOID 90–100%, null actual): MLB Pitcher Ks Combo, NHL SOG/Goals Combo,
            Soccer Passes/Clearances/Goals Combo, CBB points (combo). Counterexample:
            WNBA Points/Rebs/Asts/3PT Combo mostly WORK (VOID ~8%) — so assembly is
            fixable. slate_grader already has _split_combo_players; MLB/NHL/Soccer
            paths are not filling per-leg actuals. See
            data/reports/audit_combo_props_void_cross_sport.json.
          </Callout>"""
t = t[:start] + new + t[end:]
canvas.write_text(t, encoding="utf-8")
print("canvas updated")
