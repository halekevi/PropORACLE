#!/usr/bin/env python3
"""Audit the 6-leg CFB slip from the user's PP screenshot."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO)]

from utils.cushion_z import l5_clears
from utils.cfb_keep_gates import cfb_list_eligible, cfb_ticket_fade, cfb_pass_yards_goblin_skip_d

SLATE = "2026-10-24"
# legs as shown on slip
LEGS = [
    ("Drew Mestemaker", "Pass Yards", "goblin", "OVER", 289.5),
    ("Dylan Edwards", "Longest Reception", "standard", "OVER", 11.5),
    ("DeKalon Taylor", "Receiving Yards", "standard", "OVER", 8.5),
    ("King Miller", "Rush Yards", "standard", "OVER", 72.5),
    ("JV Gibson", "Receiving Yards", "standard", "OVER", 52.5),
    ("Conner Weigman", "Pass Yards", "goblin", "OVER", 174.5),
]


def main() -> int:
    df = pd.read_csv(_REPO / f"outputs/{SLATE}/cfb/step5b_cfb.csv", low_memory=False)
    tip = df[df["start_time"].astype(str).str.startswith(SLATE)].copy()

    print(f"SLIP AUDIT  slate={SLATE}  n={len(LEGS)}  $20->$410 = 20.5x displayed")
    print("Ignore Power '1st place 37.5x' — use N-correct only.")
    print("=" * 72)

    for player, prop, pick, side, line in LEGS:
        m = tip[
            tip["player"].astype(str).str.contains(player.split()[0], case=False, na=False)
            & tip["player"].astype(str).str.contains(player.split()[-1], case=False, na=False)
            & tip["prop_type"].astype(str).str.contains(prop.split()[0], case=False, na=False)
        ]
        if "Longest" in prop:
            m = tip[
                tip["player"].astype(str).str.contains("Edwards", case=False, na=False)
                & tip["prop_type"].astype(str).str.contains("Longest Rec", case=False, na=False)
            ]
        if "DeKalon" in player:
            m = tip[
                tip["player"].astype(str).str.contains("DeKalon", case=False, na=False)
                & tip["prop_type"].astype(str).str.contains("Receiv", case=False, na=False)
            ]
        if "Miller" in player:
            m = tip[
                tip["player"].astype(str).str.contains("King Miller", case=False, na=False)
                & tip["prop_type"].astype(str).str.contains("Rush Yard", case=False, na=False)
            ]
        if "Gibson" in player:
            m = tip[
                tip["player"].astype(str).str.contains("JV Gibson", case=False, na=False)
                & tip["prop_type"].astype(str).str.contains("Receiv", case=False, na=False)
            ]
        if "Mestemaker" in player or "Weigman" in player:
            m = tip[
                tip["player"].astype(str).str.contains(player.split()[-1], case=False, na=False)
                & tip["prop_type"].astype(str).str.contains("Pass Yard", case=False, na=False)
            ]

        hit = m[pd.to_numeric(m["line"], errors="coerce") == line]
        if hit.empty:
            # nearest
            hit = m.copy()
        if hit.empty:
            print(f"{player} {pick} {side[0]}{line} {prop}")
            print("  → NOT IN step5b")
            print()
            continue

        # prefer exact pick_type
        exact = hit[hit["pick_type"].astype(str).str.lower() == pick]
        r = (exact if len(exact) else hit).iloc[0]
        vals = [float(r[f"stat_g{i}"]) for i in range(1, 6) if pd.notna(r.get(f"stat_g{i}"))]
        live_line = float(r["line"])
        pt = str(r["pick_type"]).lower()
        d = str(
            r.get("opp_def_tier")
            or r.get("OPP_OVERALL_DEF_TIER")
            or r.get("opp_overall_def_tier_nat")
            or ""
        )
        l5o = float(pd.to_numeric(r.get("l5_over"), errors="coerce") or 0)
        clears = bool(vals) and l5_clears(vals, line, side)
        hits = sum(1 for v in vals if v > line) if side == "OVER" else sum(1 for v in vals if v < line)
        avg = sum(vals) / len(vals) if vals else 0
        ratio = line / avg if avg else None

        rec = {
            "sport": "CFB",
            "player": player,
            "prop": prop,
            "Prop": prop,
            "pick_type": pick,
            "Pick Type": pick,
            "side": side,
            "Direction": side,
            "line": line,
            "Line": line,
            "def_tier": d,
            "Def Tier": d,
        }
        list_ok = cfb_list_eligible(rec)
        fade = cfb_ticket_fade(rec)
        skip_d = cfb_pass_yards_goblin_skip_d(rec)
        d_pass = skip_d or (
            (side == "OVER" and d in {"Weak", "Below Avg"})
            or (side == "UNDER" and d in {"Elite", "Above Avg"})
        )

        flags = []
        if l5o != 5 and not (len(vals) == 5 and clears):
            flags.append("L5!=5")
        if not clears:
            flags.append("FAILS_LINE")
        if abs(live_line - line) > 0.01:
            flags.append(f"step5b_line={live_line}")
        if pt != pick:
            flags.append(f"step5b_pick={pt}")
        if not list_ok:
            flags.append("LIST_FADE")
        if fade:
            flags.append("TICKET_FADE")
        if not d_pass and pick != "demon":
            flags.append(f"NO_D ({d or '?'})")
        if skip_d:
            flags.append("D_N/A_pass_yds")

        verdict = "OK" if clears and list_ok and not fade and (d_pass or skip_d) else "WEAK/HOLD"
        if not clears:
            verdict = "OUT"

        print(f"{player}  {pick} {side[0]}{line}  {prop}")
        print(f"  L5={vals}  avg={avg:.1f}  line/avg={ratio:.3f}  hits={hits}/5  D={d or '?'}")
        print(f"  flags={flags or ['clean']}  → {verdict}")
        print()

    print("=" * 72)
    print("NOTES")
    print("- Weigman Goblin O174.5 is EASIER than the O224.5 we live-cleared (still L5=5).")
    print("- Mestemaker Goblin O289.5 matches our live survivor.")
    print("- Gibson Std O52.5 matches our live survivor (D Weak).")
    print("- Edwards Longest Rec O11.5 cleared live earlier (D Below).")
    print("- Taylor Rec O8.5: D Above Avg → fails OVER D (on raw L5=5 list only).")
    print("- Miller Rush O72.5: D Above Avg → fails OVER D; line up from 69.5.")
    print("- Mixed Goblin+Standard 6-leg: use N-correct (To Win / Entry), not 37.5x 1st place.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
