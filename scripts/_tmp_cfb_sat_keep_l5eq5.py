#!/usr/bin/env python3
"""CFB Sat 2026-10-24 keep-gate L5=5 from step5b (no step8 yet)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO)]

from utils.cfb_keep_gates import cfb_list_eligible, cfb_pass_yards_goblin_skip_d, cfb_ticket_fade

SLATE = "2026-10-24"


def _col(r, *names):
    for n in names:
        if n in r.index and pd.notna(r[n]):
            return r[n]
    return None


def main() -> int:
    p = _REPO / f"outputs/{SLATE}/cfb/step5b_cfb.csv"
    df = pd.read_csv(p, low_memory=False)
    today = df[df["start_time"].astype(str).str.startswith(SLATE)].copy()
    print(f"CFB keep L5=5  slate={SLATE}  tip_rows={len(today)}  source={p.relative_to(_REPO)}")
    print("List gate: cfb_list_eligible (no Rush Att / Kick Pts Goblin). Demons dropped.")
    print()

    buckets: dict[str, list] = {
        "Goblin OVER": [],
        "Standard OVER": [],
        "Standard UNDER": [],
    }
    for _, r in today.iterrows():
        pt = str(_col(r, "pick_type", "odds_type") or "").strip().lower()
        if pt not in {"goblin", "standard"}:
            continue
        prop = str(_col(r, "prop_type", "stat_type") or "").strip()
        if not prop:
            continue
        try:
            line = float(_col(r, "line"))
        except (TypeError, ValueError):
            continue
        l5o = pd.to_numeric(_col(r, "l5_over"), errors="coerce")
        l5u = pd.to_numeric(_col(r, "l5_under"), errors="coerce")
        vals: list[float] = []
        for i in range(1, 6):
            v = pd.to_numeric(r.get(f"stat_g{i}"), errors="coerce")
            if pd.isna(v):
                break
            vals.append(float(v))
        if len(vals) < 5 or all(v == 0 for v in vals):
            continue
        l5avg = sum(vals) / len(vals)
        player = str(_col(r, "player") or "").strip()
        if not player or "+" in player:
            continue
        team = str(_col(r, "team_abbr", "team") or "")
        opp = str(_col(r, "opp_team_abbr", "pp_opp_team") or "")
        def_tier = str(
            _col(r, "opp_def_tier", "OPP_OVERALL_DEF_TIER", "opp_overall_def_tier_nat")
            or ""
        )
        flag = str(_col(r, "season_l5_flag") or "")
        tip = str(_col(r, "start_time") or "")[:16].replace("T", " ")

        candidates: list[tuple[str, str]] = []
        if pt == "goblin" and float(l5o) == 5.0:
            candidates.append(("OVER", "Goblin OVER"))
        elif pt == "standard":
            if float(l5o) == 5.0:
                candidates.append(("OVER", "Standard OVER"))
            if float(l5u) == 5.0:
                candidates.append(("UNDER", "Standard UNDER"))

        for side, key in candidates:
            rec = {
                "sport": "CFB",
                "player": player,
                "team": team,
                "opp": opp,
                "prop": prop,
                "pick_type": pt,
                "side": side,
                "line": line,
                "def_tier": def_tier,
                "Prop": prop,
                "Pick Type": pt,
                "Direction": side,
                "Line": line,
                "L5 Over": l5o,
                "L5 Under": l5u,
                "Def Tier": def_tier,
                "Season L5 Flag": flag,
                "l5avg": l5avg,
                "tip": tip,
            }
            if not cfb_list_eligible(rec):
                continue
            skip_d = cfb_pass_yards_goblin_skip_d(rec)
            d = def_tier.strip()
            d_pass = skip_d or (
                (side == "OVER" and d in {"Weak", "Below Avg"})
                or (side == "UNDER" and d in {"Elite", "Above Avg"})
            )
            fade = cfb_ticket_fade(rec)
            buckets[key].append(
                {**rec, "d_pass": d_pass, "skip_d": skip_d, "ticket_fade": fade}
            )

    lines_out: list[str] = [
        f"CFB keep L5=5  slate={SLATE}",
        f"source={p.relative_to(_REPO)}  tip_rows={len(today)}",
        "",
    ]
    for k, rows in buckets.items():
        rows.sort(
            key=lambda x: (
                0 if x["d_pass"] else 1,
                1 if x["ticket_fade"] else 0,
                -(x["l5avg"] - x["line"]),
                x["player"],
            )
        )
        hdr = f"===== KEEP L5=5 {k} (n={len(rows)}) ====="
        print(hdr)
        lines_out.append(hdr)
        if not rows:
            print("  (none)")
            lines_out.append("  (none)")
            print()
            lines_out.append("")
            continue
        for i, rec in enumerate(rows, 1):
            if rec["skip_d"]:
                dtag = "D N/A (pass yds)"
            elif rec["d_pass"]:
                dtag = f"D-pass {rec['def_tier']}"
            else:
                dtag = f"no-D {rec['def_tier'] or '?'}"
            fade = " [ticket-fade]" if rec["ticket_fade"] else ""
            flag = rec["Season L5 Flag"]
            flag_s = f" flag={flag}" if flag and flag not in {"nan", ""} else ""
            row = (
                f"  {i}. {rec['player']} ({rec['team']} vs {rec['opp']})  "
                f"{rec['prop']}  {rec['side'][0]}{rec['line']:g}  "
                f"L5avg={rec['l5avg']:.1f}  {dtag}{fade}{flag_s}"
            )
            print(row)
            lines_out.append(row)
        print()
        lines_out.append("")

    out = _REPO / f"outputs/{SLATE}/cfb/keep_l5eq5_{SLATE}.txt"
    out.write_text("\n".join(lines_out) + "\n", encoding="utf-8")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
