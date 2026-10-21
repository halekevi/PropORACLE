"""Best NFL ticket props for 2026-09-24 from step8 + soft unit gates."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_espn_context import canon_nfl_abbr
from utils.nfl_keep_gates import (
    NFL_GOBLIN_TICKETS_ENABLED,
    nfl_goblin_ticket_eligible,
    nfl_receptions_under_soft_rank_bump,
    nfl_soft_unit_ticket_ok,
    nfl_standard_ticket_eligible,
    nfl_ticket_priority,
)
from utils.nfl_unit_line_ranks import attach_nfl_unit_line_ranks
from utils.ticket_70_pool import ticket_gate_passes

REPO = Path(r"H:\PropORACLE")
STEP8 = Path(r"H:\PropORACLE_main_cp\outputs\2026-09-24\nfl\step8_nfl_direction_clean.xlsx")
DATA = REPO / "Sports" / "NFL" / "data"
ELITE = {"Elite", "Above Avg", "Above"}


def tier_from_rank(r: object) -> str:
    try:
        v = float(r)
    except (TypeError, ValueError):
        return ""
    if v != v:
        return ""
    if v <= 8:
        return "Elite"
    if v <= 14:
        return "Above"
    if v <= 20:
        return "Avg"
    if v <= 26:
        return "Below"
    return "Weak"


def to_row(r: pd.Series) -> dict:
    prop = str(r.get("Prop") or "").strip().lower().replace(" ", "_").replace("-", "_")
    side = str(r.get("Direction") or "").strip().upper()
    pick = str(r.get("Pick Type") or "Standard").strip().title()
    d = {
        "sport": "NFL",
        "league": "NFL",
        "player": r.get("Player"),
        "team": r.get("Team"),
        "opp_team": r.get("Opp"),
        "prop": prop,
        "side": side,
        "pick_type": pick,
        "line": r.get("Line"),
        "def_tier": r.get("Def Tier"),
        "l5_over": r.get("L5 Over"),
        "l5_under": r.get("L5 Under"),
        "l10_over": r.get("L10 Over"),
        "l10_under": r.get("L10 Under"),
        "cover": r.get("Edge"),
        "dist_l5": r.get("Edge"),
        "opp_pass_def_rank": r.get("Opp Pass Def Rank"),
        "opp_rush_def_rank": r.get("Opp Rush Def Rank"),
        "own_pass_off_rank": r.get("Team Pass Off Rank"),
        "own_rush_off_rank": r.get("Team Rush Off Rank"),
        "tier": r.get("Tier"),
        "game_time": r.get("Game Time"),
        "l5_sample_n": 5,  # step8 board already shows L5; allow cross-season assembly
    }
    # soft tiers from ranks already on board
    d["opp_pass_d_tier"] = tier_from_rank(d["opp_pass_def_rank"])
    d["opp_rush_d_tier"] = tier_from_rank(d["opp_rush_def_rank"])
    d["own_pass_off_tier"] = tier_from_rank(d["own_pass_off_rank"])
    d["own_rush_off_tier"] = tier_from_rank(d["own_rush_off_rank"])
    return d


def main() -> None:
    raw = pd.read_excel(STEP8)
    # tonight only (Thu night) + rest of slate if multi-day; filter ET day
    raw["_gt"] = pd.to_datetime(raw["Game Time"], utc=True, errors="coerce")
    raw["_day"] = raw["_gt"].dt.tz_convert("America/New_York").dt.date
    today = pd.Timestamp("2026-09-24", tz="America/New_York").date()
    raw = raw[raw["_day"] == today].copy()
    print(f"TNF board rows={len(raw)}  goblin_flag={NFL_GOBLIN_TICKETS_ENABLED}")

    # attach OL/DL for soft gates that need them
    tmp = raw.copy()
    tmp["tm"] = tmp["Team"].map(lambda x: canon_nfl_abbr(x) or str(x or "").upper())
    tmp["opp"] = tmp["Opp"].map(lambda x: canon_nfl_abbr(x) or str(x or "").upper())
    tmp = attach_nfl_unit_line_ranks(tmp, REPO, team_col="tm", opp_col="opp")

    # unit ranks for pass/rush off if missing
    units = pd.read_csv(DATA / "nfl_team_unit_ranks.csv")
    units["team"] = units["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    units["own_pass_off_tier"] = units["pass_off_rank"].map(tier_from_rank)
    units["own_rush_off_tier"] = units["rush_off_rank"].map(tier_from_rank)
    tmp = tmp.merge(
        units[["team", "own_pass_off_tier", "own_rush_off_tier", "pass_off_rank", "rush_off_rank"]].rename(
            columns={"team": "tm", "pass_off_rank": "u_pass_off_rank", "rush_off_rank": "u_rush_off_rank"}
        ),
        on="tm",
        how="left",
    )

    clears = []
    near = []
    for i, r in tmp.iterrows():
        d = to_row(r)
        # prefer unit-csv tiers when present
        if pd.notna(r.get("own_pass_off_tier")):
            d["own_pass_off_tier"] = r["own_pass_off_tier"]
        if pd.notna(r.get("own_rush_off_tier")):
            d["own_rush_off_tier"] = r["own_rush_off_tier"]
        if pd.notna(r.get("own_ol_tier")):
            d["own_ol_tier"] = r["own_ol_tier"]
        if pd.notna(r.get("opp_dl_pass_tier")):
            d["opp_dl_pass_tier"] = r["opp_dl_pass_tier"]
        # wopr if present
        for k in ("WOPR", "wopr_L3", "WOPR L3", "Prop Share %"):
            if k in r.index and pd.notna(r.get(k)):
                if "share" in k.lower():
                    continue
                d["wopr"] = r.get(k)
                break
        # prop share as weak wopr proxy? skip

        allow = nfl_standard_ticket_eligible(d) or (
            NFL_GOBLIN_TICKETS_ENABLED and nfl_goblin_ticket_eligible(d)
        )
        if not allow:
            # still show soft-ok allowlist misses for near
            if d["pick_type"] == "Standard" and (
                (d["side"] == "UNDER" and d["prop"] in {
                    "receptions","longest_reception","sacks","tackles_ast",
                    "kicking_points","receiving_yards","passing_yards","pass_attempts",
                    "pass_completions","completion_percentage","passing_tds",
                })
                or (d["side"] == "OVER" and d["prop"] in {
                    "rushing_yards","punts","rush_yards_per_carry","longest_completion",
                    "passing_yards","passing_tds",
                })
            ):
                soft = nfl_soft_unit_ticket_ok(d)
                gate = ticket_gate_passes(d)
                if soft and not gate:
                    near.append((d, "soft_ok_gate_fail", r))
                elif gate and not soft:
                    near.append((d, "gate_ok_soft_fail", r))
            continue
        if not ticket_gate_passes(d):
            near.append((d, "allow_soft_ok_g70_fail", r))
            continue
        # Drop empty L5/L10 on non-skip props (punts often blank)
        from utils.ticket_70_pool import _l5 as __l5, _l10 as __l10

        if d["prop"] not in ("receptions", "receiving_yards"):
            if __l5(d) is None or __l10(d) is None:
                continue
        bump = nfl_receptions_under_soft_rank_bump(d)
        pri = nfl_ticket_priority(d)
        l5 = d["l5_under"] if d["side"] == "UNDER" else d["l5_over"]
        l10 = d["l10_under"] if d["side"] == "UNDER" else d["l10_over"]
        clears.append(
            {
                "pri": pri,
                "bump": bump,
                "player": d["player"],
                "team": d["team"],
                "opp": d["opp_team"],
                "prop": d["prop"],
                "side": d["side"],
                "pick": d["pick_type"],
                "line": d["line"],
                "l5": l5,
                "l10": l10,
                "def": d["def_tier"],
                "pass_d": d.get("opp_pass_d_tier"),
                "rush_d": d.get("opp_rush_d_tier"),
                "pass_off": d.get("own_pass_off_tier"),
                "ol": d.get("own_ol_tier"),
                "dlp": d.get("opp_dl_pass_tier"),
                "tier": d.get("tier"),
                "game": str(d.get("game_time") or "")[:22],
            }
        )

    clears.sort(key=lambda x: (x["pri"], x["bump"], -(x["l5"] or 0), -(x["l10"] or 0), str(x["player"])))
    print(f"\n=== TICKET-CLEAR (allowlist + soft stack + G70) n={len(clears)} ===\n")
    for c in clears[:40]:
        flag = " [OptionC bump]" if c["bump"] == 0 else ""
        print(
            f"{c['pick']:8s} {c['side']:5s} {c['prop']:24s}  "
            f"{c['player']:22s} {c['team']:3s} vs {c['opp']:3s}  "
            f"O/U {c['line']}  L5={c['l5']} L10={c['l10']} D={c['def']}  "
            f"PassD={c['pass_d']} PassOff={c['pass_off']}{flag}"
        )

    # group counts
    if clears:
        g = pd.DataFrame(clears)
        print("\nBy prop:")
        print(g.groupby(["pick", "side", "prop"]).size().sort_values(ascending=False).to_string())

    print(f"\nNear-misses sample n={len(near)}")
    for d, why, _ in near[:15]:
        print(
            f"  {why:24s} {d.get('pick_type')} {d.get('side')} "
            f"{d.get('prop'):20s} {d.get('player')}"
        )


if __name__ == "__main__":
    main()
