"""Soft-stack Standard OVER rushing_yards: own OL + rush off + elite RB ± weak D/DL."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_espn_context import canon_nfl_abbr
from utils.nfl_player_names import norm_nfl_player_name
from utils.nfl_prop_gate_ledger import dedupe_unique_player_games, prepare_decided, rate

MAIN = Path(r"H:\PropORACLE_main_cp")
REPO = Path(r"H:\PropORACLE")
DATA = REPO / "Sports" / "NFL" / "data"
DATES = [
    "2026-09-09",
    "2026-09-10",
    "2026-09-13",
    "2026-09-14",
    "2026-09-20",
    "2026-09-21",
]

ELITE = {"Elite", "Above Avg", "Above"}
WEAK = {"Weak", "Below Avg", "Below"}


def tier_from_rank(r: object, n: int = 32) -> str:
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


def load(d: str) -> pd.DataFrame | None:
    for root in (MAIN, REPO):
        p = root / "outputs" / d / f"graded_nfl_{d}.xlsx"
        if not p.is_file():
            continue
        df = pd.read_excel(p, sheet_name="Box Raw")
        if df.empty or "result" not in df.columns:
            return None
        if int(df["result"].astype(str).str.upper().isin(["HIT", "MISS"]).sum()) == 0:
            return None
        out = df.copy()
        out["slate"] = d
        return out
    return None


def cell(g: pd.DataFrame) -> str:
    if g is None or len(g) == 0:
        return "--"
    h = int((g["result"] == "HIT").sum())
    n = len(g)
    return f"{h}/{n} = {rate(h, n):.0f}%"


def show(label: str, g: pd.DataFrame, base_hr: float | None = None) -> None:
    if g is None or len(g) == 0:
        print(f"  {label:55s}  --")
        return
    h = int((g["result"] == "HIT").sum())
    n = len(g)
    hr = rate(h, n)
    lift = ""
    if base_hr is not None and hr is not None:
        lift = f"  lift={hr - base_hr:+.1f}pp"
    print(f"  {label:55s}  {h:3d}/{n:<3d} = {hr:5.1f}%{lift}")


def main() -> None:
    boxes = [b for d in DATES if (b := load(d)) is not None]
    dec = prepare_decided(boxes)
    u = dedupe_unique_player_games(dec)
    u["tm"] = u.get("team", pd.Series("", index=u.index)).map(
        lambda x: canon_nfl_abbr(x) or str(x or "").upper()
    )
    # graded boxes often use team / opp_team
    if "opp_team" in u.columns:
        u["opp"] = u["opp_team"].map(lambda x: canon_nfl_abbr(x) or str(x or "").upper())
    else:
        u["opp"] = ""
    u["player_norm"] = u["player"].map(norm_nfl_player_name)

    # Own OL (prefer run_block for rush yards)
    ol = pd.read_csv(DATA / "nfl_ol_ranks.csv")
    ol["team"] = ol["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    ol["own_ol_tier"] = ol["tier"]
    ol["own_run_block_tier"] = ol["run_block_rank"].map(tier_from_rank)
    u = u.merge(
        ol[["team", "own_ol_tier", "own_run_block_tier", "ol_rank", "run_block_rank"]].rename(
            columns={"team": "tm", "ol_rank": "own_ol_rank", "run_block_rank": "own_run_block_rank"}
        ),
        on="tm",
        how="left",
    )

    # Own rush offense
    units = pd.read_csv(DATA / "nfl_team_unit_ranks.csv")
    units["team"] = units["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    units["own_rush_off_tier"] = units["rush_off_rank"].map(tier_from_rank)
    u = u.merge(
        units[["team", "own_rush_off_tier", "rush_off_rank"]].rename(
            columns={"team": "tm", "rush_off_rank": "own_rush_off_rank"}
        ),
        on="tm",
        how="left",
    )

    # Opp rush D + DL run-stop
    defense = pd.read_csv(DATA / "defense_rankings.csv")
    defense["team"] = defense["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    defense["opp_rush_d_tier"] = defense["rush_def_rank"].map(tier_from_rank)
    u = u.merge(
        defense[["team", "opp_rush_d_tier", "rush_def_rank"]].rename(
            columns={"team": "opp", "rush_def_rank": "opp_rush_def_rank"}
        ),
        on="opp",
        how="left",
    )
    dl = pd.read_csv(DATA / "nfl_dl_ranks.csv")
    dl["team"] = dl["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    dl["opp_dl_tier"] = dl["tier"]
    dl["opp_run_stop_tier"] = dl["run_stop_rank"].map(tier_from_rank)
    u = u.merge(
        dl[["team", "opp_dl_tier", "opp_run_stop_tier", "dl_rank", "run_stop_rank"]].rename(
            columns={
                "team": "opp",
                "dl_rank": "opp_dl_rank",
                "run_stop_rank": "opp_run_stop_rank",
            }
        ),
        on="opp",
        how="left",
    )

    # Elite RB: team #1 rush yards OR league_rank <= 16 (top half of 32-team starters)
    leaders = pd.read_csv(DATA / "nfl_stat_leaders.csv")
    rush = leaders[leaders["category"].astype(str).eq("rush_yds")].copy()
    rush["player_norm"] = rush["player_norm"].map(norm_nfl_player_name)
    rush["elite_rb"] = (rush["team_rank"] == 1) | (
        pd.to_numeric(rush["league_rank"], errors="coerce") <= 16
    )
    # also top-3 league as "super elite"
    rush["top16_rb"] = pd.to_numeric(rush["league_rank"], errors="coerce") <= 16
    rush["rb1"] = rush["team_rank"] == 1
    u = u.merge(
        rush[["player_norm", "elite_rb", "top16_rb", "rb1", "league_rank", "team_rank"]].rename(
            columns={"league_rank": "rush_league_rank", "team_rank": "rush_team_rank"}
        ),
        on="player_norm",
        how="left",
    )
    u["elite_rb"] = u["elite_rb"].fillna(False).astype(bool)
    u["rb1"] = u["rb1"].fillna(False).astype(bool)
    u["top16_rb"] = u["top16_rb"].fillna(False).astype(bool)

    # Snap floor as volume RB proxy
    rp = pd.read_csv(DATA / "nfl_route_participation.csv")
    rp["player_norm"] = rp["player"].map(norm_nfl_player_name)
    rp = rp.sort_values("as_of_week").drop_duplicates("player_norm", keep="last")
    u = u.merge(
        rp[["player_norm", "snap_pct_L3", "snap_pct_season", "position"]],
        on="player_norm",
        how="left",
    )
    snap = pd.to_numeric(u["snap_pct_L3"], errors="coerce")
    snap = snap.fillna(pd.to_numeric(u["snap_pct_season"], errors="coerce"))
    u["high_snap_rb"] = snap.ge(55) & u.get("position", pd.Series("", index=u.index)).astype(
        str
    ).str.upper().isin({"RB", "FB", ""})
    # if position missing, still allow elite_rb / rb1
    u["volume_rb"] = u["elite_rb"] | u["rb1"] | u["high_snap_rb"].fillna(False)

    # Flags
    u["own_ol_elite"] = u["own_ol_tier"].isin(ELITE) | u["own_run_block_tier"].isin(ELITE)
    u["own_rush_off_elite"] = u["own_rush_off_tier"].isin(ELITE)
    u["opp_rush_d_weak"] = u["opp_rush_d_tier"].isin(WEAK)
    u["opp_dl_weak"] = u["opp_dl_tier"].isin(WEAK) | u["opp_run_stop_tier"].isin(WEAK)

    g = u[
        (u.prop == "rushing_yards") & (u.side == "OVER") & (u.pick == "Standard")
    ].copy()
    print(f"Standard OVER rushing_yards unique-game n={len(g)}")
    print(
        "coverage: ol="
        f"{g['own_ol_tier'].notna().mean()*100:.0f}% "
        f"rushOff={g['own_rush_off_tier'].notna().mean()*100:.0f}% "
        f"eliteRB={g['elite_rb'].mean()*100:.0f}% "
        f"oppRushD={g['opp_rush_d_tier'].notna().mean()*100:.0f}% "
        f"oppDL={g['opp_dl_tier'].notna().mean()*100:.0f}%"
    )

    base = g
    base_hr = rate(int((base.result == "HIT").sum()), len(base))
    g5 = g[g.l5 == 5]
    g4 = g[g.l5 >= 4]
    g70 = g[g.gate70]

    print("\n### Baselines")
    show("base (all)", base)
    show("L5>=4", g4, base_hr)
    show("L5=5", g5, base_hr)
    show("G70", g70, base_hr)

    print("\n### Single factors (all Standard OVER rush yards)")
    show("own OL Elite/Above (or run-block)", g[g.own_ol_elite], base_hr)
    show("own Rush Off Elite/Above", g[g.own_rush_off_elite], base_hr)
    show("elite RB (team RB1 or league top-16)", g[g.elite_rb], base_hr)
    show("RB1 only (team #1 rush yds)", g[g.rb1], base_hr)
    show("opp Rush D Weak/Below", g[g.opp_rush_d_weak], base_hr)
    show("opp DL/run-stop Weak/Below", g[g.opp_dl_weak], base_hr)

    offense_stack = g.own_ol_elite & g.own_rush_off_elite & g.elite_rb
    print("\n### Offense stack: Elite OL + Elite Rush Off + Elite RB")
    show("OL + RushOff + Elite RB", g[offense_stack], base_hr)
    show("  ... and L5>=4", g[offense_stack & (g.l5 >= 4)], base_hr)
    show("  ... and L5=5", g[offense_stack & (g.l5 == 5)], base_hr)

    print("\n### + Weak defense (opp Rush D Weak/Below)")
    s2 = offense_stack & g.opp_rush_d_weak
    show("Offense stack + Weak Rush D", g[s2], base_hr)
    show("  ... and L5>=4", g[s2 & (g.l5 >= 4)], base_hr)
    show("  ... and L5=5", g[s2 & (g.l5 == 5)], base_hr)

    print("\n### + Weak DL (opp DL / run-stop Weak/Below)")
    s3 = offense_stack & g.opp_dl_weak
    show("Offense stack + Weak DL", g[s3], base_hr)
    show("  ... and L5>=4", g[s3 & (g.l5 >= 4)], base_hr)
    show("  ... and L5=5", g[s3 & (g.l5 == 5)], base_hr)

    print("\n### Full double-lock: Offense stack + Weak Rush D + Weak DL")
    s4 = offense_stack & g.opp_rush_d_weak & g.opp_dl_weak
    show("FULL stack", g[s4], base_hr)
    show("  ... and L5>=4", g[s4 & (g.l5 >= 4)], base_hr)
    show("  ... and L5=5", g[s4 & (g.l5 == 5)], base_hr)

    # Also: offense stack without requiring elite RB (in case RB tag thin)
    soft = g.own_ol_elite & g.own_rush_off_elite
    print("\n### Softer offense (no RB tag): Elite OL + Elite Rush Off")
    show("OL + RushOff", g[soft], base_hr)
    show("OL + RushOff + Weak Rush D", g[soft & g.opp_rush_d_weak], base_hr)
    show("OL + RushOff + Weak DL", g[soft & g.opp_dl_weak], base_hr)
    show(
        "OL + RushOff + Weak Rush D + Weak DL",
        g[soft & g.opp_rush_d_weak & g.opp_dl_weak],
        base_hr,
    )
    show(
        "OL + RushOff + Weak D/DL + L5>=4",
        g[soft & g.opp_rush_d_weak & g.opp_dl_weak & (g.l5 >= 4)],
        base_hr,
    )

    # List the full-stack rows so user can see who fired
    if len(g[s4]):
        print("\nFULL stack rows:")
        cols = [
            c
            for c in [
                "player",
                "tm",
                "opp",
                "line",
                "actual",
                "result",
                "l5",
                "own_ol_tier",
                "own_rush_off_tier",
                "rush_team_rank",
                "opp_rush_d_tier",
                "opp_dl_tier",
            ]
            if c in g.columns
        ]
        print(g.loc[s4, cols].to_string(index=False))
    if len(g[offense_stack]):
        print("\nOffense-stack rows (no D filter):")
        cols = [
            c
            for c in [
                "player",
                "tm",
                "opp",
                "line",
                "actual",
                "result",
                "l5",
                "own_ol_tier",
                "own_rush_off_tier",
                "opp_rush_d_tier",
                "opp_dl_tier",
            ]
            if c in g.columns
        ]
        print(g.loc[offense_stack, cols].to_string(index=False))


if __name__ == "__main__":
    main()
