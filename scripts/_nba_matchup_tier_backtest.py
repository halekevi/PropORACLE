"""Do NBA props hit more when a strong offense meets a weak defense (Overs) and vice versa (Unders)?

Team offense / defense ratings are rebuilt as of each slate date from games
strictly before it (no lookahead), ranked 1-30, and cut into tiers:
Elite 1-6, Above 7-12, Avg 13-18, Below 19-24, Weak 25-30.

Reads the leg CSVs from _nba_std_season_gap_backtest.py (Standard and Goblin).

  py -3.14 scripts/_nba_matchup_tier_backtest.py
"""
from __future__ import annotations

import math
import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "Sports" / "NBA" / "scripts"))
from defense_report import norm_team_abbr  # noqa: E402

TIERS = ["Elite", "Above", "Avg", "Below", "Weak"]


def tier(rank: float) -> str | None:
    if pd.isna(rank):
        return None
    return TIERS[min(int((rank - 1) // 6), 4)]


def team_games() -> pd.DataFrame:
    con = sqlite3.connect(ROOT / "data" / "cache" / "proporacle_ref.db")
    b = pd.read_sql(
        "SELECT game_date, event_id, home_team, away_team, team, pts, fga, fta, tov, oreb FROM nba "
        "WHERE season_window IS NULL AND game_date >= '2026-10-20'", con)
    s = ["pts", "fga", "fta", "tov", "oreb"]
    t = b.groupby(["game_date", "event_id", "home_team", "away_team", "team"], as_index=False)[s].sum()
    t["opp"] = t.apply(lambda r: r.away_team if r.team == r.home_team else r.home_team, axis=1)
    m = t.merge(t[["event_id", "team"] + s], left_on=["event_id", "opp"], right_on=["event_id", "team"], suffixes=("", "_a"))
    m["poss"] = m.fga + 0.44 * m.fta + m.tov - m.oreb
    m["poss_a"] = m.fga_a + 0.44 * m.fta_a + m.tov_a - m.oreb_a
    m["team"] = m["team"].map(norm_team_abbr)
    return m[["game_date", "team", "pts", "pts_a", "poss", "poss_a"]]


def ranks_asof(games: pd.DataFrame, dates: list[str], min_games: int = 3) -> pd.DataFrame:
    out = []
    for d in dates:
        g = games[games.game_date < d].groupby("team").agg(
            n=("pts", "size"), pts=("pts", "sum"), pts_a=("pts_a", "sum"), poss=("poss", "sum"), poss_a=("poss_a", "sum"))
        g = g[g.n >= min_games]
        if len(g) < 20:
            continue
        g["off_rank"] = (100 * g.pts / g.poss).rank(ascending=False, method="min")
        g["def_rank"] = (100 * g.pts_a / g.poss_a).rank(ascending=True, method="min")
        g["slate"] = d
        out.append(g.reset_index()[["slate", "team", "off_rank", "def_rank"]])
    return pd.concat(out, ignore_index=True)


def wilson_lo(k: int, n: int, z: float = 1.96) -> float:
    if not n:
        return float("nan")
    p = k / n
    return (p + z * z / (2 * n) - z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n)


def attach(legs: pd.DataFrame, r: pd.DataFrame) -> pd.DataFrame:
    legs = legs.copy()
    legs["team_n"] = legs["team"].map(norm_team_abbr)
    legs["opp_n"] = legs["opp"].map(norm_team_abbr)
    own = r.rename(columns={"team": "team_n", "off_rank": "own_off", "def_rank": "own_def"})
    opp = r.rename(columns={"team": "opp_n", "off_rank": "opp_off", "def_rank": "opp_def"})
    legs = legs.merge(own, on=["slate", "team_n"], how="inner").merge(opp, on=["slate", "opp_n"], how="inner")
    legs["own_off_t"] = legs.own_off.map(tier)
    legs["opp_def_t"] = legs.opp_def.map(tier)
    return legs


def grid(d: pd.DataFrame, hit: str) -> pd.DataFrame:
    p = d.pivot_table(index="own_off_t", columns="opp_def_t", values=hit, aggfunc="mean") * 100
    return p.reindex(index=TIERS, columns=TIERS).round(1)


def cell(d: pd.DataFrame, m: pd.Series, hit: str) -> str:
    n, k = int(m.sum()), int(d.loc[m, hit].sum())
    return f"n={n:5}  hit={100 * k / max(n, 1):5.1f}%  low={100 * wilson_lo(k, n):5.1f}%"


def main() -> None:
    games = team_games()
    for label, f in (("STANDARD", "nba_std_season_gap_backtest.csv"), ("GOBLIN", "nba_goblin_season_gap_backtest.csv")):
        legs = pd.read_csv(ROOT / "data" / "reports" / f)
        legs = legs[legs.push == 0]
        r = ranks_asof(games, sorted(legs.slate.unique()))
        d = attach(legs, r)
        good_o = d.own_off_t.isin(["Elite", "Above"])
        bad_o = d.own_off_t.isin(["Below", "Weak"])
        weak_d = d.opp_def_t.isin(["Below", "Weak"])
        good_d = d.opp_def_t.isin(["Elite", "Above"])
        print(f"\n================ {label}  ({len(d)} legs with as-of ranks)")
        print(f"OVER baseline      {cell(d, pd.Series(True, index=d.index), 'over_hit')}")
        print(f"OVER  strong O vs weak D   {cell(d, good_o & weak_d, 'over_hit')}")
        print(f"OVER  Elite O vs Weak D    {cell(d, (d.own_off_t == 'Elite') & (d.opp_def_t == 'Weak'), 'over_hit')}")
        print(f"OVER  weak O vs strong D   {cell(d, bad_o & good_d, 'over_hit')}")
        if label == "STANDARD":
            print(f"UNDER baseline     {cell(d, pd.Series(True, index=d.index), 'under_hit')}")
            print(f"UNDER weak O vs strong D   {cell(d, bad_o & good_d, 'under_hit')}")
            print(f"UNDER Weak O vs Elite D    {cell(d, (d.own_off_t == 'Weak') & (d.opp_def_t == 'Elite'), 'under_hit')}")
            print(f"UNDER strong O vs weak D   {cell(d, good_o & weak_d, 'under_hit')}")
        print("\nOver rate grid: rows = player's team offense, columns = opponent defense")
        print(grid(d, "over_hit").to_string())


if __name__ == "__main__":
    main()
