"""NBA team ranks by prop type: per-game production (offense) and per-game allowed (defense).

Built from the nightly ESPN box DB (current regular season). One row per team x prop.
  for_pg / for_rank        team produces (rank 1 = most)
  allowed_pg / allowed_rank opponents produce vs this team (rank 1 = least allowed = best defense)
  allowed_tier             Elite 1-6, Above Avg 7-12, Avg 13-18, Below Avg 19-24, Weak 25-30
Turnovers are inverted for defense: allowed_pg is turnovers forced, rank 1 = most forced.

  py -3.14 Sports/NBA/scripts/build_team_prop_ranks.py [--season 2026-27] [--asof YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

from defense_report import current_season_label, norm_team_abbr

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "Sports" / "NBA" / "data" / "cache" / "team_prop_ranks.csv"
STATS = ["pts", "reb", "ast", "fg3m", "fg3a", "fgm", "fga", "ftm", "fta", "fg2m", "fg2a", "stl", "blk", "tov", "oreb", "dreb"]
PROPS = {
    "pts": "Points", "reb": "Rebounds", "ast": "Assists", "fg3m": "3PT Made", "fg3a": "3PT Attempted",
    "fgm": "FG Made", "fga": "FG Attempted", "ftm": "Free Throws Made", "fta": "Free Throws Attempted",
    "fg2m": "Two Pointers Made", "fg2a": "Two Pointers Attempted", "stl": "Steals", "blk": "Blocked Shots",
    "tov": "Turnovers", "oreb": "Offensive Rebounds", "dreb": "Defensive Rebounds",
    "pra": "Pts+Rebs+Asts", "pr": "Pts+Rebs", "pa": "Pts+Asts", "ra": "Rebs+Asts", "bs": "Blks+Stls",
}
TIERS = ["Elite", "Above Avg", "Avg", "Below Avg", "Weak"]


def build(season: str, asof: str = "") -> pd.DataFrame:
    start = int(season[:4])
    hi = asof or f"{start + 1}-07-01"
    con = sqlite3.connect(ROOT / "data" / "cache" / "proporacle_ref.db")
    b = pd.read_sql(
        f"SELECT event_id, home_team, away_team, team, {', '.join(STATS)} FROM nba "
        "WHERE season_window IS NULL AND game_date >= ? AND game_date < ?",
        con, params=(f"{start}-10-01", hi))
    con.close()
    t = b.groupby(["event_id", "home_team", "away_team", "team"], as_index=False)[STATS].sum()
    t["opp"] = t.apply(lambda r: r.away_team if r.team == r.home_team else r.home_team, axis=1)
    m = t.merge(t[["event_id", "team"] + STATS], left_on=["event_id", "opp"], right_on=["event_id", "team"],
                suffixes=("", "_a"))
    for side in ("", "_a"):
        m[f"pra{side}"] = m[f"pts{side}"] + m[f"reb{side}"] + m[f"ast{side}"]
        m[f"pr{side}"] = m[f"pts{side}"] + m[f"reb{side}"]
        m[f"pa{side}"] = m[f"pts{side}"] + m[f"ast{side}"]
        m[f"ra{side}"] = m[f"reb{side}"] + m[f"ast{side}"]
        m[f"bs{side}"] = m[f"blk{side}"] + m[f"stl{side}"]
    m["team"] = m["team"].map(norm_team_abbr)
    gp = m.groupby("team")["event_id"].nunique()
    rows = []
    for key, name in PROPS.items():
        f = m.groupby("team")[key].mean()
        a = m.groupby("team")[f"{key}_a"].mean()
        best_def_low = key != "tov"
        df = pd.DataFrame({
            "team": f.index, "prop": name, "prop_key": key, "gp": gp.reindex(f.index).values,
            "for_pg": f.values.round(2), "allowed_pg": a.reindex(f.index).values.round(2),
        })
        df["for_rank"] = df.for_pg.rank(ascending=False, method="min").astype(int)
        df["allowed_rank"] = df.allowed_pg.rank(ascending=best_def_low, method="min").astype(int)
        n = len(df)
        df["allowed_tier"] = df.allowed_rank.map(lambda r: TIERS[min(int((r - 1) * 5 // n), 4)])
        rows.append(df)
    out = pd.concat(rows, ignore_index=True)
    out["season"] = season
    out["through"] = asof or "latest"
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default=current_season_label())
    ap.add_argument("--asof", default="", help="only games before this date (YYYY-MM-DD)")
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args()
    out = build(args.season, args.asof)
    out.to_csv(args.out, index=False)
    print(f"Saved -> {args.out}  teams={out.team.nunique()}  props={out.prop.nunique()}")


if __name__ == "__main__":
    main()
