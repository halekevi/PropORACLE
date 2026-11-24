#!/usr/bin/env python3
"""Rebuild data/game_logs/nfl_player_games.parquet from the NFL box score cache.

One row per (espn_athlete_id, event_id). ``player_key`` is the ESPN athlete id, so two
players with the same name never share a log. Rows come only from final box scores
(step5 re-pulls anything not confirmed final), so the log is current through the last
completed games whenever NFL step5 has run.

  py -3.14 scripts/build_nfl_player_games.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "Sports" / "NFL" / "data" / "cache" / "nfl_boxscore_cache.csv"
OUT = ROOT / "data" / "game_logs" / "nfl_player_games.parquet"

STATS = [
    "PASS_YDS", "PASS_TD", "PASS_CMP", "PASS_ATT", "PASS_INT", "PASS_SACKS",
    "RUSH_YDS", "RUSH_TD", "RUSH_ATT", "RUSH_LONG", "REC_YDS", "REC", "REC_TD",
    "REC_LONG", "REC_TGT", "SACK", "TACK_TOT", "KICK_PTS", "KICK_FG", "KICK_XP", "DEF_INT",
]
LOWER = {
    "PASS_YDS": "pass_yds", "PASS_TD": "pass_td", "PASS_CMP": "pass_cmp", "PASS_ATT": "pass_att",
    "PASS_INT": "pass_int", "RUSH_YDS": "rush_yds", "RUSH_TD": "rush_td", "RUSH_ATT": "rush_att",
    "REC_YDS": "rec_yds", "REC": "rec", "REC_TD": "rec_td", "TACK_TOT": "tack_tot", "KICK_PTS": "kick_pts",
}


def phase_for(game_date: str) -> str:
    ds = str(game_date).replace("-", "")[:8]
    if len(ds) != 8 or not ds.isdigit():
        return "UNKNOWN"
    m, d = int(ds[4:6]), int(ds[6:8])
    if m == 8 or (m == 9 and d <= 5):
        return "PRESEASON"
    if m == 1 and d >= 9 or m == 2:
        return "POSTSEASON"
    return "REGULAR"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=str(CACHE))
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args()

    c = pd.read_csv(args.cache, dtype=str, low_memory=False).fillna("")
    c = c[c["espn_athlete_id"].str.strip() != ""].copy()
    c["game_date"] = pd.to_datetime(c["game_date"].str.replace("-", "").str[:8], format="%Y%m%d", errors="coerce")
    c = c[c["game_date"].notna()]
    c = c.drop_duplicates(["espn_athlete_id", "event_id"], keep="last")
    for s in STATS:
        c[s] = pd.to_numeric(c[s], errors="coerce") if s in c.columns else float("nan")

    out = pd.DataFrame({
        "team_id": c["team_id"],
        "team": c["team_id"],
        "player_norm": c["player_norm"],
        "player": c["player_norm"],
        "player_key": c["espn_athlete_id"],
        "espn_athlete_id": c["espn_athlete_id"],
        "game_date": c["game_date"].dt.strftime("%Y-%m-%d"),
        "event_id": c["event_id"],
        "opp_team_abbr": c.get("opp_team_abbr", ""),
        "opponent": c.get("opp_team_abbr", ""),
    })
    for s in STATS:
        out[s] = c[s].values
    for s, lo in LOWER.items():
        out[lo] = c[s].values
    out["phase"] = out["game_date"].map(phase_for)
    out["is_dnp"] = False
    out["minutes"] = 1.0
    out = out.sort_values(["player_key", "game_date"]).reset_index(drop=True)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.out, index=False)
    print(f"nfl_player_games -> {args.out}: {len(out)} rows, {out.player_key.nunique()} athletes, "
          f"max {out.game_date.max()}, phases {out.phase.value_counts().to_dict()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
