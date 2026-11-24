#!/usr/bin/env python3
"""Rebuild data/game_logs/cfb_player_games_v2.{csv,parquet} from the CFB box score cache.

Same columns as the Claude v2 log (game_date, event_id, athlete_id, player, team, opp,
stats, inferred_zero) but:

* one row per (athlete_id, event_id) — same-name players never share a log;
* only games the player appears in the ESPN box score. A stat the box does not list for
  a player who did appear is a real 0. No absence-inferred zeros (``inferred_zero`` is
  always False), so a player who sat or transferred is not padded with fake 0 games;
* team-total rows ("Team", negative ids) are dropped;
* only games before today (ET), i.e. through yesterday.

Run after CFB step5 (which refreshes the cache through the last final games):

  py -3.14 scripts/build_cfb_player_games.py
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "Sports" / "CFB" / "data" / "cache" / "cfb_boxscore_cache.csv"
EVENTS = ROOT / "Sports" / "CFB" / "data" / "cache" / "cfb_boxscore_cache_events.csv"
OUT = ROOT / "data" / "game_logs" / "cfb_player_games_v2"

STATS = {
    "pass_yds": "PASS_YDS", "rush_yds": "RUSH_YDS", "rec_yds": "REC_YDS", "rec": "REC",
    "rush_att": "RUSH_ATT", "pass_att": "PASS_ATT", "pass_td": "PASS_TD", "rush_td": "RUSH_TD",
    "rec_td": "REC_TD", "kick_pts": "KICK_PTS",
}


def _team_abbr_by_id() -> dict[str, str]:
    try:
        data = requests.get(
            "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams",
            params={"limit": "1000"}, timeout=30,
        ).json()
    except Exception as exc:
        print(f"WARN team list fetch failed ({exc}); team/opp will be ESPN ids")
        return {}
    out: dict[str, str] = {}
    for ent in (data.get("sports") or [{}])[0].get("leagues", [{}])[0].get("teams", []) or []:
        t = ent.get("team") or {}
        tid, ab = str(t.get("id") or "").strip(), str(t.get("abbreviation") or "").strip().upper()
        if tid and ab:
            out[tid] = ab
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=str(CACHE))
    ap.add_argument("--events", default=str(EVENTS))
    ap.add_argument("--out", default=str(OUT), help="path stem; writes .csv and .parquet")
    args = ap.parse_args()

    today_et = dt.datetime.now(ZoneInfo("America/New_York")).date()
    c = pd.read_csv(args.cache, dtype=str, low_memory=False).fillna("")
    aid = c["espn_athlete_id"].str.strip()
    c = c[aid.str.fullmatch(r"\d+")].copy()
    c["gd"] = pd.to_datetime(c["game_date"].str.replace("-", "").str[:8], format="%Y%m%d", errors="coerce")
    c = c[c["gd"].notna() & (c["gd"].dt.date < today_et)]
    c = c.drop_duplicates(["espn_athlete_id", "event_id"], keep="last")

    abbr = _team_abbr_by_id()
    opp_id = c.get("opp_team_id", pd.Series("", index=c.index)).astype(str).str.strip()
    if Path(args.events).is_file():
        ev = pd.read_csv(args.events, dtype=str).fillna("")
        pair = {}
        for r in ev.itertuples(index=False):
            pair[(r.event_id, r.team1_id)] = r.team2_id
            pair[(r.event_id, r.team2_id)] = r.team1_id
        miss = opp_id.isin(["", "nan"])
        opp_id = opp_id.where(~miss, [pair.get((e, t), "") for e, t in zip(c["event_id"], c["team_id"])])

    out = pd.DataFrame({
        "game_date": c["gd"].dt.strftime("%Y-%m-%d"),
        "event_id": c["event_id"],
        "athlete_id": c["espn_athlete_id"],
        "player": c["player_norm"],
        "team": c["team_id"].map(lambda t: abbr.get(t, t)),
        "opp": [abbr.get(o, "") or c_opp for o, c_opp in zip(opp_id, c.get("opp_team_abbr", pd.Series("", index=c.index)))],
    })
    for lo, up in STATS.items():
        out[lo] = pd.to_numeric(c[up], errors="coerce").fillna(0.0).values if up in c.columns else 0.0
    out["inferred_zero"] = False
    out = out.sort_values(["game_date", "event_id", "team", "athlete_id"]).reset_index(drop=True)

    stem = Path(args.out)
    stem.parent.mkdir(parents=True, exist_ok=True)
    tmp = stem.with_name(stem.name + ".csv.tmp")
    out.to_csv(tmp, index=False)
    tmp.replace(stem.with_suffix(".csv"))
    out.to_parquet(stem.with_suffix(".parquet"), index=False)
    print(f"cfb_player_games_v2 -> {stem}.csv/.parquet: {len(out)} rows, {out.athlete_id.nunique()} athletes, "
          f"{out.game_date.min()}..{out.game_date.max()} (through {today_et - dt.timedelta(days=1)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
