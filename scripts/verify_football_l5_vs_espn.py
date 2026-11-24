#!/usr/bin/env python3
"""Spot-check football step5 L5 (stat_g1..g5) against ESPN athlete game logs.

ESPN's per-athlete gamelog is keyed by athlete id and is a separate source from the
summary box scores step5 caches, so a match here means the cache + attach agree with
what the player card should show (regular season + postseason, newest first).

  py -3.14 scripts/verify_football_l5_vs_espn.py --league nfl \
      --step5 Sports/NFL/data/outputs/step5_nfl_with_stats.csv --date 2026-11-24 \
      --players "Emmett Johnson,Josh Allen"
  py -3.14 scripts/verify_football_l5_vs_espn.py --league college-football \
      --step5 outputs/2026-11-24/cfb/step5b_with_stats_cfb.csv --sample 10
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date

import pandas as pd
import requests

URL = "https://site.web.api.espn.com/apis/common/v3/sports/football/{league}/athletes/{aid}/gamelog"

# step5 prop -> ESPN gamelog stat names (summed for combos)
PROP_STATS = {
    "rushing_yards": ["rushingYards"],
    "rush_yards": ["rushingYards"],
    "rush_attempts": ["rushingAttempts"],
    "receiving_yards": ["receivingYards"],
    "rec_yards": ["receivingYards"],
    "receptions": ["receptions"],
    "rush_rec_yds": ["rushingYards", "receivingYards"],
    "passing_yards": ["passingYards"],
    "pass_yards": ["passingYards"],
    "pass_attempts": ["passingAttempts"],
    "pass_completions": ["completions"],
    "pass_rush_yds": ["passingYards", "rushingYards"],
    "passing_tds": ["passingTouchdowns"],
}


def _num(v) -> float | None:
    try:
        s = str(v).strip()
        if s in ("", "-", "--", "nan"):
            return None
        return float(s.split("/")[0])
    except (TypeError, ValueError):
        return None


def espn_games(league: str, aid: str, season: int | None) -> list[dict]:
    params = {"season": season} if season else None
    r = requests.get(URL.format(league=league, aid=aid), params=params, timeout=20)
    if r.status_code != 200:
        return []
    d = r.json()
    names = d.get("names") or []
    events = d.get("events") or {}
    out = []
    for st in d.get("seasonTypes") or []:
        label = str(st.get("displayName") or "")
        if "preseason" in label.lower():
            continue
        for cat in st.get("categories") or []:
            for ev in cat.get("events") or []:
                eid = str(ev.get("eventId"))
                meta = events.get(eid) or {}
                row = {"event_id": eid, "date": str(meta.get("gameDate", ""))[:10], "season_type": label}
                for n, v in zip(names, ev.get("stats") or []):
                    row[n] = _num(v)
                out.append(row)
    return out


def espn_last5(league: str, aid: str, prop: str, slate: date) -> tuple[list[float], list[str]]:
    stats = PROP_STATS[prop]
    games: list[dict] = []
    for season in (None, slate.year - 1 if slate.month >= 8 else slate.year - 2):
        games += espn_games(league, aid, season)
        if len({g["event_id"] for g in games if g["date"] < slate.isoformat()}) >= 5:
            break
        time.sleep(0.3)
    seen, rows = set(), []
    for g in sorted(games, key=lambda g: g["date"], reverse=True):
        if g["date"] >= slate.isoformat() or g["event_id"] in seen:
            continue
        seen.add(g["event_id"])
        vals = [g.get(s) for s in stats]
        if any(v is None for v in vals):
            continue
        rows.append((sum(vals), g["date"]))
    rows = rows[:5]
    return [v for v, _ in rows], [d for _, d in rows]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="nfl", choices=["nfl", "college-football"])
    ap.add_argument("--step5", required=True)
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--players", default="", help="Comma list of player names")
    ap.add_argument("--sample", type=int, default=0, help="Random players with an OK yards prop")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    slate = date.fromisoformat(args.date[:10])

    df = pd.read_csv(args.step5, dtype=str, low_memory=False).fillna("")
    prop_col = "prop_type_normalized" if "prop_type_normalized" in df.columns else (
        "prop_norm" if "prop_norm" in df.columns else "prop_type")
    df = df[(df.get("stat_status", "OK") == "OK") & df[prop_col].isin(PROP_STATS)]
    df = df[df["espn_athlete_id"].astype(str).str.strip() != ""]
    if args.players:
        want = {p.strip().lower() for p in args.players.split(",") if p.strip()}
        df = df[df["player"].str.lower().isin(want)]
    one = df.drop_duplicates(["player", prop_col])
    if args.sample:
        one = one.drop_duplicates("player").sample(min(args.sample, one["player"].nunique()), random_state=7)
    else:
        one = one.drop_duplicates("player")

    results = []
    for _, r in one.iterrows():
        prop = r[prop_col]
        ours = [_num(r.get(f"stat_g{i}")) for i in range(1, 6)]
        ours = [v for v in ours if v is not None]
        theirs, dates = espn_last5(args.league, str(r["espn_athlete_id"]).split(".")[0], prop, slate)
        ok = ours == theirs
        results.append({
            "player": r["player"], "team": r.get("team", ""), "pos": r.get("pos", r.get("position", "")),
            "prop": prop, "ours": ours, "espn": theirs, "espn_dates": dates, "match": ok,
        })
        flag = "OK " if ok else "MISMATCH"
        print(f"{flag} {r['player']:<26} {prop:<16} ours={ours} espn={theirs} {dates}")
        time.sleep(0.3)
    n_bad = sum(1 for x in results if not x["match"])
    print(f"\n{len(results) - n_bad}/{len(results)} match ESPN gamelog")
    if args.out:
        pd.DataFrame(results).to_csv(args.out, index=False)
    return 1 if n_bad else 0


if __name__ == "__main__":
    sys.exit(main())
