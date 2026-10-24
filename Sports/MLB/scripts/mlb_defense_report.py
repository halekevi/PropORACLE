#!/usr/bin/env python3
"""
mlb_defense_report.py

Pull MLB team pitching + hitting context (current season) from statsapi.mlb.com
and write mlb_defense_summary.csv for step3_attach_defense_mlb.py.

Pitching (hitter props: TB, H+R+RBI, hits, hitter Ks):
  - Composite OVERALL_DEF_RANK (weighted rank-of-ranks of ERA / WHIP / OBP allowed)
  - DEF_TIER / def_tier via utils.defense_tiers (quintiles)

Offense (pitcher props: hits allowed, ERA, pitcher Ks):
  - OFF_HITS_RANK / OFF_RUNS_RANK / OFF_SO_RANK — rank 1 = hardest pitcher OVER
    (fewest hits, fewest runs, lowest K-rate)
  - Matching OFF_*_TIER labels (same Weak→Elite buckets)
  - Display inverts hits to batting strength (Weak hits → Strong lineup)

Team abbreviations match PrizePicks / step2 (e.g. AZ, ATH, WSH)

Usage:
  py -3.14 mlb_defense_report.py
  py -3.14 mlb_defense_report.py --season 2026 --out ..\\mlb_defense_summary.csv --top 10
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from utils.defense_tiers import def_tier_from_overall_rank

STATS_API = "https://statsapi.mlb.com/api/v1"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _default_season_year() -> int:
    """MLB API season id is the calendar year of that season (e.g. 2026)."""
    return date.today().year


def fetch_json(url: str, retries: int = 3) -> dict[str, Any]:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=25) as resp:
                return json.loads(resp.read().decode())
        except Exception:
            if attempt < retries - 1:
                time.sleep(1.5**attempt)
    return {}


def rank_series(s: pd.Series, *, ascending: bool) -> pd.Series:
    return s.rank(method="min", ascending=ascending).astype("Int64")


def parse_stat_float(x: Any) -> float:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return float("nan")
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "-", ".-"):
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def team_id_to_abbrev() -> dict[int, str]:
    data = fetch_json(f"{STATS_API}/teams?sportId=1")
    teams = (data or {}).get("teams") or []
    out: dict[int, str] = {}
    for t in teams:
        tid = t.get("id")
        ab = str(t.get("abbreviation") or "").strip().upper()
        if tid is not None and ab:
            out[int(tid)] = ab
    return out


def pull_team_pitching(season: int, game_type: str = "R") -> pd.DataFrame:
    """
    Regular-season team pitching; one row per franchise.
    Lower ERA / WHIP / opponent OBP = stronger team pitching (stingier vs hitters).
    """
    url = (
        f"{STATS_API}/teams/stats"
        f"?season={season}&sportIds=1&group=pitching&stats=season&gameType={game_type}"
    )
    print(f"📡 MLB team pitching stats (season={season}, gameType={game_type})...")
    data = fetch_json(url)
    stats_blocks = (data or {}).get("stats") or []
    if not stats_blocks:
        return pd.DataFrame()

    splits = stats_blocks[0].get("splits") or []
    id_abbr = team_id_to_abbrev()
    rows: list[dict[str, Any]] = []
    for sp in splits:
        team_obj = sp.get("team") or {}
        tid = team_obj.get("id")
        abbr = id_abbr.get(int(tid)) if tid is not None else ""
        if not abbr:
            continue
        st = sp.get("stat") or {}
        era = parse_stat_float(st.get("era"))
        whip = parse_stat_float(st.get("whip"))
        obp = parse_stat_float(st.get("obp"))
        hits = parse_stat_float(st.get("hits"))
        runs = parse_stat_float(st.get("runs"))
        hr = parse_stat_float(st.get("homeRuns"))
        gp = parse_stat_float(st.get("gamesPlayed"))
        rows.append(
            {
                "TEAM_ABBREVIATION": abbr,
                "SP_ERA": era,
                "WHIP": whip,
                "OBP_ALLOWED": obp,
                "HITS_ALLOWED": hits,
                "RUNS_ALLOWED": runs,
                "HR_ALLOWED": hr,
                "HITS_PER_GAME": (hits / gp) if gp and gp > 0 and not math.isnan(hits) else float("nan"),
                "RUNS_PER_GAME": (runs / gp) if gp and gp > 0 and not math.isnan(runs) else float("nan"),
                "inningsPitched": parse_stat_float(st.get("inningsPitched")),
                "gamesPlayed": gp,
            }
        )
    return pd.DataFrame(rows)


def pull_team_hitting(season: int, game_type: str = "R") -> pd.DataFrame:
    """Regular-season team batting. High hits/runs = easier pitcher HA/ERA overs.

    High K-rate = easier pitcher strikeout overs (lineup gets punched out).
    """
    url = (
        f"{STATS_API}/teams/stats"
        f"?season={season}&sportIds=1&group=hitting&stats=season&gameType={game_type}"
    )
    print(f"📡 MLB team hitting stats (season={season}, gameType={game_type})...")
    data = fetch_json(url)
    stats_blocks = (data or {}).get("stats") or []
    if not stats_blocks:
        return pd.DataFrame()

    splits = stats_blocks[0].get("splits") or []
    id_abbr = team_id_to_abbrev()
    rows: list[dict[str, Any]] = []
    for sp in splits:
        team_obj = sp.get("team") or {}
        tid = team_obj.get("id")
        abbr = id_abbr.get(int(tid)) if tid is not None else ""
        if not abbr:
            continue
        st = sp.get("stat") or {}
        hits = parse_stat_float(st.get("hits"))
        runs = parse_stat_float(st.get("runs"))
        so = parse_stat_float(st.get("strikeOuts"))
        pa = parse_stat_float(st.get("plateAppearances"))
        ab = parse_stat_float(st.get("atBats"))
        gp = parse_stat_float(st.get("gamesPlayed"))
        denom = pa if pa and pa > 0 and not math.isnan(pa) else ab
        so_rate = (so / denom) if denom and denom > 0 and not math.isnan(so) else float("nan")
        rows.append(
            {
                "TEAM_ABBREVIATION": abbr,
                "OFF_HITS": hits,
                "OFF_RUNS": runs,
                "OFF_SO": so,
                "OFF_PA": pa,
                "OFF_AB": ab,
                "OFF_AVG": parse_stat_float(st.get("avg")),
                "OFF_OBP": parse_stat_float(st.get("obp")),
                "OFF_HITS_PG": (hits / gp) if gp and gp > 0 and not math.isnan(hits) else float("nan"),
                "OFF_RUNS_PG": (runs / gp) if gp and gp > 0 and not math.isnan(runs) else float("nan"),
                "OFF_SO_RATE": so_rate,
                "OFF_GAMES": gp,
            }
        )
    return pd.DataFrame(rows)


def add_offense_ranks_and_tiers(df: pd.DataFrame) -> pd.DataFrame:
    """Rank 1 = hardest pitcher OVER (few hits, few runs, low K-rate)."""
    df = df.copy()
    n_teams = max(len(df), 1)
    if "OFF_HITS_PG" in df.columns:
        df["OFF_HITS_RANK"] = rank_series(df["OFF_HITS_PG"], ascending=True)
        df["OFF_HITS_TIER"] = df["OFF_HITS_RANK"].apply(
            lambda r: def_tier_from_overall_rank(r, n_teams)
        )
    if "OFF_RUNS_PG" in df.columns:
        df["OFF_RUNS_RANK"] = rank_series(df["OFF_RUNS_PG"], ascending=True)
        df["OFF_RUNS_TIER"] = df["OFF_RUNS_RANK"].apply(
            lambda r: def_tier_from_overall_rank(r, n_teams)
        )
    if "OFF_SO_RATE" in df.columns:
        df["OFF_SO_RANK"] = rank_series(df["OFF_SO_RATE"], ascending=True)
        df["OFF_SO_TIER"] = df["OFF_SO_RANK"].apply(
            lambda r: def_tier_from_overall_rank(r, n_teams)
        )
    return df


def add_ranks_and_tiers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Lower is better for ERA, WHIP, opponent OBP allowed
    if "SP_ERA" in df.columns:
        df["ERA_RANK"] = rank_series(df["SP_ERA"], ascending=True)
    if "WHIP" in df.columns:
        df["WHIP_RANK"] = rank_series(df["WHIP"], ascending=True)
    if "OBP_ALLOWED" in df.columns:
        df["OBP_ALLOWED_RANK"] = rank_series(df["OBP_ALLOWED"], ascending=True)
    if "HITS_PER_GAME" in df.columns:
        df["HITS_ALLOWED_RANK"] = rank_series(df["HITS_PER_GAME"], ascending=False)
    if "RUNS_PER_GAME" in df.columns:
        df["RUNS_ALLOWED_RANK"] = rank_series(df["RUNS_PER_GAME"], ascending=False)
    if "HR_ALLOWED" in df.columns and "gamesPlayed" in df.columns:
        hr_pg = df["HR_ALLOWED"] / df["gamesPlayed"].replace(0, np.nan)
        df["HR_PER_GAME"] = hr_pg
        df["HR_ALLOWED_RANK"] = rank_series(df["HR_PER_GAME"], ascending=False)

    rank_cols = [c for c in ["ERA_RANK", "WHIP_RANK", "OBP_ALLOWED_RANK"] if c in df.columns]
    if rank_cols:
        weights = {"ERA_RANK": 3.0, "WHIP_RANK": 2.0, "OBP_ALLOWED_RANK": 1.5}
        w = np.array([weights.get(c, 1.0) for c in rank_cols], dtype=float)
        w = w / w.sum()
        df["OVERALL_DEF_SCORE"] = df[rank_cols].mul(w, axis=1).sum(axis=1)
        df["OVERALL_DEF_RANK"] = rank_series(df["OVERALL_DEF_SCORE"], ascending=True)
    else:
        df["OVERALL_DEF_SCORE"] = np.nan
        df["OVERALL_DEF_RANK"] = pd.NA

    n_teams = len(df)

    def tier_from_rank(r: object) -> str:
        return def_tier_from_overall_rank(r, n_teams)

    df["DEF_TIER"] = df["OVERALL_DEF_RANK"].apply(tier_from_rank)
    # Align with NHL/NBA: def_rank / def_tier duplicates for step merges / dashboards
    df["def_rank"] = df["ERA_RANK"] if "ERA_RANK" in df.columns else df["OVERALL_DEF_RANK"]
    df["def_tier"] = df["DEF_TIER"]

    return df


def print_leaders(df: pd.DataFrame, topn: int) -> None:
    def show(title: str, col: str, *, asc: bool = True) -> None:
        if col not in df.columns:
            print(f"\n{title}: (missing {col})")
            return
        t = df.sort_values(col, ascending=asc).head(topn)[["TEAM_ABBREVIATION", col]]
        print(f"\n{title}")
        print(t.to_string(index=False))

    show("BEST TEAM PITCHING (composite) — OVERALL_DEF_RANK", "OVERALL_DEF_RANK", asc=True)
    show("LOWEST ERA — SP_ERA", "SP_ERA", asc=True)
    show("LOWEST WHIP", "WHIP", asc=True)
    show("LOWEST OPPONENT AVG/OBP AGAINST — OBP_ALLOWED", "OBP_ALLOWED", asc=True)
    show("MOST HITS ALLOWED PER GAME — HITS_PER_GAME", "HITS_PER_GAME", asc=False)
    show("MOST RUNS ALLOWED PER GAME — RUNS_PER_GAME", "RUNS_PER_GAME", asc=False)
    show("MOST HR ALLOWED PER GAME — HR_PER_GAME", "HR_PER_GAME", asc=False)
    show("FEWEST HITS/G (hardest HA OVER) — OFF_HITS_RANK", "OFF_HITS_RANK", asc=True)
    show("FEWEST RUNS/G (hardest ERA OVER) — OFF_RUNS_RANK", "OFF_RUNS_RANK", asc=True)
    show("LOWEST K-RATE (hardest pitcher K OVER) — OFF_SO_RANK", "OFF_SO_RANK", asc=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=None, help="MLB season year (e.g. 2026)")
    ap.add_argument("--out", default="mlb_defense_summary.csv")
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()

    season = args.season if args.season is not None else _default_season_year()

    df = pull_team_pitching(season=season)
    if df.empty:
        print("❌ No team pitching stats returned.")
        sys.exit(1)

    df = add_ranks_and_tiers(df)
    hitting = pull_team_hitting(season=season)
    if hitting.empty:
        print("❌ No team hitting stats returned.")
        sys.exit(1)
    hitting = add_offense_ranks_and_tiers(hitting)
    df = df.merge(hitting, on="TEAM_ABBREVIATION", how="left")

    front = [
        "TEAM_ABBREVIATION",
        "SP_ERA",
        "WHIP",
        "OBP_ALLOWED",
        "HITS_ALLOWED",
        "RUNS_ALLOWED",
        "HR_ALLOWED",
        "HITS_PER_GAME",
        "RUNS_PER_GAME",
        "HR_PER_GAME",
        "ERA_RANK",
        "WHIP_RANK",
        "OBP_ALLOWED_RANK",
        "HITS_ALLOWED_RANK",
        "RUNS_ALLOWED_RANK",
        "HR_ALLOWED_RANK",
        "OVERALL_DEF_SCORE",
        "OVERALL_DEF_RANK",
        "DEF_TIER",
        "def_rank",
        "def_tier",
        "OFF_HITS_PG",
        "OFF_RUNS_PG",
        "OFF_SO_RATE",
        "OFF_AVG",
        "OFF_OBP",
        "OFF_HITS_RANK",
        "OFF_HITS_TIER",
        "OFF_RUNS_RANK",
        "OFF_RUNS_TIER",
        "OFF_SO_RANK",
        "OFF_SO_TIER",
    ]
    extra = [c for c in df.columns if c not in front]
    df = df[[c for c in front if c in df.columns] + extra]

    out_path = args.out
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        stamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
        out_path = str(Path(out_path).with_stem(Path(out_path).stem + f"_{stamp}"))
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print_leaders(df, topn=args.top)
    print(f"\n✅ Saved → {out_path}")
    print(f"Teams: {df['TEAM_ABBREVIATION'].nunique()}")
    print("\nDEF_TIER breakdown:")
    print(df["DEF_TIER"].value_counts().to_string())


if __name__ == "__main__":
    main()
