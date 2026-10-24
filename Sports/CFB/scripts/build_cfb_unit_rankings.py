#!/usr/bin/env python3
"""
Build FBS national offensive + defensive unit rankings.

Uses ESPN team statistics per FBS school (~130 from standings). National ranks
plus conference-relative ranks (1 = best in that league).

Offense rank 1 = most production (yards / TDs / kick makes). Defense *allowed*
rank 1 = fewest allowed (yards / TDs / FG / PAT / kick pts / INTs forced /
sacks taken / plays). Defense *production* rank 1 = most sacks / tackles / TFL
(context only — sack *props* use opponent sacks-taken).

Output: Sports/CFB/data/reference/cfb_team_unit_rankings.csv

  py -3.14 scripts/build_cfb_unit_rankings.py --season 2025
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

_CFB_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

FBS_TEAMS_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams"
    "?limit=500&groups=80"
)
STANDINGS_URL = (
    "https://site.api.espn.com/apis/v2/sports/football/college-football/standings"
    "?season={season}&type=0"
)
TEAM_STATS_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/football/college-football"
    "/teams/{team_id}/statistics?season={season}"
)

REQUEST_DELAY_S = 0.25
MAX_RETRIES = 5

# Standings cache historically dropped the Sun Belt. Merge these ESPN FBS ids.
EXTRA_FBS_TEAMS: tuple[dict[str, str], ...] = (
    {"team_id": "2026", "team_abbr": "APP", "team_name": "App State Mountaineers", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "2032", "team_abbr": "ARST", "team_name": "Arkansas State Red Wolves", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "324", "team_abbr": "CCU", "team_name": "Coastal Carolina Chanticleers", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "290", "team_abbr": "GASO", "team_name": "Georgia Southern Eagles", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "2247", "team_abbr": "GAST", "team_name": "Georgia State Panthers", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "256", "team_abbr": "JMU", "team_name": "James Madison Dukes", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "309", "team_abbr": "UL", "team_name": "Louisiana Ragin' Cajuns", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "2433", "team_abbr": "ULM", "team_name": "UL Monroe Warhawks", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "276", "team_abbr": "MRSH", "team_name": "Marshall Thundering Herd", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "295", "team_abbr": "ODU", "team_name": "Old Dominion Monarchs", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "6", "team_abbr": "USA", "team_name": "South Alabama Jaguars", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "2572", "team_abbr": "USM", "team_name": "Southern Miss Golden Eagles", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "326", "team_abbr": "TXST", "team_name": "Texas State Bobcats", "conference_id": "37", "conference_name": "Sun Belt Conference"},
    {"team_id": "2653", "team_abbr": "TROY", "team_name": "Troy Trojans", "conference_id": "37", "conference_name": "Sun Belt Conference"},
)

# Offense rank 1 = most (except INT thrown / sacks taken / plays: 1 = fewest).
# Pass/rush *attempts* rank 1 = most (volume). Defense allowed rank 1 = fewest.
# Defense allowed rank 1 = fewest. Defense production rank 1 = most.
OUTPUT_COLS = [
    "team_id",
    "team_abbr",
    "team_name",
    "conference_id",
    "conference_name",
    "conference_size",
    "season",
    "games",
    "off_pass_ypg",
    "off_pass_rank",
    "off_rush_ypg",
    "off_rush_rank",
    "off_rec_ypg",
    "off_rec_rank",
    "off_points_pg",
    "off_points_rank",
    "off_pass_td_pg",
    "off_pass_td_rank",
    "off_rush_td_pg",
    "off_rush_td_rank",
    "off_rec_td_pg",
    "off_rec_td_rank",
    "off_td_pg",
    "off_td_rank",
    "off_fg_pg",
    "off_fg_rank",
    "off_pat_pg",
    "off_pat_rank",
    "off_kick_pts_pg",
    "off_kick_pts_rank",
    "off_int_pg",
    "off_int_rank",
    "off_sacks_taken_pg",
    "off_sacks_taken_rank",
    "off_plays_pg",
    "off_plays_rank",
    "off_pass_att_pg",
    "off_pass_att_rank",
    "off_rush_att_pg",
    "off_rush_att_rank",
    "off_rush_rate",
    "off_rush_rate_rank",
    "off_top_sec_pg",
    "off_top_rank",
    "off_clock_tier",
    "def_pass_ypg_allowed",
    "def_pass_rank",
    "def_rush_ypg_allowed",
    "def_rush_rank",
    "def_points_allowed_pg",
    "def_points_rank",
    "def_pass_td_pg_allowed",
    "def_pass_td_rank",
    "def_rush_td_pg_allowed",
    "def_rush_td_rank",
    "def_rec_td_pg_allowed",
    "def_rec_td_rank",
    "def_td_pg_allowed",
    "def_td_rank",
    "def_fg_pg_allowed",
    "def_fg_rank",
    "def_pat_pg_allowed",
    "def_pat_rank",
    "def_kick_pts_pg_allowed",
    "def_kick_pts_rank",
    "def_int_forced_pg",
    "def_int_rank",
    "def_sacks_pg",
    "def_sacks_rank",
    "def_tackles_pg",
    "def_tackles_rank",
    "def_tfl_pg",
    "def_tfl_rank",
    "updated_at",
]

# (value_col, rank_col, ascending) — True = rank 1 is the smallest value
RANK_SPECS: tuple[tuple[str, str, bool], ...] = (
    ("off_pass_ypg", "off_pass_rank", False),
    ("off_rush_ypg", "off_rush_rank", False),
    ("off_rec_ypg", "off_rec_rank", False),
    ("off_points_pg", "off_points_rank", False),
    ("off_pass_td_pg", "off_pass_td_rank", False),
    ("off_rush_td_pg", "off_rush_td_rank", False),
    ("off_rec_td_pg", "off_rec_td_rank", False),
    ("off_td_pg", "off_td_rank", False),
    ("off_fg_pg", "off_fg_rank", False),
    ("off_pat_pg", "off_pat_rank", False),
    ("off_kick_pts_pg", "off_kick_pts_rank", False),
    ("off_int_pg", "off_int_rank", True),
    ("off_sacks_taken_pg", "off_sacks_taken_rank", True),
    ("off_plays_pg", "off_plays_rank", True),
    ("off_pass_att_pg", "off_pass_att_rank", False),
    ("off_rush_att_pg", "off_rush_att_rank", False),
    ("off_rush_rate", "off_rush_rate_rank", False),
    ("off_top_sec_pg", "off_top_rank", False),
    ("def_pass_ypg_allowed", "def_pass_rank", True),
    ("def_rush_ypg_allowed", "def_rush_rank", True),
    ("def_points_allowed_pg", "def_points_rank", True),
    ("def_pass_td_pg_allowed", "def_pass_td_rank", True),
    ("def_rush_td_pg_allowed", "def_rush_td_rank", True),
    ("def_rec_td_pg_allowed", "def_rec_td_rank", True),
    ("def_td_pg_allowed", "def_td_rank", True),
    ("def_fg_pg_allowed", "def_fg_rank", True),
    ("def_pat_pg_allowed", "def_pat_rank", True),
    ("def_kick_pts_pg_allowed", "def_kick_pts_rank", True),
    ("def_int_forced_pg", "def_int_rank", True),
    ("def_sacks_pg", "def_sacks_rank", False),
    ("def_tackles_pg", "def_tackles_rank", False),
    ("def_tfl_pg", "def_tfl_rank", False),
)

_UPDATED_AT = OUTPUT_COLS.index("updated_at")
OUTPUT_COLS[_UPDATED_AT:_UPDATED_AT] = [f"{rank_col}_conf" for _, rank_col, _ in RANK_SPECS]


def conference_group_key(df: pd.DataFrame) -> pd.Series:
    cid = df.get("conference_id", pd.Series([""] * len(df))).astype(str).str.strip()
    cname = df.get("conference_name", pd.Series([""] * len(df))).astype(str).str.strip()
    blank = {"", "nan", "none", "NaN", "None"}
    key = cid.where(~cid.isin(blank), cname)
    return key.where(~key.isin(blank), "Independent")


def add_conference_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """Rank 1 = best unit inside the team's FBS conference."""
    out = df.copy()
    key = conference_group_key(out)
    out["conference_size"] = key.groupby(key).transform("size")
    for value_col, rank_col, ascending in RANK_SPECS:
        dest = f"{rank_col}_conf"
        if value_col not in out.columns:
            out[dest] = pd.Series([pd.NA] * len(out), dtype="Int64")
            continue
        vals = pd.to_numeric(out[value_col], errors="coerce")
        out[dest] = vals.groupby(key).rank(method="min", ascending=ascending).astype("Int64")
    return out


def add_all_ranks(df: pd.DataFrame) -> pd.DataFrame:
    return add_conference_ranks(add_national_ranks(df))


def _curl_session():
    try:
        from curl_cffi.requests import Session as CurlSession

        return CurlSession(impersonate="chrome131")
    except Exception:
        return None


_CURL = _curl_session()


def get_json(_session: object, url: str) -> dict[str, Any]:
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if _CURL is not None:
                resp = _CURL.get(url, headers=HEADERS, timeout=60)
                if resp.status_code == 429:
                    print(f"  [429] retry in 1s ({url[:72]}...)", file=sys.stderr)
                    time.sleep(1.0)
                    continue
                if resp.status_code >= 400:
                    last_err = RuntimeError(f"HTTP {resp.status_code}")
                    if resp.status_code == 429:
                        print(f"  [429] retry in 1s ({url[:72]}...)", file=sys.stderr)
                        time.sleep(1.0)
                        continue
                    if attempt < MAX_RETRIES:
                        time.sleep(1.0 * attempt)
                    continue
                return resp.json()
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_err = exc
            if int(getattr(exc, "code", 0) or 0) == 429:
                print(f"  [429] retry in 1s ({url[:72]}...)", file=sys.stderr)
                time.sleep(1.0)
                continue
            if attempt < MAX_RETRIES:
                time.sleep(1.0 * attempt)
        except Exception as exc:
            last_err = exc
            if attempt < MAX_RETRIES:
                time.sleep(1.0 * attempt)
    raise RuntimeError(f"GET failed: {url}") from last_err


def _csv_season(path: Path) -> int | None:
    """Season stamped on an existing rankings CSV, or None if unreadable."""
    if not path.is_file():
        return None
    try:
        peek = pd.read_csv(path, usecols=["season"], encoding="utf-8-sig")
    except Exception:
        return None
    src = pd.to_numeric(peek.get("season"), errors="coerce").dropna()
    if src.empty:
        return None
    return int(src.mode().iloc[0])


def fetch_fbs_teams_from_existing_csv(path: Path) -> list[dict[str, str]]:
    """Reuse team_id / abbr / conference from a previous rankings CSV (standings 403)."""
    if not path.is_file():
        return []
    df = pd.read_csv(path, dtype=str).fillna("")
    need = ("team_id", "team_abbr", "team_name")
    if any(c not in df.columns for c in need):
        return []
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for _, r in df.iterrows():
        tid = str(r.get("team_id") or "").strip()
        if not tid or tid in seen:
            continue
        seen.add(tid)
        rows.append(
            {
                "team_id": tid,
                "team_abbr": str(r.get("team_abbr") or "").strip().upper(),
                "team_name": str(r.get("team_name") or "").strip(),
                "conference_id": str(r.get("conference_id") or "").strip(),
                "conference_name": str(r.get("conference_name") or "").strip(),
            }
        )
    return rows


def merge_team_lists(*lists: list[dict[str, str]] | tuple[dict[str, str], ...]) -> list[dict[str, str]]:
    by_id: dict[str, dict[str, str]] = {}
    for lst in lists:
        for team in lst or ():
            tid = str(team.get("team_id") or "").strip()
            if not tid:
                continue
            by_id[tid] = dict(team)
    return list(by_id.values())


def missing_extra_fbs(existing: list[dict[str, str]]) -> list[dict[str, str]]:
    have = {str(t.get("team_id") or "").strip() for t in existing}
    have_abbr = {str(t.get("team_abbr") or "").strip().upper() for t in existing}
    out: list[dict[str, str]] = []
    for extra in EXTRA_FBS_TEAMS:
        tid = extra["team_id"]
        abbr = extra["team_abbr"].upper()
        if tid in have or abbr in have_abbr:
            continue
        out.append(dict(extra))
    return out


def fetch_fbs_teams(session: object, season: int, fallback_csv: Path) -> list[dict[str, str]]:
    try:
        teams = fetch_fbs_teams_from_standings(session, season)
        if teams:
            return teams
    except Exception as exc:
        print(f"[CFB rankings] standings unavailable ({exc}); trying cached team list")
    cached = fetch_fbs_teams_from_existing_csv(fallback_csv)
    if cached:
        print(f"[CFB rankings] Using {len(cached)} teams from {fallback_csv.name}")
        return cached
    raise RuntimeError(f"No FBS team list for season {season}")


def fetch_fbs_teams_from_standings(session: object, season: int) -> list[dict[str, str]]:
    """
    FBS team list from conference standings (~130 teams).
    ESPN groups=80 on the teams endpoint still returns hundreds of schools; standings
    is the reliable FBS filter.
    """
    j = get_json(session, STANDINGS_URL.format(season=season))
    by_id: dict[str, dict[str, str]] = {}
    for child in j.get("children") or []:
        if not isinstance(child, dict):
            continue
        conf_id = str(child.get("id") or "").strip()
        conf_name = str(child.get("name") or "").strip()
        entries = (child.get("standings") or {}).get("entries") or []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            team = entry.get("team") or {}
            tid = str(team.get("id") or "").strip()
            if not tid:
                continue
            by_id[tid] = {
                "team_id": tid,
                "team_abbr": str(team.get("abbreviation") or "").strip().upper(),
                "team_name": str(team.get("displayName") or team.get("name") or "").strip(),
                "conference_id": conf_id,
                "conference_name": conf_name,
            }
    return list(by_id.values())


def _index_cats(blocks: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for block in blocks or []:
        key = str(block.get("name") or "").strip().lower()
        if not key:
            continue
        stats: dict[str, Any] = {}
        for st in block.get("stats") or []:
            name = str(st.get("name") or "").strip()
            if name:
                stats[name] = st.get("value")
        out[key] = stats
    return out


def _f(stats: dict[str, Any] | None, *names: str) -> float | None:
    if not stats:
        return None
    for name in names:
        raw = stats.get(name)
        if raw is None or raw == "":
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def _pg(total: float | None, games: float | None) -> float | None:
    if total is None or games is None or games <= 0:
        return None
    return float(total) / float(games)


def parse_team_unit_stats(payload: dict[str, Any]) -> dict[str, Any]:
    results = payload.get("results") or {}
    season = int(
        (results.get("requestedSeason") or {}).get("year")
        or (results.get("season") or {}).get("year")
        or 0
    )
    own = _index_cats((results.get("stats") or {}).get("categories") or [])
    opp = _index_cats(results.get("opponent") or [])

    own_pass = own.get("passing") or {}
    own_rush = own.get("rushing") or {}
    own_rec = own.get("receiving") or {}
    own_kick = own.get("kicking") or {}
    own_score = own.get("scoring") or {}
    own_def = own.get("defensive") or {}
    own_dint = own.get("defensiveinterceptions") or {}
    own_gen = own.get("general") or {}
    own_misc = own.get("miscellaneous") or {}
    opp_pass = opp.get("passing") or {}
    opp_rush = opp.get("rushing") or {}
    opp_rec = opp.get("receiving") or {}
    opp_score = opp.get("scoring") or {}

    gp = (
        _f(own_pass, "teamGamesPlayed")
        or _f(own_gen, "gamesPlayed")
        or _f(own_kick, "teamGamesPlayed")
        or _f(own_score, "teamGamesPlayed")
    )

    off_pass = _f(own_pass, "netPassingYardsPerGame", "passingYardsPerGame", "yardsPerGame")
    off_rush = _f(own_rush, "rushingYardsPerGame", "yardsPerGame")
    off_rec = _f(own_rec, "receivingYardsPerGame")
    off_pts = _f(own_score, "totalPointsPerGame")
    off_plays = _f(own_rush, "totalOffensivePlays")
    pa = _f(own_pass, "passingAttempts")
    ra = _f(own_rush, "rushingAttempts")
    if off_plays is None:
        if pa is not None or ra is not None:
            off_plays = (pa or 0.0) + (ra or 0.0)

    def_fg = _f(opp_score, "fieldGoals")
    def_pat = _f(opp_score, "kickExtraPoints")
    kick_pts_allowed = None
    if def_fg is not None or def_pat is not None:
        kick_pts_allowed = 3.0 * (def_fg or 0.0) + (def_pat or 0.0)

    return {
        "season": season,
        "games": gp,
        "off_pass_ypg": off_pass,
        "off_rush_ypg": off_rush,
        "off_rec_ypg": off_rec,
        "off_points_pg": off_pts,
        "off_pass_td_pg": _pg(_f(own_pass, "passingTouchdowns"), gp),
        "off_rush_td_pg": _pg(
            _f(own_rush, "rushingTouchdowns") or _f(own_score, "rushingTouchdowns"), gp
        ),
        "off_rec_td_pg": _pg(
            _f(own_rec, "receivingTouchdowns") or _f(own_score, "receivingTouchdowns"), gp
        ),
        "off_td_pg": _pg(_f(own_score, "totalTouchdowns"), gp),
        "off_fg_pg": _pg(_f(own_kick, "fieldGoalsMade"), gp),
        "off_pat_pg": _pg(_f(own_kick, "extraPointsMade"), gp),
        "off_kick_pts_pg": _pg(_f(own_kick, "totalKickingPoints"), gp),
        "off_int_pg": _pg(_f(own_pass, "interceptions"), gp),
        "off_sacks_taken_pg": _pg(_f(own_pass, "sacks"), gp),
        "off_plays_pg": _pg(off_plays, gp),
        "off_pass_att_pg": _pg(pa, gp),
        "off_rush_att_pg": _pg(ra, gp),
        "off_rush_rate": (
            None
            if (pa is None and ra is None)
            else (float(ra or 0.0) / (float(ra or 0.0) + float(pa or 0.0)))
            if (float(ra or 0.0) + float(pa or 0.0)) > 0
            else None
        ),
        "off_top_sec_pg": _pg(_f(own_misc, "possessionTimeSeconds"), gp),
        "def_pass_ypg_allowed": _f(
            opp_pass, "netPassingYardsPerGame", "passingYardsPerGame", "yardsPerGame"
        ),
        "def_rush_ypg_allowed": _f(opp_rush, "rushingYardsPerGame", "yardsPerGame"),
        "def_points_allowed_pg": _f(opp_score, "totalPointsPerGame"),
        "def_pass_td_pg_allowed": _pg(_f(opp_pass, "passingTouchdowns"), gp),
        "def_rush_td_pg_allowed": _pg(
            _f(opp_rush, "rushingTouchdowns") or _f(opp_score, "rushingTouchdowns"), gp
        ),
        "def_rec_td_pg_allowed": _pg(
            _f(opp_rec, "receivingTouchdowns") or _f(opp_score, "receivingTouchdowns"), gp
        ),
        "def_td_pg_allowed": _pg(_f(opp_score, "totalTouchdowns"), gp),
        "def_fg_pg_allowed": _pg(def_fg, gp),
        "def_pat_pg_allowed": _pg(def_pat, gp),
        "def_kick_pts_pg_allowed": _pg(kick_pts_allowed, gp),
        "def_int_forced_pg": _pg(_f(own_dint, "interceptions"), gp),
        "def_sacks_pg": _pg(_f(own_def, "sacks"), gp),
        "def_tackles_pg": _pg(_f(own_def, "totalTackles"), gp),
        "def_tfl_pg": _pg(_f(own_def, "tacklesForLoss"), gp),
    }


def _has_stats(row: dict[str, Any]) -> bool:
    keys = (
        "off_pass_ypg",
        "off_rush_ypg",
        "off_points_pg",
        "def_pass_ypg_allowed",
        "def_rush_ypg_allowed",
        "def_points_allowed_pg",
    )
    return any(row.get(k) is not None and pd.notna(row.get(k)) for k in keys)


def resolve_season(session: object, teams: list[dict[str, str]], hint: int) -> int:
    """Pick season year with populated team stats (off-season fallback)."""
    candidates: list[int] = []
    if hint:
        candidates.append(hint)
    y = datetime.date.today().year
    candidates.extend([y, y - 1, y - 2])
    seen: set[int] = set()
    sample = teams[: min(8, len(teams))]
    for yr in candidates:
        if yr in seen:
            continue
        seen.add(yr)
        if not sample:
            continue
        filled = 0
        for t in sample:
            url = TEAM_STATS_URL.format(team_id=t["team_id"], season=yr)
            try:
                payload = get_json(session, url)
                parsed = parse_team_unit_stats(payload)
                if int(parsed.get("season") or yr) == yr and _has_stats(parsed):
                    filled += 1
            except Exception:
                pass
            time.sleep(REQUEST_DELAY_S)
        if filled >= max(2, len(sample) // 2):
            return yr
    return hint or (y - 1)


def pull_all_team_stats(
    session: object,
    teams: list[dict[str, str]],
    season: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in teams:
        name = team.get("team_name") or team.get("team_abbr") or team.get("team_id")
        print(f"CFB rankings: fetching {name}...")
        url = TEAM_STATS_URL.format(team_id=team["team_id"], season=season)
        payload = get_json(session, url)
        time.sleep(REQUEST_DELAY_S)
        parsed = parse_team_unit_stats(payload)
        row = {**team, **parsed, "season": season}
        rows.append(row)
    return rows


def _rank_series(values: pd.Series, *, ascending: bool) -> pd.Series:
    return values.rank(method="min", ascending=ascending).astype("Int64")


def add_national_ranks(df: pd.DataFrame) -> pd.DataFrame:
    from utils.cfb_clock_script import enrich_rankings_clock

    out = enrich_rankings_clock(df.copy())
    for value_col, rank_col, ascending in RANK_SPECS:
        if value_col not in out.columns:
            out[rank_col] = pd.Series([pd.NA] * len(out), dtype="Int64")
            continue
        out[rank_col] = _rank_series(
            pd.to_numeric(out[value_col], errors="coerce"), ascending=ascending
        )
    return out


def build_rankings_table(
    session: object, season_hint: int, fallback_csv: Path
) -> tuple[pd.DataFrame, int]:
    cached = fetch_fbs_teams_from_existing_csv(fallback_csv)
    live: list[dict[str, str]] = []
    try:
        live = fetch_fbs_teams_from_standings(session, season_hint or datetime.date.today().year)
    except Exception as exc:
        print(f"[CFB rankings] standings merge skipped ({exc})")
    extras = [dict(t) for t in EXTRA_FBS_TEAMS]
    cached_season = _csv_season(fallback_csv)
    same_year_cache = (
        bool(season_hint)
        and bool(cached)
        and cached_season is not None
        and int(cached_season) == int(season_hint)
    )

    if same_year_cache:
        teams = merge_team_lists(cached, live, extras)
        season = int(season_hint)
        print(
            f"[CFB rankings] Roster {len(teams)} teams "
            f"(cached={len(cached)} live={len(live)} extras={len(extras)}) | season={season}"
        )
        existing_ids = {str(t.get("team_id") or "").strip() for t in cached}
        to_fetch = [t for t in teams if str(t.get("team_id") or "").strip() not in existing_ids]
        if to_fetch and fallback_csv.is_file():
            print(f"[CFB rankings] Fetching {len(to_fetch)} missing FBS teams only")
            new_rows = pull_all_team_stats(session, to_fetch, season)
            old = pd.read_csv(fallback_csv)
            new_df = pd.DataFrame(new_rows)
            df = pd.concat([old, new_df], ignore_index=True)
            df["team_id"] = df["team_id"].astype(str).str.strip()
            df = df.drop_duplicates(subset=["team_id"], keep="last")
            with_stats = df[df.apply(_has_stats, axis=1)]
            if len(with_stats) < max(20, len(df) * 0.25):
                print(
                    f"[CFB rankings] WARN: only {len(with_stats)}/{len(df)} teams with stats "
                    f"for season {season}"
                )
            df = add_all_ranks(df)
            df["updated_at"] = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            for col in OUTPUT_COLS:
                if col not in df.columns:
                    df[col] = pd.NA
            return df[OUTPUT_COLS].sort_values(["conference_name", "team_abbr"]).reset_index(drop=True), season
        if fallback_csv.is_file():
            df = pd.read_csv(fallback_csv)
            df = add_all_ranks(df)
            for col in OUTPUT_COLS:
                if col not in df.columns:
                    df[col] = pd.NA
            return df[OUTPUT_COLS].sort_values(["conference_name", "team_abbr"]).reset_index(drop=True), season
    else:
        if cached_season is not None and season_hint and int(cached_season) != int(season_hint):
            print(
                f"[CFB rankings] Cache season={cached_season} != requested {season_hint}; "
                "full refetch (do not reuse last-year YPG as current)"
            )
        teams = fetch_fbs_teams(session, season_hint, fallback_csv)
        teams = merge_team_lists(teams, extras)
        if not teams:
            raise RuntimeError(f"No FBS teams for season {season_hint}")
        if season_hint:
            season = int(season_hint)
        else:
            season = resolve_season(session, teams, season_hint)
            if season != season_hint:
                print(f"[CFB rankings] Using season {season} (stats empty for {season_hint})")
                teams = merge_team_lists(fetch_fbs_teams(session, season, fallback_csv), extras)
                if not teams:
                    raise RuntimeError(f"No FBS teams from standings for season {season}")

    rows = pull_all_team_stats(session, teams, season)
    df = pd.DataFrame(rows)
    if df.empty:
        return df, season

    with_stats = df[df.apply(_has_stats, axis=1)]
    if len(with_stats) < max(20, len(df) * 0.25):
        print(
            f"[CFB rankings] WARN: only {len(with_stats)}/{len(df)} teams with stats "
            f"for season {season}"
        )

    df = add_all_ranks(df)
    df["updated_at"] = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    for col in OUTPUT_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[OUTPUT_COLS].sort_values(["conference_name", "team_abbr"]).reset_index(drop=True), season


def main() -> None:
    ap = argparse.ArgumentParser(description="Build FBS national CFB unit rankings CSV.")
    ap.add_argument("--season", type=int, default=0, help="ESPN season year (0 = auto).")
    ap.add_argument(
        "--out",
        default="",
        help="Output CSV (default: data/reference/cfb_team_unit_rankings.csv).",
    )
    args = ap.parse_args()

    out_path = (
        Path(args.out)
        if args.out
        else _CFB_ROOT / "data" / "reference" / "cfb_team_unit_rankings.csv"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    season_hint = int(args.season) if args.season else datetime.date.today().year
    session = None

    print(f"→ CFB FBS unit rankings (national) | season hint={season_hint}")
    df, season = build_rankings_table(session, season_hint, out_path)
    if df.empty:
        print("❌ No FBS teams ranked — check season / network.")
        raise SystemExit(1)

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    updated = df["updated_at"].iloc[0] if len(df) else ""
    print(f"✅ Wrote {len(df)} teams → {out_path}")
    print(f"   season:    {season}")
    print(f"   updated:   {updated}")
    print(f"   conferences: {df['conference_name'].nunique()}")


if __name__ == "__main__":
    main()
