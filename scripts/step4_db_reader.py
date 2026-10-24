#!/usr/bin/env python3
"""
step4_db_reader.py — PropOracle shared DB stat lookup module

Used by all 4 sport step4 scripts. Replaces all live ESPN/NHL API calls
with indexed SQLite reads from data/cache/proporacle_ref.db.

Key functions:
    open_db(db_path)                     → sqlite3.Connection
    get_vals_nba(con, espn_id, prop, n)  → List[float]
    get_vals_cbb(con, espn_id, prop, n)  → List[float]
    get_vals_nhl(con, player, prop, n)   → List[float]
    get_vals_soccer(con, espn_id, prop, n) → List[float]
    attach_stats(slate_df, sport, con, id_col, prop_col, line_col, n)
        → slate_df with stat columns filled in

Output columns added by attach_stats() — identical schema across all sports:
    stat_g1 .. stat_gN
    stat_last5_avg, stat_last10_avg, stat_season_avg
    last5_over, last5_under, last5_push, last5_hit_rate
    line_hit_rate_over_ou_5,  line_hit_rate_under_ou_5
    line_hit_rate_over_ou_10, line_hit_rate_under_ou_10
    stat_status   (OK | NO_ID | NO_DATA | INSUFFICIENT_GAMES)

NBA/CBB also adds:
    min_last5_avg

Soccer also adds:
    avg_minutes, avg_passes
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

# ── Default DB path ────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
DB_PATH = _HERE.parent / "data" / "cache" / "proporacle_ref.db"


def open_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Open the PropOracle reference DB (read-only safe, WAL mode)."""
    path = Path(db_path) if db_path else DB_PATH
    if not path.exists():
        raise FileNotFoundError(
            f"PropOracle reference DB not found at {path}\n"
            f"Run: py scripts/build_boxscore_ref.py --backfill --days 30"
        )
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    return con


# ── NBA / CBB prop → DB column mapping ────────────────────────────────────────
# Maps prop_norm strings (lowercase) to one or more DB column expressions.
# Combos use SQL arithmetic directly.
_NBA_PROP_MAP = {
    # Simple stats
    "points":                  "pts",
    "pts":                     "pts",
    "rebounds":                "reb",
    "reb":                     "reb",
    "assists":                 "ast",
    "ast":                     "ast",
    "steals":                  "stl",
    "stl":                     "stl",
    "blocks":                  "blk",
    "blk":                     "blk",
    "turnovers":               "tov",
    "tov":                     "tov",
    "to":                      "tov",
    "fg made":                 "fgm",
    "fgm":                     "fgm",
    "fg attempted":            "fga",
    "fga":                     "fga",
    "3-pt made":               "fg3m",
    "3ptmade":                 "fg3m",
    "3pt made":                "fg3m",
    "fg3m":                    "fg3m",
    "3pm":                     "fg3m",
    "3-pt attempted":          "fg3a",
    "3ptattempted":            "fg3a",
    "3pt attempted":           "fg3a",
    "fg3a":                    "fg3a",
    "3pa":                     "fg3a",
    "two pointers made":       "fg2m",
    "twopointersmade":         "fg2m",
    "fg2m":                    "fg2m",
    "2pm":                     "fg2m",
    "two pointers attempted":  "fg2a",
    "twopointersattempted":    "fg2a",
    "fg2a":                    "fg2a",
    "2pa":                     "fg2a",
    "free throws made":        "ftm",
    "freethrowsmade":          "ftm",
    "ftm":                     "ftm",
    "free throws attempted":   "fta",
    "freethrowsattempted":     "fta",
    "fta":                     "fta",
    "offensive rebounds":      "oreb",
    "oreb":                    "oreb",
    "defensive rebounds":      "dreb",
    "dreb":                    "dreb",
    "personal fouls":          "pf",
    "personalfouls":           "pf",
    "pf":                      "pf",
    "fantasy score":           "fantasy_score",
    "fantasy_score":           "fantasy_score",
    "fantasy":                 "fantasy_score",   # norm_prop maps "Fantasy Score" → "fantasy"
    # Combos
    "pts+rebs+asts":           "pts + reb + ast",
    "pra":                     "pts + reb + ast",
    "pts+rebs":                "pts + reb",
    "pr":                      "pts + reb",
    "pts+asts":                "pts + ast",
    "pa":                      "pts + ast",
    "rebs+asts":               "reb + ast",
    "ra":                      "reb + ast",
    "blks+stls":               "blk + stl",
    "stocks":                  "blk + stl",
    "bs":                      "blk + stl",
    "minutes":                 "minutes",
    "min":                     "minutes",
}

# ── NHL prop → DB column ───────────────────────────────────────────────────────
# ESPN skater boxscores expose PPTOI but not PPP/PPG/PPA — pp_points is always NULL.
_NHL_ESPN_UNSUPPORTED = frozenset({
    "power play points",
    "power_play_points",
    "pp_points",
})

# Slate / PP name variants → canonical name in proporacle_ref.nhl
_NHL_NAME_ALIASES: dict[str, str] = {
    "zachary bolduc": "Zack Bolduc",
}

_NHL_PROP_MAP = {
    "shots on goal":           "shots_on_goal",
    "shots_on_goal":           "shots_on_goal",
    "sog":                     "shots_on_goal",
    "shots on goal (combo)":   "shots_on_goal",
    "goals":                   "goals",
    "assists":                 "assists",
    "points":                  "points",
    "hits":                    "hits",
    "blocked shots":           "blocked_shots",
    "blocked_shots":           "blocked_shots",
    "pim":                     "pim",
    "plus/minus":              "plus_minus",
    "plus_minus":              "plus_minus",
    "faceoffs won":            "faceoffs_won",
    "faceoffs_won":            "faceoffs_won",
    "time on ice":             "toi",
    "time_on_ice":             "toi",
    "toi":                     "toi",
    "goalie saves":            "saves",
    "goalie_saves":            "saves",
    "saves":                   "saves",
    "goals allowed":           "goals_allowed",
    "goals_allowed":           "goals_allowed",
    "fantasy score":           "goals * 8 + assists * 5 + shots_on_goal * 1.5 + hits * 1.3 + blocked_shots * 1.3",
    "fantasy_score":           "goals * 8 + assists * 5 + shots_on_goal * 1.5 + hits * 1.3 + blocked_shots * 1.3",
}

# ── Soccer prop → DB column ────────────────────────────────────────────────────
# Passes / tackles / clearances / dribbles: ESPN summaries leave these NULL.
# Soccer step4 fills them from Sofascore lineups (event_id sofa_*). Shots/SOT/goals
# are also filled when ESPN only has the latest league game.
_SOCCER_PROP_MAP = {
    "shots on target":         "sog",
    "shots_on_target":         "sog",
    "sog":                     "sog",
    "sot":                     "sog",
    "shots":                   "sh",
    "sh":                      "sh",
    "goals":                   "g",
    "g":                       "g",
    "assists":                 "a",
    "a":                       "a",
    "goalkeeper saves":        "sv",
    "goalie saves":            "sv",
    "saves":                   "sv",
    "sv":                      "sv",
    "key passes":              "kp",
    "kp":                      "kp",
    "fouls":                   "fc",
    "fc":                      "fc",
    "yellow cards":            "yc",
    "yc":                      "yc",
    "goal+assist":             "g + a",
    "goal_assist":             "g + a",
    "shots assisted":          "kp",
    "shots_assisted":          "kp",
    "minutes":                 "minutes_played",
    "minutes played":          "minutes_played",
    # Weak / often-null ESPN markets (ticket-banned; L5 when DB fills).
    "passes":                  "pa",
    "passes attempted":        "pa",
    "pa":                      "pa",
    "tackles":                 "tk",
    "tk":                      "tk",
    "clearances":              "clearances",
    "attempted dribbles":      "dribble_attempts",
    "attempteddribbles":       "dribble_attempts",
    "attempted_dribbles":      "dribble_attempts",
    "dribbles":                "dribble_attempts",
    "crosses":                 "crosses",
}


def _resolve_prop(prop_norm: str, sport: str) -> Optional[str]:
    """Return the SQL column expression for a prop_norm, or None if unknown."""
    p = str(prop_norm or "").lower().strip()
    # Strip trailing (combo) marker
    p = p.replace("(combo)", "").strip()
    if sport in ("nba", "cbb"):
        return _NBA_PROP_MAP.get(p)
    if sport == "nhl":
        if p in _NHL_ESPN_UNSUPPORTED:
            return None
        return _NHL_PROP_MAP.get(p)
    if sport == "soccer":
        return _SOCCER_PROP_MAP.get(p)
    return None


# ── Core DB query ──────────────────────────────────────────────────────────────
def _allstar_team_exclusion_sql(sport: str, team_col: str = "team") -> tuple[str, tuple]:
    """SQL fragment + params excluding All-Star squad team codes."""
    try:
        from utils.allstar_filter import allstar_teams_for_sport
    except Exception:
        return "1=1", ()
    teams = sorted(allstar_teams_for_sport(sport))
    if not teams:
        return "1=1", ()
    placeholders = ",".join("?" for _ in teams)
    return f"upper(coalesce({team_col}, '')) NOT IN ({placeholders})", tuple(teams)


def _allstar_date_exclusion_sql(sport: str, date_col: str = "game_date") -> tuple[str, tuple]:
    """SQL fragment + params excluding configured All-Star date windows."""
    try:
        from utils.allstar_filter import allstar_date_exclusion_sql
    except Exception:
        return "1=1", ()
    return allstar_date_exclusion_sql(sport, date_col=date_col)


def _query_vals(con: sqlite3.Connection, table: str, where_clause: str,
                stat_expr: str, params: tuple, n: int,
                *, sport: str = "", min_game_date: str = "",
                card_window: bool = False,
                regular_season_only: bool = False) -> List[float]:
    """
    Generic indexed stat query.
    Returns up to n most-recent non-null values, newest first.
    min_game_date drops older seasons (NBAP last-5 is this preseason only).
    card_window: keep regular + playoffs, drop preseason (PP card L5).
    regular_season_only: legacy — prefer card_window (playoffs stay).
    """
    sport_key = sport or table
    exclude_sql, exclude_params = _allstar_team_exclusion_sql(sport_key)
    date_sql, date_params = _allstar_date_exclusion_sql(sport_key)
    floor = str(min_game_date or "")[:10]
    floor_sql = " AND game_date >= ?" if floor else ""
    floor_params = (floor,) if floor else ()
    filter_windows = card_window or regular_season_only
    # Pull extra rows when filtering windows so L5 still fills after drops.
    fetch_n = max(n * 8, n) if filter_windows else n
    sql = f"""
        SELECT game_date, {stat_expr} AS val
        FROM {table}
        WHERE {where_clause}
          AND {stat_expr} IS NOT NULL
          AND ({exclude_sql})
          AND ({date_sql})
          {floor_sql}
        ORDER BY game_date DESC
        LIMIT {fetch_n}
    """
    try:
        rows = con.execute(
            sql,
            tuple(params) + tuple(exclude_params) + tuple(date_params) + tuple(floor_params),
        ).fetchall()
    except Exception:
        return []
    if not filter_windows:
        return [float(r[1]) for r in rows if r[1] is not None]
    try:
        from datetime import date as _date
        from utils.nba_minutes_role import classify_game
    except Exception:
        return [float(r[1]) for r in rows if r[1] is not None][:n]
    if card_window:
        allow = {"regular", "playoffs"}
    else:
        allow = {"regular"}
    out: List[float] = []
    for gd, val in rows:
        if val is None:
            continue
        try:
            d = _date.fromisoformat(str(gd)[:10])
        except ValueError:
            continue
        if classify_game(d) not in allow:
            continue
        out.append(float(val))
        if len(out) >= n:
            break
    return out


# Sofascore omits these keys at 0; PP Last 5 still counts the appearance as 0.
# Do NOT include `sv` (saves): a missing Sofa save key is unknown, not 0. Coalescing
# null→0 invented King's Cup "0 save" games and fake L5=5 Unders (Pacheco).
_SOCCER_OMITTED_ZERO_COLS = frozenset({
    "tk", "clearances", "crosses", "kp", "pa", "sh", "sog",
})


def _soccer_select_and_filter(stat_expr: str) -> tuple[str, str]:
    """SELECT alias + WHERE fragment. Sofa appearances with a missing 0-count stay in L5."""
    col = str(stat_expr or "").strip()
    if col not in _SOCCER_OMITTED_ZERO_COLS:
        return f"{stat_expr} AS val", f"{stat_expr} IS NOT NULL"
    appeared = (
        "pa IS NOT NULL OR sh IS NOT NULL OR sog IS NOT NULL "
        "OR fc IS NOT NULL OR tk IS NOT NULL OR sv IS NOT NULL "
        "OR coalesce(minutes_played, 0) > 0"
    )
    select_sql = (
        f"CASE WHEN coalesce(event_id, '') LIKE 'sofa_%' "
        f"THEN coalesce({col}, 0) ELSE {col} END AS val"
    )
    where_sql = (
        f"({col} IS NOT NULL OR "
        f"(coalesce(event_id, '') LIKE 'sofa_%' AND ({appeared})))"
    )
    return select_sql, where_sql


def _soccer_club_key(team: str) -> str:
    import re
    raw = re.sub(r"[^A-Z0-9]", "", str(team or "").upper())
    aliases = {
        "WESTHAM": "WHU", "WESTHAMUNITED": "WHU", "WHU": "WHU",
        "WOLVES": "WOL", "WOLVERHAMPTON": "WOL", "WOLVERHAMPTONWANDERERS": "WOL",
        "WOL": "WOL",
        "NORWICH": "NOR", "NORWICHCITY": "NOR", "NOR": "NOR",
        "PORTSMOUTH": "PORTSMOUTH", "POMPEY": "PORTSMOUTH",
        "AUSTRALIA": "AUS", "AUS": "AUS",
        "IVORYCOAST": "CIV", "CIV": "CIV",
        "EGYPT": "EGY", "EGY": "EGY",
        # PrizePicks board abbr ↔ ESPN soccer box codes (Saudi / big-five).
        # Without this, team=FAISALY drops ESPN ALF rows and L5 falls onto
        # old Sofascore FAISALY logs (Pacheco false L5=5 Under).
        "FAISALY": "ALF", "ALFAISALY": "ALF", "ALF": "ALF",
        "HILAL": "HIL", "ALHILAL": "HIL", "HIL": "HIL",
        "NASSR": "NAS", "ALNASSR": "NAS", "NAS": "NAS",
        "QADSIAH": "QAD", "ALQADSIAH": "QAD", "QAD": "QAD",
        "ITTIHAD": "ITT", "ALITTIHAD": "ITT", "ITT": "ITT",
        "SHABAB": "SHA", "ALSHABAB": "SHA", "SHA": "SHA",
        "AHLI": "AHL", "ALAHLI": "AHL", "AHL": "AHL",
        "ETTIFAQ": "ETT", "ALETTIFAQ": "ETT", "ETT": "ETT",
        "KHOLOOD": "KHO", "ALKHOLOOD": "KHO",
        "HAZEM": "HAZ", "ALHAZEM": "HAZ", "HAZ": "HAZ",
        "NICE": "NICE", "OGCNICE": "NICE",
        "BREST": "BRE", "STBREST": "BRE", "BRE": "BRE",
    }
    return aliases.get(raw, raw)


def _soccer_club_match(hint: str, db_team: str) -> bool:
    """True when slate club and DB club are the same side (not AUS vs Norwich)."""
    if not str(hint or "").strip():
        return True
    h = _soccer_club_key(hint)
    d = _soccer_club_key(db_team)
    if not h or not d:
        return True
    if h == d:
        return True
    if len(h) >= 3 and len(d) >= 3 and (h in d or d in h):
        return True
    return False


def _query_soccer_vals(
    con: sqlite3.Connection,
    where_clause: str,
    stat_expr: str,
    params: tuple,
    n: int,
    team_hint: str = "",
) -> List[float]:
    """Newest soccer L5, one row per game_date (ESPN beats sofa_*).

    Walk appearances newest-first. A missing prop on a recent box is a **gap** —
    stop there instead of skipping to older Sofa 0s (Summerville Crosses false
    L5=5 Under). Sofa omitted-zero coalesce still applies only inside a date.
    """
    exclude_sql, exclude_params = _allstar_team_exclusion_sql("soccer")
    date_sql, date_params = _allstar_date_exclusion_sql("soccer")
    select_sql, _val_filter = _soccer_select_and_filter(stat_expr)
    # Appearance = any box row for the player (not prop-filtered).
    appeared = (
        "pa IS NOT NULL OR sh IS NOT NULL OR sog IS NOT NULL "
        "OR fc IS NOT NULL OR tk IS NOT NULL OR sv IS NOT NULL "
        "OR crosses IS NOT NULL OR clearances IS NOT NULL "
        "OR dribble_attempts IS NOT NULL "
        "OR coalesce(minutes_played, 0) > 0"
    )
    sql = f"""
        SELECT game_date, event_id, {select_sql}, team
        FROM soccer
        WHERE {where_clause}
          AND ({appeared})
          AND ({exclude_sql})
          AND ({date_sql})
        ORDER BY game_date DESC,
                 CASE WHEN coalesce(event_id, '') LIKE 'sofa_%' THEN 1 ELSE 0 END
    """
    try:
        rows = con.execute(
            sql, tuple(params) + tuple(exclude_params) + tuple(date_params)
        ).fetchall()
    except Exception:
        return []

    def _window(*, prefer_hint: bool) -> List[float]:
        seen: set[str] = set()
        out: List[float] = []
        hint = str(team_hint or "").strip()
        for gd, _eid, val, db_team in rows:
            key = str(gd or "")[:10]
            if not key or key in seen:
                continue
            if prefer_hint and hint and not _soccer_club_match(hint, str(db_team or "")):
                continue
            seen.add(key)
            # Gap: played this date but prop missing — do not skip to older games.
            if val is None:
                break
            out.append(float(val))
            if len(out) >= n:
                break
        return out

    hint = str(team_hint or "").strip()
    matched = _window(prefer_hint=True) if hint else []
    vals = _window(prefer_hint=False)
    if hint and len(matched) >= n:
        return matched[:n]
    if hint and matched and len(matched) >= len(vals):
        return matched[:n]
    if vals:
        return vals[:n]
    if hint and matched:
        return matched[:n]
    return []


def _nfkd_norm(s: str) -> str:
    """Strip accents via NFKD — matches step2 norm_name_strict and step4 bridge."""
    import unicodedata as _ud
    n = _ud.normalize("NFKD", str(s).strip().lower())
    return "".join(c for c in n if not _ud.combining(c))


# Last April is not tonight's preseason minutes. NBAP last-5 starts here.
NBAP_L5_FLOOR = "2026-10-01"


def nbap_l5_floor(league: str = "", league_id: str = "") -> str:
    """Date floor for NBAP rows. Empty string means the normal NBA window."""
    lg = str(league or "").strip().upper()
    lid = str(league_id or "").strip()
    if lg in {"NBAP", "NBA PRESEASON", "NBA-PRESEASON"} or lid == "237":
        return NBAP_L5_FLOOR
    return ""


def get_vals_nba(con: sqlite3.Connection, espn_id: str,
                  prop_norm: str, n: int = 10, player_name: str = "",
                  min_game_date: str = "",
                  card_window: bool = False,
                  regular_season_only: bool = False) -> List[float]:
    expr = _resolve_prop(prop_norm, "nba")
    if not expr:
        return []
    floor = str(min_game_date or "")[:10]
    # Primary: ESPN athlete ID (exact, unambiguous)
    vals = _query_vals(con, "nba",
                       "ESPN_ATHLETE_ID = ?", expr, (str(espn_id),), n,
                       min_game_date=floor,
                       card_window=card_window,
                       regular_season_only=regular_season_only)
    if not vals and player_name:
        # Fallback 1: exact lowercase match (covers DB storing accented names)
        norm = player_name.strip().lower()
        vals = _query_vals(con, "nba",
                           "lower(player) = ?", expr, (norm,), n,
                           min_game_date=floor,
                           card_window=card_window,
                           regular_season_only=regular_season_only)
        # Fallback 2: NFKD-normalized match (covers DB storing ASCII names vs
        #             PrizePicks sending accented names, or vice versa)
        if not vals:
            norm_ascii = _nfkd_norm(player_name)
            if norm_ascii != norm:
                # Fetch candidates by first+last token, filter to exact NFKD match in Python
                parts = norm_ascii.split()
                if len(parts) >= 2:
                    first, last = parts[0], parts[-1]
                    try:
                        exclude_sql, exclude_params = _allstar_team_exclusion_sql("NBA")
                        floor_sql = " AND game_date >= ?" if floor else ""
                        floor_params = (floor,) if floor else ()
                        sql = f"""
                            SELECT player, {expr} AS val
                            FROM nba
                            WHERE lower(player) LIKE ?
                              AND lower(player) LIKE ?
                              AND {expr} IS NOT NULL
                              AND ({exclude_sql})
                              {floor_sql}
                            ORDER BY game_date DESC
                            LIMIT {n * 5}
                        """
                        rows = con.execute(
                            sql,
                            (f"{first}%", f"%{last}") + tuple(exclude_params) + tuple(floor_params),
                        ).fetchall()
                        vals = [
                            float(r[1]) for r in rows
                            if r[1] is not None and _nfkd_norm(r[0]) == norm_ascii
                        ][:n]
                    except Exception:
                        pass
    return vals


def get_vals_cbb(con: sqlite3.Connection, espn_id: str,
                  prop_norm: str, n: int = 10) -> List[float]:
    expr = _resolve_prop(prop_norm, "cbb")
    if not expr:
        return []
    return _query_vals(con, "cbb",
                       "ESPN_ATHLETE_ID = ?", expr, (str(espn_id),), n)


def _nhl_lookup_name(player: str) -> str:
    p = str(player or "").strip()
    if not p:
        return p
    key = _nfkd_norm(p)
    return _NHL_NAME_ALIASES.get(key, p)


def get_vals_nhl(con: sqlite3.Connection, player: str,
                  prop_norm: str, n: int = 10) -> List[float]:
    """
    NHL lookup by player name (no ESPN ID in NHL pipeline).
    Exact match first, then case-insensitive / NFKD / alias fallback.
    """
    expr = _resolve_prop(prop_norm, "nhl")
    if not expr:
        return []
    lookup = _nhl_lookup_name(player)
    vals = _query_vals(con, "nhl",
                       "player = ?", expr, (lookup,), n)
    if not vals:
        vals = _query_vals(con, "nhl",
                           "lower(player) = lower(?)", expr, (lookup,), n)
    if not vals and lookup != player:
        vals = _query_vals(con, "nhl",
                           "player = ?", expr, (str(player),), n)
        if not vals:
            vals = _query_vals(con, "nhl",
                               "lower(player) = lower(?)", expr, (str(player),), n)
    if not vals:
        norm_ascii = _nfkd_norm(player)
        if norm_ascii:
            parts = norm_ascii.split()
            if len(parts) >= 2:
                first, last = parts[0], parts[-1]
                try:
                    sql = f"""
                        SELECT player, {expr} AS val
                        FROM nhl
                        WHERE lower(player) LIKE ?
                          AND lower(player) LIKE ?
                          AND {expr} IS NOT NULL
                        ORDER BY game_date DESC
                        LIMIT {n * 5}
                    """
                    rows = con.execute(sql, (f"{first}%", f"%{last}")).fetchall()
                    vals = [
                        float(r[1]) for r in rows
                        if r[1] is not None and _nfkd_norm(r[0]) == norm_ascii
                    ][:n]
                except Exception:
                    pass
    return vals


def get_vals_soccer(con: sqlite3.Connection, espn_player_id: str,
                     prop_norm: str, n: int = 10,
                     player_name: str = "", team: str = "") -> List[float]:
    expr = _resolve_prop(prop_norm, "soccer")
    if not expr:
        return []
    # Normalize ID (CSV float strings like "12345.0")
    eid = str(espn_player_id or "").strip()
    if eid.endswith(".0") and eid[:-2].isdigit():
        eid = eid[:-2]
    team_hint = str(team or "").strip()

    if not eid:
        vals = []
    elif player_name:
        # Wrong/shared ESPN IDs were copying one keeper/shooter log onto the whole slate.
        vals = _query_soccer_vals(
            con,
            "espn_player_id = ? AND lower(player) = ?",
            expr,
            (eid, player_name.strip().lower()),
            n,
            team_hint=team_hint,
        )
    else:
        vals = _query_soccer_vals(
            con, "espn_player_id = ?", expr, (eid,), n, team_hint=team_hint
        )

    # Fallback 1: exact name match (DB often uses a different ID space than PrizePicks cache)
    if len(vals) < 5 and player_name:
        norm = player_name.strip().lower()
        named = _query_soccer_vals(
            con, "lower(player) = ?", expr, (norm,), n, team_hint=team_hint
        )
        if len(named) > len(vals):
            vals = named

    # Fallback 2: legacy FBref rows (subset of name match; kept for explicitness)
    if len(vals) < 5 and player_name:
        norm = player_name.strip().lower()
        named = _query_soccer_vals(
            con,
            "lower(player) = ? AND espn_player_id LIKE 'fbref_%'",
            expr,
            (norm,),
            n,
            team_hint=team_hint,
        )
        if len(named) > len(vals):
            vals = named

    # Fallback 3: NFKD accent-normalized name via the same appearance-gap walker.
    # Do not re-query "non-null prop only" — that skips recent ESPN gaps and
    # resurrects old Sofa 0s (Summerville Crosses false L5=5 Under).
    # If we already keyed by ESPN id and hit a recent prop gap, stop (no pad).
    if len(vals) < 5 and player_name and not eid:
        norm_ascii = _nfkd_norm(player_name)
        parts = [p for p in norm_ascii.split() if p]
        last = parts[-1] if parts else ""
        needle = last[:4] if len(last) >= 4 else last
        if needle:
            try:
                select_sql, _vf = _soccer_select_and_filter(expr)
                appeared = (
                    "pa IS NOT NULL OR sh IS NOT NULL OR sog IS NOT NULL "
                    "OR fc IS NOT NULL OR tk IS NOT NULL OR sv IS NOT NULL "
                    "OR crosses IS NOT NULL OR clearances IS NOT NULL "
                    "OR dribble_attempts IS NOT NULL "
                    "OR coalesce(minutes_played, 0) > 0"
                )
                sql = f"""
                    SELECT player, game_date, team, {select_sql}
                    FROM soccer
                    WHERE ({appeared})
                      AND player LIKE ?
                    ORDER BY game_date DESC,
                             CASE WHEN coalesce(event_id, '') LIKE 'sofa_%' THEN 1 ELSE 0 END
                    LIMIT ?
                """
                rows = con.execute(sql, (f"%{needle}%", n * 40)).fetchall()
                seen_dates: set[str] = set()
                named: List[float] = []
                for player, gd, db_team, val in rows:
                    if _nfkd_norm(str(player)) != norm_ascii:
                        continue
                    if team_hint and not _soccer_club_match(team_hint, str(db_team or "")):
                        continue
                    key = str(gd or "")[:10]
                    if not key or key in seen_dates:
                        continue
                    seen_dates.add(key)
                    if val is None:
                        break
                    named.append(float(val))
                    if len(named) >= n:
                        break
                if len(named) > len(vals):
                    vals = named
            except Exception:
                pass

    return vals


# ── Minutes / passes lookups (soccer context columns) ─────────────────────────
def get_avg_minutes_soccer(con: sqlite3.Connection, espn_player_id: str,
                            n: int = 5, player_name: str = "") -> Optional[float]:
    vals = get_vals_soccer(con, espn_player_id, "minutes", n, player_name=player_name)
    return float(np.mean(vals)) if vals else None


def get_avg_passes_soccer(con: sqlite3.Connection, espn_player_id: str,
                           n: int = 5, player_name: str = "") -> Optional[float]:
    vals = get_vals_soccer(con, espn_player_id, "passes", n, player_name=player_name)
    return float(np.mean(vals)) if vals else None


def get_avg_minutes_nba(con: sqlite3.Connection, espn_id: str,
                         n: int = 5, table: str = "nba") -> Optional[float]:
    vals = _query_vals(con, table, "ESPN_ATHLETE_ID = ?",
                       "minutes", (str(espn_id),), n)
    return float(np.mean(vals)) if vals else None


# ── Combo player support ───────────────────────────────────────────────────────
def get_vals_combo(get_fn, con, ids: List[str], prop_norm: str, n: int) -> List[float]:
    """
    Sum stats across multiple players (combo props).
    Aligns by game index (most recent game 1, etc.).
    Returns summed values for games where ALL players have data.
    """
    per_player = []
    for pid in ids:
        vals = get_fn(con, pid, prop_norm, n)
        if not vals:
            return []
        per_player.append(vals)
    min_games = min(len(v) for v in per_player)
    if min_games == 0:
        return []
    return [sum(v[i] for v in per_player) for i in range(min_games)]


# ── Hit rate math ──────────────────────────────────────────────────────────────
def calc_hit_context(vals: List[float], line: float, k: int = 5):
    """
    Returns (over, under, push, hit_rate_all, hit_rate_ou, under_rate_ou)
    for the first k values (most-recent first).
    """
    over = under = push = 0
    for v in vals[:k]:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        if v > line:
            over += 1
        elif v < line:
            under += 1
        else:
            push += 1
    total_all = over + under + push
    total_ou  = over + under
    hr_all  = (over / total_all) if total_all > 0 else np.nan
    hr_ou   = (over / total_ou)  if total_ou  > 0 else np.nan
    ur_ou   = (under / total_ou) if total_ou  > 0 else np.nan
    return over, under, push, hr_all, hr_ou, ur_ou


def fmt_num(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return f"{float(x):.3f}".rstrip("0").rstrip(".")


# ── Main attach function ───────────────────────────────────────────────────────
def attach_stats(
    slate: pd.DataFrame,
    sport: str,                   # "nba" | "cbb" | "nhl" | "soccer"
    con: sqlite3.Connection,
    id_col: str,                  # column with player ID (espn_id or player name for NHL)
    prop_col: str = "prop_norm",
    line_col: str = "line",
    n: int = 10,
    combo_sep: str = "|",
    slate_date: str = "",
) -> pd.DataFrame:
    """
    Attach stat columns to slate DataFrame from the SQLite reference DB.

    id_col:   for NBA/CBB/Soccer: ESPN athlete ID column
              for NHL: player name column (no ESPN ID in NHL pipeline)
    """
    sport = sport.lower()
    # Period boards share the full-game box table / L5 attach path.
    if sport in ("nba1q", "nba1h", "nbap"):
        sport = "nba"

    # Determine which get_fn to use
    _get_fns = {
        "nba":    get_vals_nba,
        "cbb":    get_vals_cbb,
        "nhl":    get_vals_nhl,
        "soccer": get_vals_soccer,
    }
    get_fn = _get_fns.get(sport)
    if get_fn is None:
        raise ValueError(f"Unknown sport: {sport}. Must be one of: nba, cbb, nhl, soccer")

    # Ensure output columns exist
    stat_cols = [f"stat_g{i}" for i in range(1, n + 1)]
    out_cols = stat_cols + [
        "stat_last5_avg", "stat_last10_avg", "stat_season_avg",
        "last5_over", "last5_under", "last5_push", "last5_hit_rate",
        "line_hit_rate_over_ou_5",  "line_hit_rate_under_ou_5",
        "line_hit_rate_over_ou_10", "line_hit_rate_under_ou_10",
        "stat_status",
    ]
    if sport in ("nba", "cbb"):
        out_cols.append("min_last5_avg")
    if sport == "soccer":
        out_cols += ["avg_minutes", "avg_passes"]

    for c in out_cols:
        if c not in slate.columns:
            slate[c] = ""

    slate["_line_num"] = pd.to_numeric(slate.get(line_col, ""), errors="coerce")

    status_counts = {"OK": 0, "NO_ID": 0, "NO_DATA": 0,
                     "UNSUPPORTED_PROP": 0, "INSUFFICIENT_GAMES": 0}

    for idx, row in slate.iterrows():
        prop     = str(row.get(prop_col, "")).lower().strip()
        raw_id   = str(row.get(id_col,   "")).strip()
        line     = row.get("_line_num", np.nan)
        try:
            line = float(line)
        except Exception:
            line = np.nan

        # Check prop is supported
        if not _resolve_prop(prop, sport):
            slate.at[idx, "stat_status"] = "UNSUPPORTED_PROP"
            status_counts["UNSUPPORTED_PROP"] += 1
            continue

        # Parse IDs — support combo players (pipe-separated)
        player_name = str(row.get("player", "")).strip()
        if not raw_id or raw_id in ("nan", ""):
            is_combo_flag = (
                str(row.get("is_combo_player", "")).strip().lower() in ("1", "true", "yes")
                or "+" in player_name
            )
            if is_combo_flag:
                slate.at[idx, "stat_status"] = "NO_ID"
                status_counts["NO_ID"] += 1
                continue
            # Soccer Popular markets can attach by player name after Sofascore fill
            # even when step2 ESPN IDs are still blank (Saudi roster lag).
            if sport == "soccer" and player_name:
                raw_id = ""
            else:
                slate.at[idx, "stat_status"] = "NO_ID"
                status_counts["NO_ID"] += 1
                continue

        is_combo = (combo_sep in raw_id) or (
            str(row.get("is_combo_player", "")).strip().lower() in ("1", "true", "yes")
        )

        if is_combo:
            ids = [p.strip() for p in raw_id.split(combo_sep) if p.strip()]
            vals = get_vals_combo(get_fn, con, ids, prop, n)
        else:
            player_name = str(row.get("player", "")).strip()
            if sport == "nba" and player_name:
                floor = nbap_l5_floor(
                    str(row.get("league") or ""),
                    str(row.get("league_id") or ""),
                )
                # PP card L5 = RS + playoffs, never preseason.
                vals = get_fn(
                    con, raw_id, prop, n,
                    player_name=player_name, min_game_date=floor,
                    card_window=True,
                )
            elif sport == "cbb" and player_name:
                vals = get_fn(con, raw_id, prop, n, player_name=player_name)
            elif sport == "soccer" and player_name:
                team = str(row.get("team") or row.get("pp_home_team") or "").strip()
                vals = get_fn(
                    con, raw_id, prop, n, player_name=player_name, team=team
                )
            else:
                vals = get_fn(con, raw_id, prop, n)

        if not vals:
            slate.at[idx, "stat_status"] = "NO_DATA"
            status_counts["NO_DATA"] += 1
            continue

        if len(vals) < 2:
            slate.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
            status_counts["INSUFFICIENT_GAMES"] += 1
            # Still attach what we have rather than voiding
            # (1-game players are better than no data)

        # ── Fill per-game columns ──────────────────────────────────────────
        for i in range(1, n + 1):
            v = vals[i - 1] if (i - 1) < len(vals) else np.nan
            slate.at[idx, f"stat_g{i}"] = fmt_num(v)

        def avg_k(k):
            s = vals[:k] if len(vals) >= k else vals
            return float(np.mean(s)) if s else np.nan

        slate.at[idx, "stat_last5_avg"]  = fmt_num(avg_k(5))
        slate.at[idx, "stat_last10_avg"] = fmt_num(avg_k(10))
        slate.at[idx, "stat_season_avg"] = fmt_num(float(np.mean(vals)))

        # ── Hit rates ──────────────────────────────────────────────────────
        if not np.isnan(line):
            o5, u5, p5, hr5, hr5_ou, ur5_ou = calc_hit_context(vals, line, k=5)
            slate.at[idx, "last5_over"]               = str(o5)
            slate.at[idx, "last5_under"]              = str(u5)
            slate.at[idx, "last5_push"]               = str(p5)
            slate.at[idx, "last5_hit_rate"]           = fmt_num(hr5)
            slate.at[idx, "line_hit_rate_over_ou_5"]  = fmt_num(hr5_ou)
            slate.at[idx, "line_hit_rate_under_ou_5"] = fmt_num(ur5_ou)
            _, _, _, _, hr10_ou, ur10_ou = calc_hit_context(vals, line, k=10)
            slate.at[idx, "line_hit_rate_over_ou_10"]  = fmt_num(hr10_ou)
            slate.at[idx, "line_hit_rate_under_ou_10"] = fmt_num(ur10_ou)

        # ── Sport-specific context columns ─────────────────────────────────
        if sport in ("nba", "cbb") and not is_combo:
            min_avg = get_avg_minutes_nba(con, raw_id, n=5, table=sport)
            if min_avg is not None:
                slate.at[idx, "min_last5_avg"] = fmt_num(min_avg)

        if sport == "soccer" and not is_combo:
            pname = str(row.get("player", "")).strip()
            min_avg  = get_avg_minutes_soccer(con, raw_id, n=5, player_name=pname)
            pass_avg = get_avg_passes_soccer(con, raw_id, n=5, player_name=pname)
            if min_avg  is not None: slate.at[idx, "avg_minutes"] = fmt_num(min_avg)
            if pass_avg is not None: slate.at[idx, "avg_passes"]  = fmt_num(pass_avg)

        if len(vals) >= 2:
            slate.at[idx, "stat_status"] = "OK"
            status_counts["OK"] += 1

    slate = _attach_role_stability(slate, sport, con, id_col, is_combo_sep=combo_sep)
    slate = slate.drop(columns=["_line_num"], errors="ignore")
    return slate, status_counts


def _attach_role_stability(
    slate: pd.DataFrame,
    sport: str,
    con: sqlite3.Connection,
    id_col: str,
    is_combo_sep: str = "|",
) -> pd.DataFrame:
    """Add minutes_L10_list, role_stability_score, high_variance_role from DB samples."""
    from role_stability import role_stability

    sport = sport.lower()
    if "minutes_L10_list" not in slate.columns:
        slate["minutes_L10_list"] = "[]"
    slate["role_stability_score"] = np.nan
    slate["high_variance_role"] = False

    for idx, row in slate.iterrows():
        raw_id = str(row.get(id_col, "")).strip()
        if not raw_id or raw_id in ("nan", "") or is_combo_sep in raw_id:
            continue
        mins: List[float] = []
        if sport in ("nba", "cbb", "nba1h", "nba1q"):
            table = "nba" if sport.startswith("nba") else sport
            floor = ""
            if sport == "nba":
                floor = nbap_l5_floor(
                    str(row.get("league") or ""),
                    str(row.get("league_id") or ""),
                )
            mins = _query_vals(
                con, table, "ESPN_ATHLETE_ID = ?", "minutes", (raw_id,), 10,
                min_game_date=floor,
            )
        elif sport == "nhl":
            pname = str(row.get("player_name", row.get("player", ""))).strip()
            mins = get_vals_nhl(con, pname, "time on ice", 10)
        elif sport == "soccer":
            pname = str(row.get("player", "")).strip()
            mins = get_vals_soccer(con, raw_id, "minutes", 10, player_name=pname)

        if len(mins) < 3:
            continue
        slate.at[idx, "minutes_L10_list"] = json.dumps(mins)
        score = role_stability(mins)
        if score is not None:
            slate.at[idx, "role_stability_score"] = score
            slate.at[idx, "high_variance_role"] = score < 0.35
    return slate


# ── DB health check ────────────────────────────────────────────────────────────
def db_summary(con: sqlite3.Connection) -> None:
    print("\n── PropOracle Ref DB Summary ──────────────────────")
    for table in ("nba", "cbb", "nhl", "soccer"):
        try:
            total   = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            dates   = con.execute(f"SELECT MIN(game_date), MAX(game_date) FROM {table}").fetchone()
            players = con.execute(f"SELECT COUNT(DISTINCT player) FROM {table}").fetchone()[0]
            print(f"  {table:8s}  {total:>7,} rows  {players:>5,} players  "
                  f"{dates[0] or '—'} → {dates[1] or '—'}")
        except Exception:
            print(f"  {table:8s}  (not found)")
    print()


