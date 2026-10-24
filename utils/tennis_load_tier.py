"""Tennis match-load / fatigue proxy → minutes_tier labels.

Uses Sackmann ``minutes`` (preferred), else match_total_games / total_sets,
plus days since last match. Soft score path treats HIGH as OVER-friendly
(fresh) and LOW as UNDER-friendly (fatigued) — same as basketball volume.
Unknown stays neutral (not HIGH/LOW).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping

import numpy as np
import pandas as pd

# Court-time bands (Sackmann minutes): long match + short rest → fatigue (LOW).
_HIGH_MINUTES = 150.0  # long prior match
_LOW_MINUTES = 90.0  # short prior match
_HIGH_GAMES = 35.0  # fallback when minutes missing
_LOW_GAMES = 22.0
_SHORT_REST_DAYS = 1
_LONG_REST_DAYS = 3


def _to_float(v: object) -> float | None:
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def parse_slate_date(val: object) -> date | None:
    """Best-effort slate / start-time date (ISO or YYYYMMDD)."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        try:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")[:19]).date()
    except ValueError:
        return None


def days_rest(last_match: date | None, slate: date | None) -> float | None:
    if last_match is None or slate is None:
        return None
    return float((slate - last_match).days)


def load_tier_from_features(
    *,
    last_minutes: float | None = None,
    last_match_games: float | None = None,
    last_total_sets: float | None = None,
    days_since: float | None = None,
) -> str:
    """
    Map load features → HIGH / MEDIUM / LOW / UNKNOWN.

    HIGH = fresh (soft favors OVER); LOW = fatigued (soft favors UNDER).
    """
    minutes = last_minutes
    games = last_match_games
    sets = last_total_sets
    rest = days_since

    # Proxy minutes from games when Sackmann minutes absent (~3.5 min/game).
    if minutes is None and games is not None and games > 0:
        minutes = float(games) * 3.5
    if minutes is None and sets is not None and sets > 0:
        minutes = float(sets) * 45.0  # rough set-length proxy

    if minutes is None and rest is None:
        return "UNKNOWN"

    fatigued = False
    fresh = False

    if minutes is not None:
        if minutes >= _HIGH_MINUTES:
            fatigued = True
        elif minutes <= _LOW_MINUTES:
            fresh = True

    if games is not None:
        if games >= _HIGH_GAMES:
            fatigued = True
        elif games <= _LOW_GAMES and minutes is None:
            fresh = True

    if sets is not None and sets >= 4:
        fatigued = True

    if rest is not None:
        if rest <= _SHORT_REST_DAYS and (fatigued or (minutes is not None and minutes >= 120)):
            return "LOW"
        if rest >= _LONG_REST_DAYS and not fatigued:
            return "HIGH"
        if rest <= 0:
            # Same-day / unknown ordering — treat as short rest if long prior.
            if fatigued:
                return "LOW"

    if fatigued and not fresh:
        return "LOW"
    if fresh and not fatigued:
        return "HIGH"
    if minutes is not None or rest is not None:
        return "MEDIUM"
    return "UNKNOWN"


def extract_last_match_load(
    matches: list[Mapping[str, Any]] | None,
    *,
    slate: date | None = None,
) -> dict[str, Any]:
    """
    From newest-first match history rows, pull last-match load fields.

    Expected keys per row: date, minutes, match_total_games, total_sets
    (Sackmann index / ESPN match cache).
    """
    out: dict[str, Any] = {
        "last_match_date": None,
        "last_match_minutes": np.nan,
        "last_match_games": np.nan,
        "last_match_sets": np.nan,
        "days_rest": np.nan,
        "minutes_tier": "UNKNOWN",
    }
    if not matches:
        return out

    row = matches[0]
    last_d = parse_slate_date(row.get("date") or row.get("match_date_utc") or row.get("tourney_date"))
    mins = _to_float(row.get("minutes"))
    games = _to_float(row.get("match_total_games"))
    sets = _to_float(row.get("total_sets"))
    rest = days_rest(last_d, slate)

    out["last_match_date"] = last_d.isoformat() if last_d else ""
    out["last_match_minutes"] = mins if mins is not None else np.nan
    out["last_match_games"] = games if games is not None else np.nan
    out["last_match_sets"] = sets if sets is not None else np.nan
    out["days_rest"] = rest if rest is not None else np.nan
    out["minutes_tier"] = load_tier_from_features(
        last_minutes=mins,
        last_match_games=games,
        last_total_sets=sets,
        days_since=rest,
    )
    return out


def assign_tennis_load_tier(
    df: pd.DataFrame,
    *,
    history_by_player: Mapping[str, list[Mapping[str, Any]]] | None = None,
    history_by_espn_id: Mapping[str, list[Mapping[str, Any]]] | None = None,
    player_col: str = "player",
    espn_col: str = "espn_athlete_id",
    slate_col: str | None = None,
    player_key_fn=None,
) -> pd.DataFrame:
    """
    Fill minutes_tier + load columns from player match history.

    Missing history → UNKNOWN (neutral). Does not hard-void rows.
    """
    out = df.copy()
    n = len(out)
    tiers: list[str] = []
    last_mins: list[float] = []
    last_games: list[float] = []
    last_sets: list[float] = []
    rests: list[float] = []
    last_dates: list[str] = []

    by_pk = history_by_player or {}
    by_id = history_by_espn_id or {}

    date_cols = []
    if slate_col and slate_col in out.columns:
        date_cols.append(slate_col)
    for c in ("start_time", "game_time", "game_date", "slate_date", "commence_time"):
        if c in out.columns and c not in date_cols:
            date_cols.append(c)

    for i in range(n):
        row = out.iloc[i]
        slate = None
        for c in date_cols:
            slate = parse_slate_date(row.get(c))
            if slate:
                break

        hist: list[Mapping[str, Any]] | None = None
        pname = str(row.get(player_col, "") or "")
        pk = player_key_fn(pname) if player_key_fn else pname
        # Prefer Sackmann (has minutes) over ESPN scoreboard (games/sets only).
        if pk and pk in by_pk:
            hist = list(by_pk[pk])
        if not hist:
            aid = str(row.get(espn_col, "") or "").strip()
            if aid and aid in by_id:
                hist = list(by_id[aid])

        feat = extract_last_match_load(hist, slate=slate)
        tiers.append(str(feat["minutes_tier"]))
        last_mins.append(float(feat["last_match_minutes"]) if feat["last_match_minutes"] == feat["last_match_minutes"] else np.nan)
        last_games.append(float(feat["last_match_games"]) if feat["last_match_games"] == feat["last_match_games"] else np.nan)
        last_sets.append(float(feat["last_match_sets"]) if feat["last_match_sets"] == feat["last_match_sets"] else np.nan)
        rests.append(float(feat["days_rest"]) if feat["days_rest"] == feat["days_rest"] else np.nan)
        last_dates.append(str(feat.get("last_match_date") or ""))

    out["minutes_tier"] = tiers
    out["last_match_minutes"] = last_mins
    out["last_match_games"] = last_games
    out["last_match_sets"] = last_sets
    out["days_rest"] = rests
    out["last_match_date"] = last_dates
    # Soft score / step8 "Avg Min" display: prefer real minutes.
    out["avg_minutes"] = pd.to_numeric(out["last_match_minutes"], errors="coerce")
    return out
