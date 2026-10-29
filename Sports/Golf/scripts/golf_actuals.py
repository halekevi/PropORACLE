#!/usr/bin/env python3
"""Shared PGA round actuals for fetch_golf_actuals + golf_grader."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

_GOLF_SCRIPTS = Path(__file__).resolve().parent
_REPO = _GOLF_SCRIPTS.parents[2]
if str(_GOLF_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_GOLF_SCRIPTS))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from step4_attach_player_stats_golf import (  # noqa: E402
    CACHE_COLUMNS,
    ensure_round_cache,
    load_round_cache,
    prop_stat_key,
    _player_key,
)
from utils.golf_windows import is_missed_cut_status, is_wd_status  # noqa: E402

STAT_TO_PROP = {
    "strokes": "Strokes",
    "birdies_or_better": "Birdies Or Better",
    "pars": "Pars",
    "bogeys_or_worse": "Bogeys Or Worse",
}

DEFAULT_CACHE = _REPO / "Sports" / "Golf" / "cache" / "golf_round_cache.csv"


@dataclass(frozen=True)
class GolfPlayerDay:
    """One player's round on a calendar day, or why it cannot grade."""

    status: str  # OK, WD, INCOMPLETE, MISSED_CUT, NO_DATA
    rec: pd.Series | None = None
    round_n: object = None


def round_calendar_date(tournament_date: str, round_n: object) -> str:
    """Map ESPN event start + round number to the round's calendar day (Thu=R1)."""
    raw = str(tournament_date or "").strip()[:10]
    try:
        d0 = date.fromisoformat(raw)
        rn = int(float(round_n))
    except (TypeError, ValueError):
        return raw
    if rn < 1:
        rn = 1
    return (d0 + timedelta(days=rn - 1)).isoformat()


def load_golf_round_cache(cache_path: Path | None = None, *, force_refresh: bool = False) -> pd.DataFrame:
    path = Path(cache_path) if cache_path else DEFAULT_CACHE
    if not path.is_absolute():
        path = _REPO / path
    if force_refresh or not path.is_file():
        return ensure_round_cache(path, weeks_back=12, force_refresh=True)
    return load_round_cache(path)


def _row_round_date(rec: pd.Series) -> str:
    rd = str(rec.get("round_date") or "").strip()[:10]
    if rd and rd.lower() not in {"nan", "none", "nat", ""}:
        return rd
    return round_calendar_date(rec.get("tournament_date"), rec.get("round"))


def _annotate_round_dates(cache: pd.DataFrame) -> pd.DataFrame:
    work = cache.copy()
    if "player_key" not in work.columns:
        work["player_key"] = work.get("player_name", "").map(_player_key)
    work["_round_date"] = [_row_round_date(rec) for _, rec in work.iterrows()]
    return work


def _event_key(rec: pd.Series) -> str:
    eid = str(rec.get("event_id") or "").strip()
    if eid and eid.lower() not in {"nan", "none"}:
        return f"id:{eid}"
    name = str(rec.get("tournament_name") or "").strip().lower()
    tdate = str(rec.get("tournament_date") or "").strip()[:10]
    return f"name:{name}|{tdate}"


def _is_short_round(rec: pd.Series) -> bool:
    holes = pd.to_numeric(rec.get("holes_played"), errors="coerce")
    return bool(pd.notna(holes) and float(holes) < 18)


def _row_status(rec: pd.Series) -> str:
    if is_wd_status(rec.get("player_status")) or _is_short_round(rec):
        return "WD" if is_wd_status(rec.get("player_status")) or _is_short_round(rec) else "INCOMPLETE"
    if _is_short_round(rec):
        return "INCOMPLETE"
    return "OK"


def _missed_cut(work: pd.DataFrame, player_key: str, want: str) -> bool:
    """True when this player posted R1/R2 but the field is on R3/R4 today."""
    day = work[work["_round_date"] == want]
    if day.empty:
        return False
    day_round = pd.to_numeric(day.get("round"), errors="coerce")
    weekend = day.loc[day_round.fillna(0) >= 3]
    if weekend.empty:
        return False
    events = {_event_key(r) for _, r in weekend.iterrows()}
    prior = work[work["player_key"].astype(str) == player_key]
    if prior.empty:
        return False
    for _, rec in prior.iterrows():
        if _event_key(rec) not in events:
            continue
        rn = pd.to_numeric(rec.get("round"), errors="coerce")
        if pd.notna(rn) and float(rn) <= 2 and not _is_short_round(rec):
            return True
        if is_missed_cut_status(rec.get("player_status")):
            return True
    return False


def lookup_player_day(cache: pd.DataFrame, target: str, player_key: str) -> GolfPlayerDay:
    """Status + cache row for ``player_key`` on calendar day ``target``."""
    if cache is None or cache.empty:
        return GolfPlayerDay("NO_DATA")
    pk = str(player_key or "").strip()
    if not pk:
        return GolfPlayerDay("NO_DATA")
    work = _annotate_round_dates(cache)
    want = str(target).strip()[:10]
    day = work[(work["_round_date"] == want) & (work["player_key"].astype(str) == pk)]
    if not day.empty:
        day = day.sort_values(["round"], kind="mergesort")
        rec = day.iloc[-1]
        if is_wd_status(rec.get("player_status")):
            return GolfPlayerDay("WD", rec, rec.get("round"))
        if _is_short_round(rec):
            return GolfPlayerDay("INCOMPLETE", rec, rec.get("round"))
        return GolfPlayerDay("OK", rec, rec.get("round"))
    if _missed_cut(work, pk, want):
        return GolfPlayerDay("MISSED_CUT")
    return GolfPlayerDay("NO_DATA")


def _float_or_none(raw: object) -> float | None:
    try:
        actual = float(raw)
    except (TypeError, ValueError):
        return None
    if actual != actual:
        return None
    return actual


def actuals_rows_for_date(cache: pd.DataFrame, target: str) -> list[dict[str, object]]:
    """One row per player × prop for completed rounds on ``target``.

    WD / short rounds / missed cuts are omitted so a 9-hole WD cannot grade
    as an Under hit. The golf grader VOID those via ``lookup_player_day``.
    """
    if cache is None or cache.empty:
        return []
    want = str(target).strip()[:10]
    work = _annotate_round_dates(cache)
    day = work[work["_round_date"] == want]
    if day.empty:
        return []
    day = day.sort_values(["player_key", "round"], kind="mergesort")
    day = day.drop_duplicates(subset=["player_key"], keep="last")
    rows: list[dict[str, object]] = []
    for _, rec in day.iterrows():
        if is_wd_status(rec.get("player_status")) or _is_short_round(rec):
            continue
        player = str(rec.get("player_name") or "").strip()
        if not player:
            continue
        team = str(rec.get("tournament_name") or "PGA").strip() or "PGA"
        for stat, prop in STAT_TO_PROP.items():
            actual = _float_or_none(rec.get(stat))
            if actual is None:
                continue
            rows.append(
                {
                    "player": player,
                    "team": team,
                    "prop_type": prop,
                    "actual": actual,
                    "round": rec.get("round"),
                    "round_date": want,
                }
            )
    return rows


def actuals_lookup(cache: pd.DataFrame, target: str) -> dict[tuple[str, str], float]:
    """(player_key, stat_col) → actual for completed rounds that calendar day."""
    if cache is None or cache.empty:
        return {}
    want = str(target).strip()[:10]
    work = _annotate_round_dates(cache)
    day = work[work["_round_date"] == want]
    if day.empty:
        return {}
    day = day.sort_values(["player_key", "round"], kind="mergesort")
    day = day.drop_duplicates(subset=["player_key"], keep="last")
    out: dict[tuple[str, str], float] = {}
    for _, rec in day.iterrows():
        if is_wd_status(rec.get("player_status")) or _is_short_round(rec):
            continue
        pk = str(rec.get("player_key") or "").strip()
        if not pk:
            continue
        for stat in STAT_TO_PROP:
            actual = _float_or_none(rec.get(stat))
            if actual is None:
                continue
            out[(pk, stat)] = actual
    return out


__all__ = [
    "CACHE_COLUMNS",
    "DEFAULT_CACHE",
    "STAT_TO_PROP",
    "GolfPlayerDay",
    "actuals_lookup",
    "actuals_rows_for_date",
    "load_golf_round_cache",
    "lookup_player_day",
    "prop_stat_key",
    "round_calendar_date",
    "_player_key",
]
