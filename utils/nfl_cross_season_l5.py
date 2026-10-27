"""NFL L5 window: current RS → current preseason → prior RS → prior preseason.

Weeks 1–4 of the regular season often have 0–2 current-season boxes. The
Goblin-70 / list L5 count is hits vs today's line in this assembled window,
not current-season games only. Disable with ``ALLOW_CROSS_SEASON_L5`` or
``PROPORACLE_ALLOW_CROSS_SEASON_L5=0``. CFB and other sports are untouched.
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Any

from utils.nfl_keep_gates import ALLOW_CROSS_SEASON_L5

log = logging.getLogger("nfl.l5")

NFL_L5_SOURCE_ORDER: tuple[str, ...] = (
    "current_season",
    "preseason_current",
    "prior_season",
    "preseason_prior",
)
_PRESEASON_MONTHS = frozenset({7, 8})
_FALSEY = frozenset({"0", "false", "no", "off"})
_TRUTHY = frozenset({"1", "true", "yes", "on"})


def allow_cross_season_l5() -> bool:
    env = str(os.environ.get("PROPORACLE_ALLOW_CROSS_SEASON_L5") or "").strip().lower()
    if env in _FALSEY:
        return False
    if env in _TRUTHY:
        return True
    return bool(ALLOW_CROSS_SEASON_L5)


def parse_box_date(raw: object) -> date | None:
    s = str(raw or "").strip()
    if not s:
        return None
    digits = s.replace("-", "")[:8]
    if len(digits) == 8 and digits.isdigit():
        try:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None


def nfl_season_start(slate: date) -> date:
    if slate.month >= 9:
        return date(slate.year, 9, 1)
    return date(slate.year - 1, 9, 1)


def nfl_week1_thursday(season_start: date) -> date:
    d = date(season_start.year, 9, 4)
    return d + timedelta(days=(3 - d.weekday()) % 7)


def nfl_cross_season_l5_active(slate: date) -> bool:
    """Weeks 1–4 of NFL regular season when the lookback flag is on.

    Mar–Aug is NFLP (prior Sep 1 is already 'current'). Week 5+ is
    current-season-only so the pad falls out of the window.
    """
    if not allow_cross_season_l5():
        return False
    if 3 <= slate.month <= 8:
        return False
    start = nfl_season_start(slate)
    week4_sunday = nfl_week1_thursday(start) + timedelta(days=24)
    return slate <= week4_sunday


def classify_nfl_l5_source(game_day: date, slate: date) -> str | None:
    start = nfl_season_start(slate)
    prior_start = date(start.year - 1, 9, 1)
    rs = game_day.month >= 9 or game_day.month <= 2
    if rs and game_day >= start:
        return "current_season"
    if game_day.month in _PRESEASON_MONTHS and game_day.year == start.year:
        return "preseason_current"
    if rs and prior_start <= game_day < start:
        return "prior_season"
    if game_day.month in _PRESEASON_MONTHS and game_day.year == prior_start.year:
        return "preseason_prior"
    return None


def source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {k: 0 for k in NFL_L5_SOURCE_ORDER}
    for row in rows:
        src = str(row.get("l5_source") or "")
        if src in counts:
            counts[src] += 1
    return counts


def format_l5_source_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{k}={counts[k]}" for k in NFL_L5_SOURCE_ORDER if counts.get(k))


def debug_l5_sources(player: object, prop_type: object, counts: dict[str, int]) -> None:
    log.debug(
        "%s %s L5 sources: %s",
        player,
        prop_type,
        format_l5_source_counts(counts) or "(none)",
    )


def assemble_nfl_l5_window(
    games: list[dict[str, Any]],
    slate: date,
    *,
    n: int = 5,
) -> list[dict[str, Any]]:
    """Newest-first per source, sources in ``NFL_L5_SOURCE_ORDER``, stop at ``n``.

    ``games`` are already decided (played + a numeric value). Each output row
    is a shallow copy with ``l5_source`` set. Empty input → empty window.
    """
    if n <= 0:
        return []
    buckets: dict[str, list[dict[str, Any]]] = {k: [] for k in NFL_L5_SOURCE_ORDER}
    for raw in games:
        day = parse_box_date(raw.get("game_date") or raw.get("_dt"))
        if day is None:
            continue
        if day >= slate:
            continue
        src = classify_nfl_l5_source(day, slate)
        if src is None:
            continue
        row = dict(raw)
        row["_dt"] = day
        row["l5_source"] = src
        buckets[src].append(row)
    out: list[dict[str, Any]] = []
    for src in NFL_L5_SOURCE_ORDER:
        bucket = buckets[src]
        bucket.sort(key=lambda r: r.get("_dt") or date.min, reverse=True)
        for row in bucket:
            out.append(row)
            if len(out) >= n:
                return out
    return out
