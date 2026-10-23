"""Season / backtest from-dates for active sports (2026 slate).

Use season opener when known. Fall back to 2026-01-01 when the opener is
year-round / unclear (Soccer, Tennis). MLB All-Star break dates are excluded
via ``utils.allstar_filter`` (2026-07-13..15).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

# Inclusive calendar from-dates for L5 / consistency season windows.
SPORT_FROM_DATES: dict[str, str] = {
    "WNBA": "2026-05-01",  # 2026 season graded opener
    "MLB": "2026-03-30",  # 2026 regular-season opener (first graded_props MLB day)
    "SOCCER": "2026-01-01",  # year-round; opener unclear → Jan 1
    "TENNIS": "2026-01-01",  # year-round; opener unclear → Jan 1
    "NBA": "2025-10-21",
    "NHL": "2025-10-07",
    "CBB": "2025-11-04",
    "NFL": "2026-09-10",
    "CFB": "2026-08-23",
    "GOLF": "2026-01-01",
}

SPORT_FROM_NOTES: dict[str, str] = {
    "WNBA": "Season graded opener (first WNBA graded_props day).",
    "MLB": "Regular-season opener; ASG break 2026-07-13..15 excluded.",
    "SOCCER": "Jan 1 fallback (year-round calendar); first graded file may be later.",
    "TENNIS": "Jan 1 fallback (year-round calendar); first graded file may be later.",
    "NBA": "2025-26 regular-season opener (graded archive).",
    "NHL": "2025-26 regular-season opener (graded archive).",
    "CBB": "2025-26 season opener (graded archive; live L5 from step8).",
    "NFL": "2026 regular-season Week 1 (grades land after build_grades_html export).",
    "CFB": "2026 Week 1 (grades land after build_grades_html export).",
    "GOLF": "Year-round PGA; first graded file may be later.",
}

# Canonical sport keys used in graded_props JSON.
ACTIVE_SPORTS: tuple[str, ...] = (
    "WNBA",
    "MLB",
    "SOCCER",
    "TENNIS",
    "NBA",
    "NHL",
    "CBB",
    "NFL",
    "CFB",
    "GOLF",
)

MLB_ASG_HARD: frozenset[str] = frozenset({"2026-07-13", "2026-07-14", "2026-07-15"})


def nba_stats_season(as_of: str | date | datetime | None = None) -> str:
    """stats.nba.com season string (``2026-27``) for a calendar date.

    NBA regular season opens in October. October 2026 is ``2026-27``, not
    the completed ``2025-26`` year still hardcoded on some pipeline callers.
    """
    if as_of is None:
        y, m = date.today().year, date.today().month
    elif isinstance(as_of, datetime):
        y, m = as_of.year, as_of.month
    elif isinstance(as_of, date):
        y, m = as_of.year, as_of.month
    else:
        s = str(as_of or "").strip()[:10]
        if len(s) >= 7 and s[4] == "-":
            y, m = int(s[:4]), int(s[5:7])
        else:
            y, m = date.today().year, date.today().month
    start = y if m >= 10 else y - 1
    return f"{start}-{str(start + 1)[-2:]}"


def prior_nba_stats_season(season: str | None = None) -> str:
    raw = str(season or nba_stats_season()).strip()
    try:
        start = int(raw.split("-", 1)[0])
    except (TypeError, ValueError):
        start = date.today().year if date.today().month >= 10 else date.today().year - 1
    return f"{start - 1}-{str(start)[-2:]}"


def sport_from_date(sport: str) -> str:
    key = str(sport or "").upper().strip()
    if key == "SOCCER":
        key = "SOCCER"
    return SPORT_FROM_DATES.get(key, "2026-01-01")


def from_dates_payload() -> dict[str, Any]:
    return {
        sport: {
            "from": SPORT_FROM_DATES[sport],
            "note": SPORT_FROM_NOTES.get(sport, ""),
        }
        for sport in ACTIVE_SPORTS
    }
