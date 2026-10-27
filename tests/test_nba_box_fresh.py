"""NBA box DB freshness — fail closed after one missing night."""
from __future__ import annotations

from datetime import date

import pandas as pd

from utils.nba_minutes_role import assert_nba_box_fresh


def test_assert_nba_box_fresh_one_night_ok_two_nights_fail():
    fresh = pd.DataFrame({"game_date": ["2026-10-26"]})
    assert assert_nba_box_fresh(as_of=date(2026, 10, 27), max_stale_days=1, df=fresh) == date(
        2026, 10, 26
    )
    stale = pd.DataFrame({"game_date": ["2026-10-24"]})
    try:
        assert_nba_box_fresh(as_of=date(2026, 10, 27), max_stale_days=1, df=stale)
    except RuntimeError as exc:
        assert "stale" in str(exc)
        assert "2026-10-24" in str(exc)
    else:
        raise AssertionError("two-night box lag must fail closed")
