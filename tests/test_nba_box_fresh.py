"""NBA box DB freshness — last completed slate, not calendar yesterday."""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from utils.nba_minutes_role import assert_nba_box_fresh


def test_assert_nba_box_fresh_matches_last_completed_slate():
    fresh = pd.DataFrame({"game_date": ["2026-10-26"]})
    assert (
        assert_nba_box_fresh(
            as_of=date(2026, 10, 27),
            df=fresh,
            last_completed=date(2026, 10, 26),
        )
        == date(2026, 10, 26)
    )


def test_assert_nba_box_fresh_off_night_does_not_false_fail():
    """Thanksgiving / All-Star: last completed can be two+ days old."""
    df = pd.DataFrame({"game_date": ["2026-10-24"]})
    assert (
        assert_nba_box_fresh(
            as_of=date(2026, 10, 27),
            df=df,
            last_completed=date(2026, 10, 24),
        )
        == date(2026, 10, 24)
    )


def test_assert_nba_box_fresh_missing_completed_slate_fails():
    stale = pd.DataFrame({"game_date": ["2026-10-24"]})
    with pytest.raises(RuntimeError, match="last_completed_slate=2026-10-26"):
        assert_nba_box_fresh(
            as_of=date(2026, 10, 27),
            df=stale,
            last_completed=date(2026, 10, 26),
        )
