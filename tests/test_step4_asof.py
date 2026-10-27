"""Tonight's box must not enter L5."""
from __future__ import annotations

from utils.step4_asof import (
    box_date_before_slate,
    game_date_before_slate_sql,
    normalize_slate_date,
    row_l5_cutoff,
)


def test_normalize_and_strict_before():
    assert normalize_slate_date("2026-10-27") == "2026-10-27"
    assert normalize_slate_date("20261027") == "2026-10-27"
    assert box_date_before_slate("2026-10-26", "2026-10-27") is True
    assert box_date_before_slate("2026-10-27", "2026-10-27") is False
    assert box_date_before_slate("2026-10-28", "2026-10-27") is False
    assert box_date_before_slate("2026-10-27", "") is True


def test_row_cutoff_prefers_game_date():
    assert row_l5_cutoff({"game_date": "2026-10-26"}, "2026-10-27") == "2026-10-26"
    assert row_l5_cutoff({}, "2026-10-27") == "2026-10-27"
    sql, params = game_date_before_slate_sql("2026-10-27")
    assert "game_date < ?" in sql
    assert params == ("2026-10-27",)
    assert game_date_before_slate_sql("") == ("", ())
