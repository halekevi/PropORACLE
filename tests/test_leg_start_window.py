"""Placeholder stamps stay unresolved. A real start with no match is a void."""
from utils.leg_start_window import (
    grade_dates_for_start,
    is_placeholder_start,
    missing_actual_disposition,
    usable_start_date,
    within_window,
)


def test_shared_11pm_stamp_is_not_a_start():
    stamp = "2026-09-21T23:00:00.000-04:00"
    assert is_placeholder_start(stamp)
    assert usable_start_date(stamp) == ""
    assert missing_actual_disposition(stamp) == "UNRESOLVED"


def test_usable_start_without_a_match_is_a_void():
    stamp = "2026-09-07T11:00:00.000-04:00"
    assert usable_start_date(stamp) == "2026-09-07"
    assert missing_actual_disposition(stamp) == "VOID"


def test_walkover_note_is_a_void_without_a_start():
    assert missing_actual_disposition("", "WALKOVER") == "VOID"


def test_evening_start_also_checks_the_next_eastern_date():
    # 8:20pm ET is inside six hours of midnight.
    dates = grade_dates_for_start("2026-09-27T20:20:00-04:00")
    assert dates[0] == "2026-09-27"
    assert "2026-09-28" in dates


def test_window_accepts_a_forty_minute_clock_difference():
    assert within_window(
        "2026-09-07T11:00:00-04:00",
        "2026-09-07T11:40:00-04:00",
    )
    assert not within_window(
        "2026-09-21T22:00:00-04:00",
        "2026-09-22T09:55:00-04:00",
    )
