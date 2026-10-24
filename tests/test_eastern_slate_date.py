"""Event starts file under the US Eastern slate day, not the UTC date."""
from utils.eastern_slate_date import eastern_slate_date, keep_event_on_slate, on_eastern_slate


def test_evening_eastern_start_stays_on_that_slate_day():
    # 8:30pm ET is 00:30 UTC the next day.
    assert eastern_slate_date("2026-09-01T00:30:00Z") == "2026-08-31"
    assert on_eastern_slate("2026-09-01T00:30:00Z", "2026-08-31")
    assert not on_eastern_slate("2026-09-01T00:30:00Z", "2026-09-01")


def test_date_only_is_not_shifted_back():
    assert eastern_slate_date("2026-08-27") == "2026-08-27"


def test_naive_espn_timestamp_is_treated_as_utc():
    assert eastern_slate_date("2026-09-28T02:15:00") == "2026-09-27"


def test_unparsed_start_stays_on_the_page_that_listed_it():
    assert keep_event_on_slate("", "2026-09-27", "2026-09-27")
    assert not keep_event_on_slate("", "2026-09-27", "2026-09-28")
    assert keep_event_on_slate("2026-09-28T02:15:00Z", "2026-09-27", "2026-09-28")
