"""Fail-closed real scraped quote rule for promotion / P&L shadow."""
from utils.comparison_tracks import TRACK_MIXED, summarize_tracks, track_log_row
from utils.scraped_quote import (
    SCRAPED_PAYOUT_SOURCES,
    count_real_quotes,
    format_real_quote_coverage,
    is_real_scraped_quote,
    log_real_quote_coverage,
    real_scraped_payout_x,
)

_TS = "2026-10-22T21:52:19-04:00"


def test_is_real_scraped_quote_matrix_locked():
    """Only live_cdp / n_correct_live with a timestamp are real quotes."""
    assert SCRAPED_PAYOUT_SOURCES == frozenset({"live_cdp", "n_correct_live"})

    # Pass: the two scraped sources with a capture stamp.
    assert is_real_scraped_quote("live_cdp", _TS) is True
    assert is_real_scraped_quote("n_correct_live", _TS) is True
    assert is_real_scraped_quote("LIVE_CDP", _TS) is True  # case-insensitive

    # Fail: pin / estimate sources even with a timestamp.
    for src in (
        "mix_floor",
        "goblin_3",
        "n_correct_median",
        "n_correct_p25",
        "payout_rate_card",
        "pending_live",
        "rate_card",
        "",
        None,
    ):
        assert is_real_scraped_quote(src, _TS) is False, src

    # Fail: scraped source labels without a real capture.
    for cap in (None, "", "none", "null", "  "):
        assert is_real_scraped_quote("live_cdp", cap) is False, cap
        assert is_real_scraped_quote("n_correct_live", cap) is False, cap

    # Fail: blank source + null capture (track-log shape).
    assert is_real_scraped_quote("", None) is False
    assert is_real_scraped_quote(None, None) is False


def test_real_scraped_payout_x_requires_both_fields():
    assert (
        real_scraped_payout_x(
            {
                "payout_source": "n_correct_live",
                "captured_at": None,
                "scraped_payout": 2.0,
            }
        )
        is None
    )
    assert (
        real_scraped_payout_x(
            {
                "payout_source": "mix_floor",
                "captured_at": _TS,
                "scraped_payout": 2.0,
            }
        )
        is None
    )
    assert (
        real_scraped_payout_x(
            {
                "payout_source": "live_cdp",
                "captured_at": _TS,
                "scraped_payout": 1.6,
            }
        )
        == 1.6
    )


def test_count_and_format_real_quote_coverage():
    rows = [
        {"payout_source": "live_cdp", "captured_at": _TS},
        {"payout_source": "n_correct_live", "captured_at": None},
        {"payout_source": "mix_floor", "captured_at": _TS},
        {"payout_source": "", "captured_at": None},
        {"payout": {"payout_source": "n_correct_live", "captured_at": _TS}},
    ]
    real_n, total_n = count_real_quotes(rows)
    assert (real_n, total_n) == (2, 5)
    assert (
        format_real_quote_coverage(real_n, total_n, label="gated")
        == "gated: real_quote 2 of 5 slips (3 excluded)"
    )
    lines: list[str] = []
    log_real_quote_coverage(rows, label="run", print_fn=lines.append)
    assert lines == ["run: real_quote 2 of 5 slips (3 excluded)"]


def test_summarize_tracks_ignores_unverified_quotes():
    rows = [
        {
            "track": "gated_goblin",
            "result": "win",
            "p_win": 0.49,
            "payout_source": "live_cdp",
            "captured_at": "2026-10-01T12:00:00Z",
            "scraped_payout": 1.7,
            "stake": 20,
            "standard_sports": [],
        },
        {
            "track": "gated_goblin",
            "result": "win",
            "p_win": 0.49,
            "payout_source": "n_correct_live",
            "captured_at": None,
            "scraped_payout": 2.0,
            "stake": 20,
            "standard_sports": [],
        },
        {
            "track": "gated_goblin",
            "result": "loss",
            "p_win": 0.49,
            "payout_source": "",
            "captured_at": None,
            "scraped_payout": 1.7,
            "stake": 20,
            "standard_sports": [],
        },
    ]
    summary = summarize_tracks(rows)
    assert summary[0]["graded"] == 3
    assert summary[0]["real_quote_n"] == 1
    assert summary[0]["scraped_n"] == 1


def test_track_log_omits_scraped_without_capture():
    logged = track_log_row(
        {
            "id": "M2-1",
            "track": TRACK_MIXED,
            "payout_source": "n_correct_live",
            "captured_at": None,
            "all_hit_x": 2.0,
            "scraped_payout": 2.0,
            "stake": 20,
            "legs": [
                {"pick_type": "Goblin", "sport": "Soccer"},
                {"pick_type": "Standard", "sport": "NFL"},
            ],
        },
        date="2026-10-23",
    )
    assert logged["real_quote"] is False
    assert logged["scraped_payout"] is None
