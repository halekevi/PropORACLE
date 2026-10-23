"""Fail-closed real scraped quote rule for promotion / P&L shadow."""
from utils.comparison_tracks import TRACK_MIXED, summarize_tracks, track_log_row
from utils.scraped_quote import is_real_scraped_quote, real_scraped_payout_x


def test_is_real_scraped_quote_fails_closed():
    assert is_real_scraped_quote("live_cdp", "2026-10-22T21:52:19-04:00")
    assert is_real_scraped_quote("n_correct_live", "2026-09-09T22:05:48")
    assert not is_real_scraped_quote("n_correct_live", None)
    assert not is_real_scraped_quote("mix_floor", "2026-10-22T21:52:19-04:00")
    assert not is_real_scraped_quote("", "2026-10-22T21:52:19-04:00")
    assert real_scraped_payout_x(
        {"payout_source": "n_correct_live", "captured_at": None, "scraped_payout": 2.0}
    ) is None


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
