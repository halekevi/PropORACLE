from datetime import date

from utils.sport_season_windows import nba_stats_season, prior_nba_stats_season


def test_nba_stats_season_october_rolls_forward():
    assert nba_stats_season("2026-10-23") == "2026-27"
    assert nba_stats_season("2026-09-30") == "2025-26"
    assert nba_stats_season("2026-01-15") == "2025-26"
    assert nba_stats_season(date(2025, 10, 21)) == "2025-26"


def test_prior_nba_stats_season():
    assert prior_nba_stats_season("2026-27") == "2025-26"
    assert prior_nba_stats_season("2025-26") == "2024-25"
