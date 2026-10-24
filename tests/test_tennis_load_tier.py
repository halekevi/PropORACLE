"""Tests for tennis match-load / fatigue → minutes_tier."""

from utils.tennis_load_tier import (
    extract_last_match_load,
    load_tier_from_features,
    parse_slate_date,
)


def test_parse_slate_date_iso():
    assert parse_slate_date("2026-08-22T19:00:00.000-04:00").isoformat() == "2026-08-22"


def test_long_match_short_rest_is_low():
    assert (
        load_tier_from_features(last_minutes=180.0, days_since=1.0) == "LOW"
    )


def test_short_match_long_rest_is_high():
    assert (
        load_tier_from_features(last_minutes=75.0, days_since=4.0) == "HIGH"
    )


def test_missing_is_unknown():
    assert load_tier_from_features() == "UNKNOWN"


def test_games_proxy_when_no_minutes():
    # 40 games * 3.5 ≈ 140 → fatigued band without rest → LOW
    assert load_tier_from_features(last_match_games=40.0) == "LOW"


def test_extract_last_match_load():
    hist = [
        {"date": "20260820", "minutes": 165, "match_total_games": 38, "total_sets": 3},
    ]
    feat = extract_last_match_load(hist, slate=parse_slate_date("2026-08-21"))
    assert feat["minutes_tier"] == "LOW"
    assert feat["last_match_minutes"] == 165
    assert feat["days_rest"] == 1.0
