"""Smoke tests: basketball minutes_tier from real MPG (NBA bands)."""

from __future__ import annotations

import pandas as pd

from utils.basketball_minutes_tier import (
    assign_minutes_tier,
    resolve_mpg_series,
    tier_minutes_from_mpg,
)


def test_nba_band_thresholds():
    assert tier_minutes_from_mpg(20) == "LOW"
    assert tier_minutes_from_mpg(20.0) == "LOW"
    assert tier_minutes_from_mpg(20.1) == "MEDIUM"
    assert tier_minutes_from_mpg(30) == "MEDIUM"
    assert tier_minutes_from_mpg(30.1) == "HIGH"
    assert tier_minutes_from_mpg(35) == "HIGH"
    assert tier_minutes_from_mpg(float("nan")) == "UNKNOWN"
    assert tier_minutes_from_mpg(None) == "UNKNOWN"


def test_prefer_min_last5_over_prop_avg():
    df = pd.DataFrame(
        {
            "min_last5_avg": [32.0, None, None],
            "min_per_game": [None, 18.0, None],
            "min_season_avg": [None, None, 25.0],
            "stat_last5_avg": [1.2, 1.2, 1.2],  # steals — must not drive tier
        }
    )
    mpg = resolve_mpg_series(df)
    assert list(mpg) == [32.0, 18.0, 25.0]
    out = assign_minutes_tier(df)
    assert list(out["minutes_tier"]) == ["HIGH", "LOW", "MEDIUM"]


def test_does_not_use_stat_last5_as_minutes():
    df = pd.DataFrame({"stat_last5_avg": [28.0, 5.0]})
    out = assign_minutes_tier(df)
    assert list(out["minutes_tier"]) == ["UNKNOWN", "UNKNOWN"]
