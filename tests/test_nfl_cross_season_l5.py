"""NFL L5 lookback: current RS → preseason → prior RS → prior preseason."""
from __future__ import annotations

from datetime import date

from utils.nfl_cross_season_l5 import (
    assemble_nfl_l5_window,
    classify_nfl_l5_source,
    format_l5_source_counts,
    nfl_cross_season_l5_active,
    source_counts,
)
from utils.nfl_keep_gates import nfl_current_season_l5_ok

SLATE = date(2026, 9, 20)


def _g(day: str) -> dict:
    return {"game_date": day}


def test_two_current_pulls_three_prior():
    games = [
        _g("20260914"),
        _g("20260907"),
        _g("20260112"),
        _g("20251228"),
        _g("20251221"),
        _g("20251214"),
        _g("20251207"),
    ]
    window = assemble_nfl_l5_window(games, SLATE, n=5)
    assert len(window) == 5
    counts = source_counts(window)
    assert counts["current_season"] == 2
    assert counts["prior_season"] == 3
    assert counts["preseason_current"] == 0
    assert counts["preseason_prior"] == 0
    assert format_l5_source_counts(counts) == "current_season=2, prior_season=3"
    assert [r["l5_source"] for r in window] == [
        "current_season",
        "current_season",
        "prior_season",
        "prior_season",
        "prior_season",
    ]


def test_five_current_does_not_pull_prior():
    games = [
        _g("20260918"),
        _g("20260914"),
        _g("20260910"),
        _g("20260907"),
        _g("20260904"),
        _g("20260112"),
        _g("20251228"),
        _g("20250816"),
    ]
    window = assemble_nfl_l5_window(games, SLATE, n=5)
    assert len(window) == 5
    counts = source_counts(window)
    assert counts["current_season"] == 5
    assert counts["prior_season"] == 0
    assert counts["preseason_current"] == 0
    assert counts["preseason_prior"] == 0
    assert all(r["l5_source"] == "current_season" for r in window)


def test_zero_rows_stays_empty():
    window = assemble_nfl_l5_window([], SLATE, n=5)
    assert window == []
    assert source_counts(window) == {
        "current_season": 0,
        "preseason_current": 0,
        "prior_season": 0,
        "preseason_prior": 0,
    }


def test_preseason_fills_before_prior_regular_season():
    games = [
        _g("20260914"),
        _g("20260907"),
        _g("20260823"),
        _g("20260816"),
        _g("20260809"),
        _g("20251228"),
        _g("20250816"),
    ]
    window = assemble_nfl_l5_window(games, SLATE, n=5)
    counts = source_counts(window)
    assert counts["current_season"] == 2
    assert counts["preseason_current"] == 3
    assert counts["prior_season"] == 0
    assert counts["preseason_prior"] == 0


def test_classify_buckets():
    assert classify_nfl_l5_source(date(2026, 9, 7), SLATE) == "current_season"
    assert classify_nfl_l5_source(date(2026, 8, 16), SLATE) == "preseason_current"
    assert classify_nfl_l5_source(date(2025, 12, 28), SLATE) == "prior_season"
    assert classify_nfl_l5_source(date(2026, 1, 12), SLATE) == "prior_season"
    assert classify_nfl_l5_source(date(2025, 8, 16), SLATE) == "preseason_prior"
    assert classify_nfl_l5_source(date(2024, 12, 1), SLATE) is None


def test_weeks_1_4_active_week_5_off():
    assert nfl_cross_season_l5_active(date(2026, 9, 5))
    assert nfl_cross_season_l5_active(date(2026, 9, 20))
    assert not nfl_cross_season_l5_active(date(2026, 8, 15))
    # Week 1 Thu 2026-09-10 + 24d = 2026-10-04 (Week 4 Sunday).
    assert nfl_cross_season_l5_active(date(2026, 10, 4))
    assert not nfl_cross_season_l5_active(date(2026, 10, 5))


def test_gate_accepts_assembled_window():
    assert (
        nfl_current_season_l5_ok(
            {
                "l5_window_n": 5,
                "l5_sample_n": 2,
                "season_l5_flag": "CROSS_SEASON_L5",
                "l5_over": 4,
            }
        )
        is True
    )


def test_gate_rejects_old_prior_fill_without_window_n():
    assert (
        nfl_current_season_l5_ok(
            {"l5_sample_n": 1, "season_l5_flag": "PRIOR_SEASON_FILL", "l5_over": 5}
        )
        is False
    )


def test_gate_respects_env_off(monkeypatch):
    monkeypatch.setenv("PROPORACLE_ALLOW_CROSS_SEASON_L5", "0")
    assert (
        nfl_current_season_l5_ok(
            {
                "l5_window_n": 5,
                "l5_sample_n": 2,
                "season_l5_flag": "CROSS_SEASON_L5",
            }
        )
        is False
    )
