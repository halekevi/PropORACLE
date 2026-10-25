"""DNP-zero voids only with positive evidence; NFL UNDER zeros need snaps."""
from __future__ import annotations

from utils.dnp_zero_void import (
    FACEOFF_PCT_MISMATCH,
    NFL_UNDER_ZERO_NO_SNAPS,
    SOCCER_UNDER_ZERO_NO_MINUTES,
    is_dnp_zero_actual,
    void_if_dnp_zero,
    void_if_nfl_under_zero_no_snaps,
    void_if_nhl_faceoff_pct_vs_count,
    void_if_soccer_under_zero_no_minutes,
    would_old_unknown_pt_void,
)


def test_zero_with_zero_minutes_is_dnp():
    assert is_dnp_zero_actual(0.0, {"minutes": 0, "prop": "points"})
    assert void_if_dnp_zero(0.0, {"minutes": 0}) == ("VOID", "DNP_ZERO")


def test_zero_sacks_with_snaps_is_not_dnp():
    assert not is_dnp_zero_actual(0.0, {"snaps": 42, "prop": "sacks"})


def test_unknown_playing_time_stays_graded():
    # Missing minutes — do not invent VOID from actual alone (non-NFL-under).
    assert not is_dnp_zero_actual(0.0, {"prop": "shots_on_goal"})
    assert not is_dnp_zero_actual(0.0, {"prop": "receiving_yards"})
    assert void_if_dnp_zero(0.0, {"prop": "points"}) is None


def test_explicit_inactive_flag_voids():
    assert is_dnp_zero_actual(0.0, {"inactive": True, "prop": "points"})
    assert is_dnp_zero_actual(0.0, {"not_in_box": True, "prop": "shots"})


def test_old_unknown_pt_rule_counted_separately():
    assert would_old_unknown_pt_void(0.0, {"prop": "shots_on_goal"})
    assert would_old_unknown_pt_void(0.0, {"prop": "receiving_yards"})
    assert not would_old_unknown_pt_void(0.0, {"prop": "sacks"})
    # Known PT → old invent-void path did not apply.
    assert not would_old_unknown_pt_void(0.0, {"minutes": 16, "prop": "shots"})
    # True DNP with minutes==0 is not an \"old unknown\" void.
    assert not would_old_unknown_pt_void(0.0, {"minutes": 0, "prop": "points"})


def test_nfl_under_zero_without_snaps_voids():
    row = {
        "sport": "NFL",
        "bet_direction": "UNDER",
        "prop": "receiving_yards",
    }
    assert void_if_nfl_under_zero_no_snaps(0.0, row) == (
        "VOID",
        NFL_UNDER_ZERO_NO_SNAPS,
    )


def test_nfl_under_zero_with_snaps_stands():
    row = {
        "sport": "NFL",
        "bet_direction": "UNDER",
        "prop": "receiving_yards",
        "snaps": 18,
    }
    assert void_if_nfl_under_zero_no_snaps(0.0, row) is None


def test_nfl_under_zero_with_played_box_evidence_stands():
    row = {
        "sport": "NFL",
        "bet_direction": "UNDER",
        "prop": "rushing_yards",
        "played_box_evidence": True,
    }
    assert void_if_nfl_under_zero_no_snaps(0.0, row) is None


def test_nfl_over_zero_unknown_pt_not_forced_void():
    # OVER misses with actual=0 must stay graded when PT is unknown.
    row = {"sport": "NFL", "bet_direction": "OVER", "prop": "receiving_yards"}
    assert void_if_nfl_under_zero_no_snaps(0.0, row) is None
    assert void_if_dnp_zero(0.0, row) is None


def test_nhl_faceoff_pct_vs_count_voids():
    row = {"prop": "Faceoffs Won", "line": 5.5, "sport": "NHL"}
    assert void_if_nhl_faceoff_pct_vs_count(44.4, row) == (
        "VOID",
        FACEOFF_PCT_MISMATCH,
    )
    # Real FOW count vs count line stands.
    assert void_if_nhl_faceoff_pct_vs_count(6.0, row) is None


def test_soccer_under_zero_without_minutes_voids():
    row = {"sport": "SOCCER", "bet_direction": "UNDER", "prop": "shots"}
    assert void_if_soccer_under_zero_no_minutes(0.0, row) == (
        "VOID",
        SOCCER_UNDER_ZERO_NO_MINUTES,
    )


def test_soccer_under_zero_with_minutes_stands():
    row = {
        "sport": "SOCCER",
        "bet_direction": "UNDER",
        "prop": "shots",
        "minutes_played": 84,
    }
    assert void_if_soccer_under_zero_no_minutes(0.0, row) is None


def test_soccer_under_zero_minutes_played_zero_is_dnp():
    row = {
        "sport": "SOCCER",
        "bet_direction": "UNDER",
        "prop": "shots",
        "minutes_played": 0,
    }
    assert void_if_dnp_zero(0.0, row) == ("VOID", "DNP_ZERO")
    assert void_if_soccer_under_zero_no_minutes(0.0, row) == ("VOID", "DNP_ZERO")


def test_soccer_over_zero_unknown_minutes_stays_graded():
    row = {"sport": "SOCCER", "bet_direction": "OVER", "prop": "shots"}
    assert void_if_soccer_under_zero_no_minutes(0.0, row) is None
    assert void_if_dnp_zero(0.0, row) is None
