"""Attach guard: no L5 unless OK; no zero-fill; scale / DNP / all-zero."""
from __future__ import annotations

from utils.stat_attach_guard import (
    attach_status_blocks_l5,
    decide_attach,
    implausible_l5_vs_line,
    l5_hits_without_games,
    mapping_failure_all_zero,
    row_qualifies_for_list,
    void_dnp_zeros,
)


def test_status_blocks():
    assert attach_status_blocks_l5("UNSUPPORTED_PROP")
    assert attach_status_blocks_l5("INSUFFICIENT_GAMES")
    assert attach_status_blocks_l5("MAPPING_FAILURE")
    assert attach_status_blocks_l5("NO_DATA")
    assert not attach_status_blocks_l5("OK")
    assert not attach_status_blocks_l5("PRIOR_SEASON_FILL")
    assert not attach_status_blocks_l5("")


def test_ypc_and_longest_scale():
    assert implausible_l5_vs_line("rush_yards_per_carry", 82.0, 3.7) == "implausible_ypc_scale"
    assert implausible_l5_vs_line("longest_reception", 4.6, 25.5) == "implausible_longest_scale"
    assert implausible_l5_vs_line("completion_percentage", 0.0, 69.0) == "implausible_comp_pct_scale"
    assert implausible_l5_vs_line("receiving_yards", 52.4, 19.5) == ""
    assert implausible_l5_vs_line("rushing_yards", 58.8, 9.5) == ""


def test_period_props_unsupported():
    vals, status = decide_attach([82, 70, 60, 55, 40], "rush_yards_in_first_5_attempts", 18.5)
    assert vals is None and status == "UNSUPPORTED_PROP"
    vals, status = decide_attach([70, 60], "yards_on_first_reception", 11.5)
    assert vals is None and status == "UNSUPPORTED_PROP"
    vals, status = decide_attach([250, 220], "quarters_with_25_pass_yards", 1.5)
    assert vals is None and status == "UNSUPPORTED_PROP"


def test_all_zero_volume_is_mapping_failure():
    assert mapping_failure_all_zero([0, 0, 0, 0, 0], "sacks")
    assert mapping_failure_all_zero([0, 0, 0, 0, 0], "completion_percentage")
    vals, status = decide_attach([0, 0, 0, 0, 0], "sacks", 0.5, role="DEF")
    assert vals is None and status == "MAPPING_FAILURE"


def test_dnp_zero_voided_real_zero_kept():
    kept = void_dnp_zeros(
        [0, 3, 0],
        [
            {"minutes": 0},
            {"minutes": 90},
            {"minutes": 90},
        ],
    )
    assert kept == [3.0, 0.0]


def test_insufficient_and_ok():
    vals, status = decide_attach([12], "receiving_yards", 19.5)
    assert vals is None and status == "INSUFFICIENT_GAMES"
    vals, status = decide_attach([43, 67, 56, 43, 53], "receiving_yards", 19.5)
    assert status == "OK" and vals is not None and len(vals) == 5


def test_l5_hits_without_games():
    assert l5_hits_without_games({"l5_over": 5, "stat_last5_avg": "", "games_used": ""})
    assert not l5_hits_without_games({"l5_over": 5, "stat_last5_avg": 22.2, "games_used": 8})


def test_list_gate():
    ok, why = row_qualifies_for_list(
        {
            "stat_status": "OK",
            "prop": "receiving_yards",
            "line": 19.5,
            "stat_last5_avg": 52.4,
            "l5_over": 5,
            "games_used": 8,
            "stat_g1": 43,
            "stat_g2": 67,
            "stat_g3": 56,
            "stat_g4": 43,
            "stat_g5": 53,
        }
    )
    assert ok, why
    bad, why = row_qualifies_for_list(
        {"stat_status": "UNSUPPORTED_PROP", "prop": "yards_on_first_reception", "line": 11.5, "l5_over": 5}
    )
    assert not bad and why == "stat_status_not_ok"
