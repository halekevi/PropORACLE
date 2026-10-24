"""Play-side cushion: Unders flip the gap, and a push is not L5=5."""
from utils.cushion_z import (
    assign_tercile,
    clear_count,
    current_game_map,
    cushion_z,
    l5_clears,
    median_gap,
    rank_tiebreak,
    row_excluded,
    screen_status,
    tercile_edges,
)

GAMES = [26, 25, 24, 23, 22, 21, 20, 19, 18, 17]


def test_over_cushion_is_positive_when_the_average_clears_the_line():
    z = cushion_z(GAMES, 14.5, "OVER")
    assert z is not None and z > 0


def test_under_cushion_flips_the_same_window():
    over = cushion_z(GAMES, 30, "OVER")
    under = cushion_z(GAMES, 30, "UNDER")
    assert over is not None and under is not None
    assert under == -over
    assert under > 0


def test_l5_over_needs_all_five_above_the_line():
    assert l5_clears([22, 23, 24, 25, 26], 21.5, "OVER")
    assert not l5_clears([22, 23, 24, 25, 16], 21.5, "OVER")
    assert not l5_clears([21.5, 22, 23, 24, 25], 21.5, "OVER")


def test_l5_under_needs_all_five_below_the_line():
    assert l5_clears([10, 11, 12, 13, 14], 14.5, "UNDER")
    assert not l5_clears([10, 11, 12, 13, 16], 14.5, "UNDER")


def test_zero_spread_has_no_cushion():
    assert cushion_z([5, 5, 5, 5, 5, 5, 5, 5], 3, "OVER") is None


def test_median_and_clear_count_follow_the_side():
    assert median_gap(GAMES, 20, "OVER") == 1.5
    assert median_gap(GAMES, 20, "UNDER") == -1.5
    assert clear_count(GAMES, 20, "OVER", 1) == 6
    assert clear_count(GAMES, 20, "UNDER", 1) == 3


def test_terciles_use_the_train_edges():
    edges = tercile_edges(list(range(9)))
    assert edges is not None
    assert assign_tercile(edges[0] - 0.01, edges) == "low"
    assert assign_tercile(edges[1], edges) == "high"


def test_current_season_games_do_not_pad():
    mapped = current_game_map([12, 9, None])
    assert mapped["current_stat_g1"] == 12
    assert mapped["current_stat_g3"] == ""
    assert mapped["current_stat_g10"] == ""


def test_rank_tiebreak_orders_without_dropping():
    assert rank_tiebreak({"cushion_z": 1.4}) < rank_tiebreak({"cushion_z": 0.2})
    assert rank_tiebreak({"cushion_z": 0.2}) < rank_tiebreak({})
    under = {"pick_type": "Standard", "side": "UNDER", "cushion_z": 4.0}
    assert rank_tiebreak(under) == rank_tiebreak({})


def test_screen_stays_open_under_200_holdout_legs():
    assert screen_status(
        holdout_n=61,
        top_n=61,
        rising=False,
        wilson_lo=0.6,
        breakeven=0.845,
        beats_shuffle=False,
        ceiling=False,
    ) == "OPEN"
    assert row_excluded({"minutes_status": "dnp"}) == "dnp"
    assert row_excluded({"retirement_reading": "walkover_chart_silent"}) == "retirement"
    assert row_excluded({"result": "HIT"}) == ""


def test_last10_includes_the_previous_season():
    from scripts.cushion_log_window import last10

    series = {
        ("MLB", "kyle isbel", "hits"): [
            ("2025-09-28", 1, 1.0),
            ("2025-09-29", 1, 2.0),
            ("2026-03-31", 1, 1.0),
            ("2026-04-01", 1, 1.0),
            ("2026-04-02", 1, 1.0),
            ("2026-04-03", 1, 1.0),
            ("2026-04-04", 1, 1.0),
            ("2026-04-05", 1, 1.0),
            ("2026-04-06", 1, 1.0),
            ("2026-04-07", 1, 0.0),
            ("2026-04-08", 1, 3.0),
        ]
    }
    window = last10(series, "MLB", "kyle isbel", "hits", "2026-04-08")
    assert window[0] == 0.0
    assert 2.0 in window
    assert 3.0 not in window
    assert len(window) == 10

