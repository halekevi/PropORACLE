"""BO3 PrizePicks lines must not average slam / best-of-5 match totals."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_TENNIS_SCRIPTS = _REPO / "Sports" / "Tennis" / "scripts"
sys.path.insert(0, str(_TENNIS_SCRIPTS))

from tennis_shared import (  # noqa: E402
    apply_format_matched_stat_g,
    build_sackmann_player_log,
    collect_history_values,
    line_expects_best_of_three,
    match_is_best_of_five,
    match_is_incomplete,
    match_is_low_level,
    normalize_espn_athlete_id,
    tennis_bo5_graded_on_bo3_tape,
)


def _safiullin_espn_hist() -> list[dict]:
    # Mixed L5 that produced the fake +21 Total Games edge: 40, 27, 47, 54, 55.
    return [
        {"opponent": "Djokovic", "match_total_games": 40, "games_won": 13, "total_sets": 5, "sets_won": 1},
        {"opponent": "Fonseca", "match_total_games": 27, "games_won": 16, "total_sets": 3, "sets_won": 2},
        {"opponent": "Alcaraz", "match_total_games": 47, "games_won": 19, "total_sets": 5, "sets_won": 2},
        {"opponent": "Sinner", "match_total_games": 54, "games_won": 22, "total_sets": 5, "sets_won": 2},
        {"opponent": "Medvedev", "match_total_games": 55, "games_won": 24, "total_sets": 5, "sets_won": 2},
        {"opponent": "Norrie", "match_total_games": 25, "games_won": 15, "total_sets": 2, "sets_won": 2},
        {"opponent": "Draper", "match_total_games": 22, "games_won": 13, "total_sets": 2, "sets_won": 2},
        {"opponent": "Shelton", "match_total_games": 38, "games_won": 20, "total_sets": 3, "sets_won": 2},
        {"opponent": "Fritz", "match_total_games": 36, "games_won": 19, "total_sets": 3, "sets_won": 1},
    ]


def test_bo3_line_caps_and_slam_detection():
    assert line_expects_best_of_three("match_total_games", 23.5)
    assert line_expects_best_of_three("match_total_games", 28.5)
    assert not line_expects_best_of_three("match_total_games", 32.5)
    assert line_expects_best_of_three("games_won", 12.5)
    assert line_expects_best_of_three("games_won", 18.5)
    assert not line_expects_best_of_three("games_won", 19.5)
    assert not line_expects_best_of_three("games_won", 21.5)
    assert match_is_best_of_five({"total_sets": 5, "match_total_games": 40, "games_won": 13})
    assert not match_is_best_of_five({"total_sets": 3, "match_total_games": 27, "games_won": 16})


def test_safiullin_total_games_skips_wimbledon_bo5():
    vals = collect_history_values(_safiullin_espn_hist(), "match_total_games", 5, line=23.5)
    assert vals == [27.0, 25.0, 22.0, 38.0, 36.0]
    assert sum(vals) / 5 == 29.6
    mixed = [40.0, 27.0, 47.0, 54.0, 55.0]
    assert sum(mixed) / 5 == 44.6
    assert abs((sum(vals) / 5) - 23.5) < 10
    assert (sum(mixed) / 5) - 23.5 > 20


def test_faa_games_won_drops_slam_24_plus():
    hist = [
        {"games_won": 13, "match_total_games": 21, "total_sets": 2},
        {"games_won": 28, "match_total_games": 57, "total_sets": 5},
        {"games_won": 31, "match_total_games": 55, "total_sets": 5},
        {"games_won": 19, "match_total_games": 28, "total_sets": 3},
        {"games_won": 20, "match_total_games": 34, "total_sets": 3},
        {"games_won": 18, "match_total_games": 26, "total_sets": 2},
        {"games_won": 15, "match_total_games": 31, "total_sets": 3},
    ]
    vals = collect_history_values(hist, "games_won", 5, line=13.5)
    assert vals == [13.0, 19.0, 20.0, 18.0, 15.0]
    assert sum(vals) / 5 == 17.0


def test_bo5_posted_line_keeps_mixed_last5():
    """US Open Total Games 36.5 keeps slam + tour BO3 like PrizePicks last-5."""
    vals = collect_history_values(_safiullin_espn_hist(), "match_total_games", 5, line=36.5)
    assert vals == [40.0, 27.0, 47.0, 54.0, 55.0]


def test_navone_games_won_19_5_matches_prizepicks_l5():
    """PP Games Won 19.5 last-5 is mixed slam + BO3 (2 over / 3 under, avg 15.8)."""
    hist = [
        {"opponent": "Berrettini", "games_won": 22, "match_total_games": 45, "total_sets": 5},
        {"opponent": "Djokovic", "games_won": 28, "match_total_games": 50, "total_sets": 5},
        {"opponent": "Duckworth", "games_won": 9, "match_total_games": 20, "total_sets": 3},
        {"opponent": "Blockx", "games_won": 7, "match_total_games": 16, "total_sets": 2},
        {"opponent": "Collignon", "games_won": 13, "match_total_games": 24, "total_sets": 3},
    ]
    vals = collect_history_values(hist, "games_won", 5, line=19.5)
    assert vals == [22.0, 28.0, 9.0, 7.0, 13.0]
    assert sum(v > 19.5 for v in vals) == 2
    assert sum(v < 19.5 for v in vals) == 3
    assert abs(sum(vals) / 5 - 15.8) < 1e-9


def test_tiafoe_bo5_line_keeps_cincy_tape_but_list_veto():
    """US Open Total Games 40.5 last-5 can be Cincy BO3; do not Gold it."""
    hist = [
        {"match_total_games": 22, "total_sets": 2, "games_won": 13},
        {"match_total_games": 21, "total_sets": 2, "games_won": 12},
        {"match_total_games": 25, "total_sets": 3, "games_won": 15},
        {"match_total_games": 19, "total_sets": 2, "games_won": 12},
        {"match_total_games": 30, "total_sets": 3, "games_won": 18},
        {"match_total_games": 47, "total_sets": 5, "games_won": 25},
    ]
    vals = collect_history_values(hist, "match_total_games", 5, line=40.5)
    assert vals == [22.0, 21.0, 25.0, 19.0, 30.0]
    row = {
        "prop_type": "Total Games",
        "line": 40.5,
        "stat_g1": 22,
        "stat_g2": 21,
        "stat_g3": 25,
        "stat_g4": 19,
        "stat_g5": 30,
        "stat_last5_avg": 23.4,
    }
    assert tennis_bo5_graded_on_bo3_tape(row)
    assert not tennis_bo5_graded_on_bo3_tape(
        {
            "prop_type": "Total Games",
            "line": 40.5,
            "stat_g1": 47,
            "stat_g2": 42,
            "stat_g3": 38,
            "stat_g4": 51,
            "stat_g5": 36,
            "stat_last5_avg": 42.8,
        }
    )
    df = pd.DataFrame(
        [
            {
                "prop_norm": "match_total_games",
                "line": 40.5,
                "stat_g1": 22,
                "stat_g2": 21,
                "stat_g3": 25,
                "stat_g4": 19,
                "stat_g5": 30,
            }
        ]
    )
    changed = apply_format_matched_stat_g(df)
    assert changed == 0
    assert df.loc[0, "stat_g1"] == 22.0


def test_apply_format_matched_stat_g_strips_flattened_slam_totals():
    df = pd.DataFrame(
        [
            {
                "prop_norm": "match_total_games",
                "line": 23.5,
                "stat_g1": 40,
                "stat_g2": 27,
                "stat_g3": 47,
                "stat_g4": 54,
                "stat_g5": 55,
            }
        ]
    )
    changed = apply_format_matched_stat_g(df)
    assert changed == 1
    assert df.loc[0, "stat_g1"] == 27.0
    assert pd.isna(df.loc[0, "stat_g2"])
    assert abs(float(df.loc[0, "stat_last5_avg"]) - 27.0) < 1e-9
    edge = float(df.loc[0, "stat_last5_avg"]) - 23.5
    assert edge < 8


def test_sackmann_log_skips_five_set_scores_on_bo3_total_games():
    df = pd.DataFrame(
        [
            {
                "winner_name": "Ada Player",
                "loser_name": "Bea Rival",
                "tourney_date": "20260701",
                "match_num": 5,
                "score": "6-4 6-4 6-4 3-6 6-4",
                "w_ace": 8,
                "l_ace": 4,
                "w_df": 1,
                "l_df": 2,
            },
            {
                "winner_name": "Ada Player",
                "loser_name": "Cara Rival",
                "tourney_date": "20260620",
                "match_num": 3,
                "score": "6-4 6-3",
                "w_ace": 3,
                "l_ace": 1,
                "w_df": 1,
                "l_df": 1,
            },
            {
                "winner_name": "Ada Player",
                "loser_name": "Dee Rival",
                "tourney_date": "20260610",
                "match_num": 2,
                "score": "7-6(4) 6-4",
                "w_ace": 5,
                "l_ace": 2,
                "w_df": 2,
                "l_df": 1,
            },
        ]
    )
    mixed = build_sackmann_player_log(df, "ada player", "match_total_games", last_n=5)
    # Missing line defaults to BO3 so slam totals cannot leak onto summer boards.
    assert 49.0 not in mixed
    assert mixed == [19.0, 23.0]
    slam_line = build_sackmann_player_log(df, "ada player", "match_total_games", last_n=5, line=36.5)
    assert 49.0 in slam_line
    bo3 = build_sackmann_player_log(df, "ada player", "match_total_games", last_n=5, line=23.5)
    assert 49.0 not in bo3
    assert bo3 == [19.0, 23.0]


def test_stoiana_skips_qualifying_matches_pp_l5():
    """PrizePicks last-5 is main-draw. Qualifying made Stoiana O20.0 look 5/0 instead of 4/1."""
    hist = [
        {"match_total_games": 21.0, "round": "Round 1", "opponent": "Valentova"},
        {"match_total_games": 21.0, "round": "Qualifying 1st Round", "opponent": "Erjavec"},
        {"match_total_games": 28.0, "round": "Qualifying Final", "opponent": "Selekhmeteva"},
        {"match_total_games": 27.0, "round": "Qualifying Final", "opponent": "Korneeva"},
        {"match_total_games": 29.0, "round": "Round 1", "opponent": "Stearns"},
        {"match_total_games": 16.0, "round": "Round 1", "opponent": "Scott"},
        {"match_total_games": 26.0, "round": "Round 2", "opponent": "Shymanovich"},
        {"match_total_games": 29.0, "round": "Quarterfinal", "opponent": "Sawangkaew"},
    ]
    vals = collect_history_values(hist, "match_total_games", 5, line=20.0)
    assert vals == [21.0, 29.0, 16.0, 26.0, 29.0]
    assert sum(v > 20.0 for v in vals) == 4
    assert sum(v < 20.0 for v in vals) == 1
    mixed = collect_history_values(
        hist, "match_total_games", 5, line=20.0, skip_qualifying=False
    )
    assert mixed == [21.0, 21.0, 28.0, 27.0, 29.0]


def test_sackmann_q_rounds_skipped():
    hist = [
        {"match_total_games": 21.0, "round": "Q1"},
        {"match_total_games": 18.0, "round": "R32"},
        {"match_total_games": 22.0, "round": "Q2"},
        {"match_total_games": 25.0, "round": "R16"},
    ]
    vals = collect_history_values(hist, "match_total_games", 5, line=20.0)
    assert vals == [18.0, 25.0]


def test_grouping_slug_qualifying_is_skipped():
    hist = [
        {"match_total_games": 21.0, "grouping": "womens-singles-qualifying"},
        {"match_total_games": 18.0, "round": "R32"},
    ]
    vals = collect_history_values(hist, "match_total_games", 5, line=20.0)
    assert vals == [18.0]


def test_normalize_espn_athlete_id_strips_csv_float():
    assert normalize_espn_athlete_id("3562.0") == "3562"
    assert normalize_espn_athlete_id(3562.0) == "3562"
    assert normalize_espn_athlete_id("3562") == "3562"


def test_retirement_scores_skipped_from_l5():
    assert match_is_incomplete({"score": "5-2 RET"})
    hist = [
        {"match_total_games": 26.0, "score": "7-6 7-6", "round": "R32"},
        {"match_total_games": 7.0, "score": "5-2 RET", "round": "QF"},
        {"match_total_games": 18.0, "score": "6-4 6-2", "round": "R16"},
    ]
    vals = collect_history_values(hist, "match_total_games", 5, line=22.5)
    assert vals == [26.0, 18.0]


def test_itf_skipped_challenger_kept_on_l5():
    assert not match_is_low_level({"tourn": "Quebec City CH"})
    assert not match_is_low_level({"level": "C"})
    assert match_is_low_level({"tourn": "W100 Landisville PA"})
    hist = [
        {"games_won": 12.0, "tourn": "Quebec City CH", "round": "R16"},
        {"games_won": 8.0, "tourn": "Canada Masters", "round": "R128"},
        {"games_won": 18.0, "tourn": "W50 Hurghada", "round": "F"},
        {"games_won": 13.0, "tourn": "Wimbledon", "round": "R128"},
    ]
    vals = collect_history_values(hist, "games_won", 5, line=10.5)
    assert vals == [12.0, 8.0, 13.0]


def test_fearnley_total_games_challenger_is_3_of_5():
    """PP last-5 includes Quebec CH 17/17/22; skip Cincy Q1. Goblin 19.5 is 3/5."""
    hist = [
        {"match_total_games": 17.0, "tourn": "Quebec City CH", "round": "QF", "level": "C"},
        {"match_total_games": 17.0, "tourn": "Quebec City CH", "round": "R16", "level": "C"},
        {"match_total_games": 22.0, "tourn": "Quebec City CH", "round": "R32", "level": "C"},
        {"match_total_games": 26.0, "tourn": "Cincinnati Masters", "round": "Q1"},
        {"match_total_games": 27.0, "tourn": "Canada Masters", "round": "R64"},
        {"match_total_games": 23.0, "tourn": "Canada Masters", "round": "R128"},
    ]
    vals = collect_history_values(hist, "match_total_games", 5, line=19.5)
    assert vals == [17.0, 17.0, 22.0, 27.0, 23.0]
    assert sum(v > 19.5 for v in vals) == 3
    assert sum(v < 19.5 for v in vals) == 2
