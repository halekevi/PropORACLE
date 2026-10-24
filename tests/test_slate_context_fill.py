"""Fill CV% and min-tier labels when step8 left them blank."""

from __future__ import annotations

import pandas as pd

from utils.slate_context_fill import (
    fill_cv_pct_if_missing,
    fill_min_tier_labels,
    flip_tennis_total_games_all_under,
    overlay_live_step1_board,
    strip_pitcher_ks_hitter_defense,
    summarize_board_context_fill,
    tennis_total_games_over_blocked_by_l5,
)


def test_fill_min_tier_from_codes():
    df = pd.DataFrame({"minutes_tier": [0, 2, "HIGH"]})
    out = fill_min_tier_labels(df)
    assert list(out["min_tier"]) == ["LOW", "HIGH", "HIGH"]


def test_cv_pct_fills_only_when_missing():
    df = pd.DataFrame(
        {
            "cv_pct": [12.0, pd.NA],
            "stat_g1": [2.0, 4.0],
            "stat_g2": [2.0, 6.0],
            "stat_g3": [2.0, 8.0],
        }
    )
    out = fill_cv_pct_if_missing(df, min_games=3)
    assert float(out["cv_pct"].iloc[0]) == 12.0
    assert float(out["cv_pct"].iloc[1]) > 0


def test_summarize_board_context_fill_counts():
    df = pd.DataFrame(
        {
            "l5_over": [4, pd.NA],
            "cv_pct": [10.0, pd.NA],
            "min_tier": ["HIGH", pd.NA],
            "stat_g1": [1.0, 2.0],
            "stat_g2": [1.0, 2.0],
            "stat_g3": [1.0, 2.0],
        }
    )
    s = summarize_board_context_fill(df)
    assert s["rows"] == 2
    assert s["l5"] == 1
    assert s["cv"] == 1
    assert s["min_tier"] == 1
    assert s["g3"] == 2


def test_strip_pitcher_ks_defense_rank():
    df = pd.DataFrame(
        {
            "player": ["Tanner Scott", "A Hitter"],
            "prop_type": ["Strikeouts", "Hits"],
            "player_type": ["pitcher", "hitter"],
            "opponent_def_rank": [28, 28],
            "def_tier": ["WEAK", "WEAK"],
        }
    )
    out = strip_pitcher_ks_hitter_defense(df)
    assert pd.isna(out["opponent_def_rank"].iloc[0]) or str(out["opponent_def_rank"].iloc[0]) == "<NA>"
    assert float(out["opponent_def_rank"].iloc[1]) == 28


def test_overlay_live_goblin_line_and_pick_type():
    step8 = pd.DataFrame(
        {
            "player": ["Elena Rybakina"],
            "prop_type": ["Aces"],
            "pick_type": ["Standard"],
            "line": [6.5],
            "direction": ["UNDER"],
        }
    )
    step1 = pd.DataFrame(
        {
            "player": ["Elena Rybakina"],
            "prop_type": ["Aces"],
            "pick_type": ["Goblin"],
            "line": [4.5],
        }
    )
    out = overlay_live_step1_board(step8, step1)
    assert float(out["line"].iloc[0]) == 4.5
    assert out["pick_type"].iloc[0] == "Goblin"
    assert out["direction"].iloc[0] == "OVER"


def test_overlay_live_demon_replaces_standard_under():
    step8 = pd.DataFrame(
        {
            "player": ["Esmerlyn Valdez"],
            "prop_type": ["Walks"],
            "pick_type": ["Standard"],
            "line": [0.5],
            "direction": ["UNDER"],
        }
    )
    step1 = pd.DataFrame(
        {
            "player": ["Esmerlyn Valdez"],
            "prop_type": ["Walks"],
            "pick_type": ["Demon"],
            "line": [0.5],
        }
    )
    out = overlay_live_step1_board(step8, step1)
    assert out["pick_type"].iloc[0] == "Demon"
    assert out["direction"].iloc[0] == "OVER"


def test_tennis_total_games_all_under_not_over():
    row = {
        "prop_type": "Total Games",
        "direction": "OVER",
        "l5_over": 0,
        "l5_under": 5,
    }
    assert tennis_total_games_over_blocked_by_l5(row)
    df = pd.DataFrame([row])
    out = flip_tennis_total_games_all_under(df)
    assert out["direction"].iloc[0] == "UNDER"
