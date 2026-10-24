"""NFL/CFB period + first-N props are unsupported for boxscore L5."""
from __future__ import annotations

import pandas as pd

from utils.nfl_unsupported_props import (
    PERIOD_SPLIT_PROP_NORMS,
    is_football_period_split_prop,
    mark_football_unsupported_props,
    norm_football_prop,
)


def test_board_labels_flagged():
    labels = [
        "Quarters with 5 Rush Yards",
        "Halves with 100 Pass Yards",
        "Halves with 25 Receiving Yards",
        "Halves with 25 Rush Yards",
        "Quarters with 25 Pass Yards",
        "Quarters with 1 Reception",
        "Yards on First Rush Attempt",
        "Yards on First Reception",
        "Rush Yards in First 5 Attempts",
        "Receiving Yards in First 2 Receptions",
        "Completions in First 10 Pass Attempts",
        "Quarters with 10 Rush Yards",
    ]
    for lab in labels:
        assert is_football_period_split_prop(lab), lab


def test_core_props_not_flagged():
    for lab in (
        "Passing Yards",
        "Rushing Yards",
        "Receiving Yards",
        "Receptions",
        "Sacks",
        "Pass Attempts",
        "Rush Attempts",
        "Kicking Points",
        "Player Touchdowns",
    ):
        assert not is_football_period_split_prop(lab), lab


def test_explicit_norms_covered():
    for n in PERIOD_SPLIT_PROP_NORMS:
        assert is_football_period_split_prop(n)


def test_prop_value_would_have_fuzzy_matched():
    """Document the fabrication hole this marker closes.

    yards_on_first_rush_attempt contains rush+attempt → old prop_value
    returned RUSH_ATT. quarters_with_5_rush_yards contains rush+yard →
    RUSH_YDS. Those must stay unsupported.
    """
    assert "rush" in norm_football_prop("yards_on_first_rush_attempt")
    assert "attempt" in norm_football_prop("yards_on_first_rush_attempt")
    assert is_football_period_split_prop("yards_on_first_rush_attempt")
    assert is_football_period_split_prop("quarters_with_5_rush_yards")
    assert is_football_period_split_prop("halves_with_100_pass_yards")


def test_mark_dataframe():
    df = pd.DataFrame(
        {
            "prop_type_normalized": [
                "passing_yards",
                "yards_on_first_rush_attempt",
                "halves_with_25_receiving_yards",
                "sacks",
            ]
        }
    )
    out = mark_football_unsupported_props(df)
    assert list(out["unsupported_prop"]) == [0, 1, 1, 0]
    assert out.loc[1, "unsupported_reason"] == "period_or_first_n_prop_no_box_l5"
