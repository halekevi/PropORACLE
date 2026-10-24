"""NBA board prop map + category reconciliation."""
from __future__ import annotations

import pandas as pd
import pytest

from utils.pp_board_prop_map import (
    is_ou_ranker_prop,
    lookup_category,
    required_keys,
)
from utils.pp_category_reconcile import (
    assert_board_coverage,
    missing_required_fetched,
    reconcile_categories,
)


def test_card_label_is_source_of_truth_for_fga():
    cat = lookup_category("NBA", "FG Attempted")
    assert cat is not None
    assert cat.key == "fga"
    assert cat.card_label == "FG Attempted"
    assert "fga" in cat.box_cols


def test_hyphen_pra_chip_maps_to_same_key_as_card():
    a = lookup_category("NBA", "Pts-Rebs-Asts")
    b = lookup_category("NBA", "Pts+Rebs+Asts")
    assert a is not None and b is not None
    assert a.key == b.key == "pra"


def test_specials_and_combo_players_off_ou_ranker():
    assert is_ou_ranker_prop("NBA", "Double-Double") is False
    assert is_ou_ranker_prop("NBA", "Triple-Double") is False
    assert is_ou_ranker_prop("NBA", "Points (Combo)") is False
    assert is_ou_ranker_prop("NBA", "FG Attempted") is True
    assert is_ou_ranker_prop("NBA", "Points") is True


def test_reconcile_fails_when_required_tab_missing():
    # Thick Points board without FGA — the 2026-10-20 morning bug.
    board = pd.DataFrame(
        {
            "prop_type": ["Points"] * 25
            + ["Rebounds"] * 10
            + ["Assists"] * 10
            + ["Pts+Rebs"] * 10
        }
    )
    rows = reconcile_categories(
        sport="NBA",
        board_df=board,
        fetched_df=board,
        kept_df=board,
    )
    miss = missing_required_fetched(rows, sport="NBA", min_points=20)
    assert "fga" in miss
    with pytest.raises(RuntimeError, match="FG Attempted|fga|coverage miss"):
        assert_board_coverage(rows, sport="NBA", min_points=20)


def test_reconcile_passes_when_fga_present():
    labels = (
        ["Points"] * 25
        + ["FG Attempted"] * 5
        + ["FG Made"] * 5
        + ["3-PT Made"] * 5
        + ["3-PT Attempted"] * 5
        + ["Free Throws Made"] * 5
        + ["Free Throws Attempted"] * 5
        + ["Two Pointers Made"] * 5
        + ["Two Pointers Attempted"] * 5
        + ["Rebounds"] * 5
        + ["Assists"] * 5
        + ["Pts+Rebs"] * 5
        + ["Pts+Asts"] * 5
        + ["Pts+Rebs+Asts"] * 5
        + ["Rebs+Asts"] * 5
        + ["Blks+Stls"] * 5
        + ["Steals"] * 5
        + ["Blocked Shots"] * 5
        + ["Turnovers"] * 5
        + ["Offensive Rebounds"] * 5
        + ["Defensive Rebounds"] * 5
        + ["Personal Fouls"] * 5
    )
    board = pd.DataFrame({"prop_type": labels})
    rows = reconcile_categories(
        sport="NBA",
        board_df=board,
        fetched_df=board,
        kept_df=board,
    )
    assert missing_required_fetched(rows, sport="NBA", min_points=20) == []
    assert_board_coverage(rows, sport="NBA", min_points=20)


def test_required_keys_include_fga():
    assert "fga" in required_keys("NBA")
