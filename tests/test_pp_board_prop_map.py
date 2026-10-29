"""NBA board prop map + category reconciliation."""
from __future__ import annotations

import pandas as pd
import pytest

from utils.pp_board_prop_map import (
    categories_for_sport,
    is_ou_ranker_prop,
    lookup_category,
    required_keys,
)
from utils.prop_norm import canon_prop
from utils.pp_category_reconcile import (
    assert_board_coverage,
    missing_required_fetched,
    reconcile_categories,
)


# Board tabs from the live NBA chip row + card-face shorts on the player tiles.
_NBA_LIVE_CHIPS_AND_FACES = (
    "Points",
    "Pts",
    "Rebounds",
    "Rebs",
    "Assists",
    "Asts",
    "Pts+Rebs+Asts",
    "Fantasy Score",
    "3-PT Made",
    "3PM",
    "FG Made",
    "FG Attempted",
    "FG Att",
    "3-PT Attempted",
    "3-PT Att",
    "Rebs+Asts",
    "Pts+Rebs",
    "Two Pointers Made",
    "2-PT Made",
    "Pts+Asts",
    "Two Pointers Attempted",
    "2-PT Att",
    "Free Throws Made",
    "FT Made",
    "Free Throws Attempted",
    "FT Att",
    "Defensive Rebounds",
    "Def Reb",
    "Offensive Rebounds",
    "Off Reb",
    "Triple-Double",
    "Dunks",
    "Blks+Stls",
    "Blocked Shots",
    "Blks",
    "Double-Double",
    "Steals",
    "Stls",
    "3-PT Made (Combo)",
    "Points (Combo)",
    "Rebounds (Combo)",
    "Assists (Combo)",
    "Quarters with 3+ Points",
    "Quarters with 5+ Points",
    "Qtrs with 3+ Pts",
    "Qtrs with 5+ Pts",
)


def test_every_live_nba_chip_and_card_face_maps():
    keys = {c.key for c in categories_for_sport("NBA")}
    for label in _NBA_LIVE_CHIPS_AND_FACES:
        cat = lookup_category("NBA", label)
        assert cat is not None, f"unmapped card/tab: {label!r}"
        assert canon_prop("NBA", label) == cat.key, (label, canon_prop("NBA", label), cat.key)
        assert cat.key in keys


def test_card_face_2pt_att_maps_to_two_pointers_attempted():
    cat = lookup_category("NBA", "2-PT Att")
    assert cat is not None
    assert cat.key == "fg2a"
    assert cat.card_label == "Two Pointers Attempted"


def test_card_label_is_source_of_truth_for_fga():
    cat = lookup_category("NBA", "FG Attempted")
    assert cat is not None
    assert cat.key == "fga"
    assert cat.card_label == "FG Attempted"
    assert "fga" in cat.box_cols


def test_quarters_plus_points_chips_map():
    q3 = lookup_category("NBA", "Quarters with 3+ Points")
    q5 = lookup_category("NBA", "Quarters with 5+ Points")
    assert q3 is not None and q3.key == "quarters_3plus"
    assert q5 is not None and q5.key == "quarters_5plus"


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
