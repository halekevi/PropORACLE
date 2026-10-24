"""Ticket grading treats board labels and fetch labels as one stat."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))

import build_ticket_eval as te  # noqa: E402


def test_total_games_shares_a_key_with_match_total_games():
    board = set(te._prop_index_keys("Total Games"))
    fetch = set(te._prop_index_keys("match_total_games"))
    assert "totalgames" in board
    assert "matchtotalgames" in board
    assert board == fetch


def test_games_won_goalie_saves_and_rush_yards_alias():
    assert set(te._prop_index_keys("Total Games Won")) == set(te._prop_index_keys("games_won"))
    assert set(te._prop_index_keys("Goalie Saves")) == set(te._prop_index_keys("Goalkeeper Saves"))
    assert set(te._prop_index_keys("Rush Yards")) == set(te._prop_index_keys("rushing_yards"))


def test_full_name_token_set_matches_reversed_order_only():
    assert te._norm_player_name("Qinwen Zheng") == te._norm_player_name("Zheng Qinwen")
    assert te._norm_player_name("Kevin Viveros") != te._norm_player_name("Emiliano Viveros")


def test_sog_alias_is_soccer_only():
    nhl = set(te._prop_index_keys("Shots On Goal", sport="NHL"))
    assert "shotsontarget" not in nhl
    assert "sot" not in nhl
    assert "shotsongoal" in nhl
    soccer = set(te._prop_index_keys("Shots On Target", sport="Soccer"))
    assert "sog" in soccer
    assert "shotsongoal" in soccer


def test_shots_do_not_alias_to_shots_on_target():
    shots = set(te._prop_index_keys("Shots"))
    sot = set(te._prop_index_keys("Shots On Target"))
    assert "shots" in shots
    assert "sog" in sot or "shotsontarget" in sot
    assert shots.isdisjoint(sot)


def test_alias_index_finds_fetch_prop_name():
    triple: dict = {}
    pairs: dict = {}
    te._ingest_workbook_rows_into_index(
        [
            {
                "player": "Arthur Gea",
                "prop_type": "match_total_games",
                "actual": 16,
                "direction": "OVER",
            }
        ],
        triple,
        pairs,
    )
    row = te._match_leg_in_index(
        {
            "player": "Arthur Gea",
            "prop_type": "Total Games",
            "direction": "OVER",
            "line": 20.5,
        },
        triple,
        pairs,
    )
    assert row is not None
    assert float(row["actual"]) == 16.0
    assert te._leg_grade(row["actual"], 20.5, "OVER", "", "") == "MISS"
