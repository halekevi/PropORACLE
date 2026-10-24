"""Unit tests for NFL actuals zero-fill (returners / active roster ghosts)."""
from __future__ import annotations

from scripts.fetch_football_actuals import _is_return_cat, _parse_box
from scripts.player_name_norm import fold_player_name


def test_fold_strips_apostrophe():
    assert fold_player_name("Lil'Jordan Humphrey") == "liljordan humphrey"
    assert fold_player_name("Marvin Mims Jr.") == "marvin mims"


def test_is_return_cat():
    assert _is_return_cat("puntReturns")
    assert _is_return_cat("kick returns")
    assert not _is_return_cat("receiving")


def test_returner_only_zero_fills_receiving():
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"abbreviation": "DEN"},
                    "statistics": [
                        {
                            "name": "puntReturns",
                            "labels": ["NO", "YDS", "TD"],
                            "athletes": [
                                {
                                    "athlete": {"displayName": "Marvin Mims Jr."},
                                    "stats": ["2", "18", "0"],
                                }
                            ],
                        }
                    ],
                }
            ]
        }
    }
    rows = _parse_box(summary, season=None)
    by_prop = {
        r["prop_type"]: r["actual"]
        for r in rows
        if r["player"] == "Marvin Mims Jr."
    }
    assert by_prop["player_touchdowns"] == 0.0
    assert by_prop["receiving_yards"] == 0.0
    assert by_prop["receptions"] == 0.0
    assert by_prop["longest_reception"] == 0.0
    assert by_prop["rushing_yards"] == 0.0
    assert by_prop["fantasy_score"] == 0.0


def test_fumble_only_zero_fills_defense():
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"abbreviation": "SF"},
                    "statistics": [
                        {
                            "name": "fumbles",
                            "labels": ["FUM", "LOST", "REC"],
                            "athletes": [
                                {
                                    "athlete": {"displayName": "Keion White"},
                                    "stats": ["0", "0", "1"],
                                }
                            ],
                        }
                    ],
                }
            ]
        }
    }
    rows = _parse_box(summary, season=None)
    by_prop = {
        r["prop_type"]: r["actual"]
        for r in rows
        if r["player"] == "Keion White"
    }
    assert by_prop["sacks"] == 0.0
    assert by_prop["tackles_assists"] == 0.0


def test_zero_fg_does_not_emit_shortest_longest():
    """No makes → shortest/longest undefined (void), not actual=0 MISS."""
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"abbreviation": "LAR"},
                    "statistics": [
                        {
                            "name": "kicking",
                            "labels": ["FG", "XP", "PTS", "LONG"],
                            "athletes": [
                                {
                                    "athlete": {"displayName": "Harrison Mevis"},
                                    "stats": ["0", "1", "1", "--"],
                                }
                            ],
                        }
                    ],
                }
            ]
        }
    }
    rows = _parse_box(summary, season=None)
    by_prop = {
        r["prop_type"]: r["actual"]
        for r in rows
        if r["player"] == "Harrison Mevis"
    }
    assert by_prop["fg_made"] == 0.0
    assert by_prop["field_goal_yards"] == 0.0
    assert "shortest_fg_made" not in by_prop
    assert "longest_fg_made" not in by_prop
