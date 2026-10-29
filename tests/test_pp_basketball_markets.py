"""Pregame basketball markets (FG/FT/2PT) coverage helpers."""
from __future__ import annotations

import pytest

from utils.pp_basketball_markets import (
    hoops_sport_for_league,
    log_shooting_coverage,
    missing_core_shooting,
    shooting_family_counts,
)


def test_counts_board_labels():
    props = [
        "Points",
        "FG Attempted",
        "FG Made",
        "3-PT Made",
        "3-PT Attempted",
        "Free Throws Made",
        "Free Throws Attempted",
        "Two Pointers Made",
        "Two Pointers Attempted",
        "2-PT Att",
        "2pts Made",
        "2pts Attempted",
    ]
    c = shooting_family_counts(props)
    assert c["fg_attempted"] == 1
    assert c["fg_made"] == 1
    assert c["threes_made"] == 1
    assert c["threes_attempted"] == 1
    assert c["ft_made"] == 1
    assert c["ft_attempted"] == 1
    assert c["two_made"] == 2
    assert c["two_attempted"] == 3
    assert missing_core_shooting(c) == []


def test_warns_when_points_without_fga(capsys):
    props = ["Points"] * 25 + ["Rebounds"] * 10
    with pytest.raises(RuntimeError, match="missing"):
        log_shooting_coverage(props, sport="WNBA", n_points=25)
    out = capsys.readouterr().out
    assert "missing" in out.lower()
    assert "in_game=false" in out


def test_period_league_ids_are_not_full_game():
    assert hoops_sport_for_league("84") == "NBA1H"
    assert hoops_sport_for_league("192") == "NBA1Q"
    log_shooting_coverage(["Points"] * 25, sport="NBA1Q", n_points=25)


def test_nbap_league_id_maps_to_nba_shooting_family():
    assert hoops_sport_for_league("237") == "NBA"
    assert hoops_sport_for_league("7") == "NBA"
    assert hoops_sport_for_league("3") == "WNBA"


def test_period_board_does_not_warn(capsys):
    props = ["Points"] * 25
    log_shooting_coverage(props, sport="WNBA1H", n_points=25)
    out = capsys.readouterr().out
    assert "[WARN]" not in out
