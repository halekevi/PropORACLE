"""Board team vs last-box team helpers."""
from __future__ import annotations

from utils.roster_change import norm_nba_team


def test_bk_alias_matches_bkn():
    assert norm_nba_team("BK") == "BKN"
    assert norm_nba_team("BKN") == "BKN"


def test_board_loads_garland_lac():
    from utils.nba_board_team import load_board_player_teams

    board = load_board_player_teams("2026-10-21")
    assert board["darius garland"]["team"] == "LAC"
    assert board["isaiah jackson"]["team"] == "LAC"
    assert board["darius garland"]["opp"] == "SAC"
