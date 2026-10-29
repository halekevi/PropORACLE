"""PrizePicks NBA Fantasy Score weights from the official scoring chart."""
from utils.nba_fantasy_score import (
    NBA_FANTASY_WEIGHTS,
    keep_nba_fantasy_sport,
    nba_fantasy_score,
)


def test_chart_weights_match_prizepicks_playbook():
    assert NBA_FANTASY_WEIGHTS == {
        "pts": 1.0,
        "reb": 1.2,
        "ast": 1.5,
        "stl": 3.0,
        "blk": 3.0,
        "tov": -1.0,
    }


def test_formula_no_threes_or_double_double_bonus():
    # 22 pts, 8 reb, 5 ast, 1 stl, 0 blk, 3 tov, 4 threes, a double-double.
    # Chart has no 3PM or DD bonus — 22 + 9.6 + 7.5 + 3 + 0 - 3 = 39.1
    got = nba_fantasy_score(22, 8, 5, 1, 0, 3)
    assert got == 39.1
    # Same box plus a made 3 and a 10th rebound (still a DD) must not move.
    assert nba_fantasy_score(22, 10, 5, 1, 0, 3) == 22 + 12.0 + 7.5 + 3.0 + 0.0 - 3.0


def test_missing_piece_is_none():
    assert nba_fantasy_score(10, 5, 3, 1, 1, None) is None


def test_keep_nba_family_only():
    assert keep_nba_fantasy_sport("NBA")
    assert keep_nba_fantasy_sport("nba1q")
    assert not keep_nba_fantasy_sport("WNBA")
    assert not keep_nba_fantasy_sport("NFL")
