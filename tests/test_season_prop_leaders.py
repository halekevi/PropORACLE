"""Season leader / consistency stamps."""
from __future__ import annotations

from utils.season_prop_leaders import (
    PlayerSeasonStat,
    _consistency,
    annotate_rows,
    leader_flag,
)


def test_leader_flag_bands():
    assert leader_flag(1) == "LEADER"
    assert leader_flag(3) == "LEADER"
    assert leader_flag(5) == "TOP5"
    assert leader_flag(10) == "TOP10"
    assert leader_flag(11) == ""
    assert leader_flag(None) == ""


def test_consistency_over_under():
    stat = PlayerSeasonStat(
        player_fold="x",
        player="X",
        n=5,
        avg=50.0,
        values=(40.0, 60.0, 55.0, 30.0, 70.0),
        dates=("2026-01-01",) * 5,
    )
    h, n, rate = _consistency(stat, line=50.0, side="OVER")
    assert (h, n) == (3, 5) and rate == 0.6
    h, n, rate = _consistency(stat, line=50.0, side="UNDER")
    assert (h, n) == (2, 5) and rate == 0.4


def test_annotate_skips_unknown_sport():
    rows = [{"sport": "GOLF", "player": "A", "prop": "Birdies", "line": 3.5, "side": "OVER"}]
    assert annotate_rows(rows, as_of="2026-11-21") == 0
