"""NFL Standard UNDER-heavy ticket gate + Week 1 priority order."""

from utils.nfl_keep_gates import (
    NFL_GOBLIN_TICKETS_ENABLED,
    nfl_goblin_is_extreme_low_line,
    nfl_goblin_ticket_eligible,
    nfl_standard_ticket_eligible,
    nfl_ticket_priority,
)
from utils.ticket_70_pool import goblin_70_eligible, standard_sort_key, standard_ticket_eligible


def _row(**kwargs):
    base = {
        "sport": "NFL",
        "pick_type": "Standard",
        "league": "NFL",
        "l5_over": 5,
        "l5_under": 5,
        "l10_over": 9,
        "l10_under": 9,
        "def_tier": "Elite",
        "cover": -10,
        "dist_l5": -10,
    }
    base.update(kwargs)
    return base


def test_pass_yards_gated_out_both_sides():
    assert nfl_standard_ticket_eligible(_row(side="UNDER", prop="passing_yards", def_tier="Elite")) is False
    assert nfl_standard_ticket_eligible(_row(side="OVER", prop="passing_yards", def_tier="Weak", cover=20, dist_l5=20)) is False
    assert standard_ticket_eligible(_row(side="UNDER", prop="passing_yards")) is False


def test_priority_order_rec_before_sacks_before_rush():
    rec = _row(side="UNDER", prop="receiving_yards")
    long = _row(side="UNDER", prop="longest_reception")
    sack = _row(side="UNDER", prop="sacks")
    rush_u = _row(side="UNDER", prop="rushing_yards")
    rush_o = _row(side="OVER", prop="rushing_yards", def_tier="Weak", cover=20, dist_l5=20)
    assert nfl_ticket_priority(rec) < nfl_ticket_priority(long) < nfl_ticket_priority(sack)
    assert nfl_ticket_priority(sack) < nfl_ticket_priority(rush_u) < nfl_ticket_priority(rush_o)
    assert standard_sort_key(rec) < standard_sort_key(sack) < standard_sort_key(rush_o)


def test_rush_yards_bidirectional_keep():
    assert nfl_standard_ticket_eligible(_row(side="UNDER", prop="rushing_yards")) is True
    assert nfl_standard_ticket_eligible(
        _row(side="OVER", prop="rushing_yards", def_tier="Weak", cover=20, dist_l5=20)
    ) is True


def test_rec_under_keep_over_fade():
    assert nfl_standard_ticket_eligible(_row(side="UNDER", prop="receiving_yards")) is True
    assert nfl_standard_ticket_eligible(
        _row(side="OVER", prop="receiving_yards", def_tier="Weak", cover=12, dist_l5=12)
    ) is False


def test_nfl_goblin_unissued_until_flag():
    assert NFL_GOBLIN_TICKETS_ENABLED is False
    gob = _row(
        pick_type="Goblin",
        side="OVER",
        prop="receiving_yards",
        def_tier="Weak",
        cover=12,
        dist_l5=12,
    )
    assert nfl_goblin_ticket_eligible(gob) is False
    assert goblin_70_eligible(gob) is False


def test_extreme_low_goblin_line_helper():
    assert nfl_goblin_is_extreme_low_line(
        _row(pick_type="Goblin", side="OVER", prop="sacks", line=0.5)
    )
    assert not nfl_goblin_is_extreme_low_line(
        _row(pick_type="Goblin", side="OVER", prop="receiving_yards", line=12.5)
    )
