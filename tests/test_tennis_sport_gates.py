"""Tennis sport-gate unit tests: Ace/DF ban, totals-only, L5 floors."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from combined_slate_tickets import (  # noqa: E402
    TENNIS_LEG_MIN_HIT_RATE,
    tennis_allowed_leg,
)


def _leg(**kwargs):
    base = {
        "sport": "TENNIS",
        "pick_type": "Goblin",
        "direction": "OVER",
        "prop_type": "Total Games",
        "line": 21.5,
        "standard_line": 21.5,
        "l5_over": 4,
        "l5_under": 1,
        "l10_over": 8,
        "tier": "B",
    }
    base.update(kwargs)
    return base


def _games_won(**kwargs):
    base = {
        "prop_type": "Total Games Won",
        "line": 7.5,
        "standard_line": 12.5,
    }
    base.update(kwargs)
    return _leg(**base)


def test_tennis_ace_goblin_hard_banned():
    assert not tennis_allowed_leg(_leg(prop_type="Aces", l5_over=5))


def test_tennis_double_faults_goblin_hard_banned():
    assert not tennis_allowed_leg(_leg(prop_type="Double Faults", l5_over=5))


def test_tennis_standard_under_aces_df_faded():
    """Serve UNDER keep removed after missing→0 inflation was backfilled away."""
    assert not tennis_allowed_leg(
        _leg(pick_type="Standard", direction="UNDER", prop_type="Aces")
    )
    assert not tennis_allowed_leg(
        _leg(pick_type="Standard", direction="UNDER", prop_type="Double Faults")
    )
    assert not tennis_allowed_leg(
        _leg(pick_type="Standard", direction="OVER", prop_type="Aces")
    )


def test_tennis_goblin_totals_requires_l5():
    assert not tennis_allowed_leg(_leg(prop_type="Total Games", l5_over=2))
    assert not tennis_allowed_leg(_leg(prop_type="Total Games", l5_over=4, l10_over=7))
    assert tennis_allowed_leg(_leg(prop_type="Total Games", l5_over=4))
    assert tennis_allowed_leg(_games_won(l5_over=2, l10_over=3))
    assert not tennis_allowed_leg(_games_won(standard_line=9.5))  # only 2 under Standard


def test_tennis_goblin_games_won_drops_top10_opponent():
    assert not tennis_allowed_leg(_games_won(l5_over=5, opponent_rank=8))
    assert tennis_allowed_leg(_games_won(l5_over=5, opponent_rank=18))
    assert tennis_allowed_leg(_games_won(l5_over=5, opponent_rank=70))


def test_tennis_goblin_total_games_drops_tight_11_25():
    assert not tennis_allowed_leg(
        _leg(prop_type="Total Games", l5_over=5, opponent_rank=18)
    )
    assert tennis_allowed_leg(
        _leg(prop_type="Total Games", l5_over=5, opponent_rank=8)
    )
    assert tennis_allowed_leg(
        _leg(prop_type="Total Games", l5_over=5, opponent_rank=70)
    )


def test_tennis_standard_over_games_won_drops_opp_25():
    assert not tennis_allowed_leg(
        _leg(
            pick_type="Standard",
            direction="OVER",
            prop_type="Total Games Won",
            l5_over=4,
            opponent_rank=20,
            tier="B",
        )
    )


def test_tennis_standard_over_totals_off():
    assert not tennis_allowed_leg(
        _leg(
            pick_type="Standard",
            direction="OVER",
            prop_type="Total Games Won",
            l5_over=4,
            tier="B",
        )
    )


def test_tennis_standard_under_totals_off():
    assert not tennis_allowed_leg(
        _leg(
            pick_type="Standard",
            direction="UNDER",
            prop_type="Total Games",
            l5_over=1,
            l5_under=4,
            l10_under=8,
            tier="B",
        )
    )


def test_tennis_total_games_over_blocked_when_l5_all_under():
    assert not tennis_allowed_leg(
        _leg(
            pick_type="Standard",
            direction="OVER",
            prop_type="Total Games",
            l5_over=0,
            l5_under=5,
            opponent_rank=70,
            tier="B",
        )
    )


def test_tennis_leg_min_hit_rate_raised():
    assert TENNIS_LEG_MIN_HIT_RATE[2] >= 0.68
    assert TENNIS_LEG_MIN_HIT_RATE[3] >= 0.72
