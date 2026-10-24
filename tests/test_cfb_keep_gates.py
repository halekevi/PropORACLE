"""CFB tape-confirmed keep/fade: tickets vs list, Rec U vs Rush U."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from rank_best_props_today import _clears_list_gate  # noqa: E402
from utils.cfb_keep_gates import (  # noqa: E402
    cfb_l8_bonus,
    cfb_list_eligible,
    cfb_pass_yards_goblin_skip_d,
    cfb_ticket_fade,
)
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402


def _row(**kwargs) -> dict:
    row = {
        "sport": "CFB",
        "player": "Test",
        "prop": "Pass Yards",
        "side": "OVER",
        "line": 225.5,
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "l5_under": 5,
        "l10_under": 8,
        "cover": 18.0,
        "dist_l5": 18.0,
        "def": "Weak",
        "season_type": 2,
    }
    row.update(kwargs)
    return row


def test_pass_yards_goblin_over_drops_d():
    ok = _row()
    d_fail = _row(**{"def": "Avg"})
    d_elite = _row(**{"def": "Elite"})
    assert cfb_pass_yards_goblin_skip_d(ok)
    assert goblin_70_eligible(ok)
    assert goblin_70_eligible(d_fail)
    assert goblin_70_eligible(d_elite)
    assert not goblin_70_eligible(_row(l5_over=4, **{"def": "Avg"}))
    # L10 is not a CFB ticket block. A short L10 still clears on face L5=5.
    assert goblin_70_eligible(_row(l10_over=7, **{"def": "Avg"}))
    assert goblin_70_eligible(_row(l10_over=3, **{"def": "Avg"}))
    assert not goblin_70_eligible(_row(cover=5.0, dist_l5=5.0, **{"def": "Avg"}))
    rush = _row(prop="Rush Yards", cover=12.0, dist_l5=12.0, **{"def": "Avg"})
    assert not cfb_pass_yards_goblin_skip_d(rush)
    assert not goblin_70_eligible(rush)
    assert goblin_70_eligible(_row(prop="Rush Yards", cover=12.0, dist_l5=12.0))


def test_player_td_fades_from_ticket_stack_not_list():
    gob = _row(prop="Player Touchdowns", line=0.5, cover=0.6, dist_l5=0.6)
    std = _row(prop="Player Touchdowns", pick_type="Standard", line=0.5, cover=0.6, dist_l5=0.6)
    assert cfb_ticket_fade(gob)
    assert cfb_ticket_fade(std)
    assert not goblin_70_eligible(gob)
    assert not standard_ticket_eligible(std)
    assert cfb_list_eligible(gob)
    assert _clears_list_gate(gob)
    assert _clears_list_gate(_row(prop="Player Touchdowns", pick_type="Goblin", l5_over=4))


def test_standard_rec_yards_under_blocked_from_cover_stack():
    rec_u = _row(
        prop="Receiving Yards",
        pick_type="Standard",
        side="UNDER",
        cover=-10.0,
        dist_l5=-10.0,
        **{"def": "Elite"},
    )
    rec_o = _row(prop="Receiving Yards", pick_type="Standard", side="OVER", cover=10.0, dist_l5=10.0)
    assert cfb_ticket_fade(rec_u)
    assert not standard_ticket_eligible(rec_u)
    assert not cfb_ticket_fade(rec_o)
    assert standard_ticket_eligible(rec_o)
    assert _clears_list_gate(rec_u)


def test_standard_rush_yards_under_keeps_full_stack():
    rush_u = _row(
        prop="Rush Yards",
        pick_type="Standard",
        side="UNDER",
        cover=-10.0,
        dist_l5=-10.0,
        **{"def": "Elite"},
    )
    assert not cfb_ticket_fade(rush_u)
    assert standard_ticket_eligible(rush_u)
    assert not standard_ticket_eligible({**rush_u, "def": "Weak"})
    assert standard_ticket_eligible({**rush_u, "l10_under": 4})
    assert not standard_ticket_eligible({**rush_u, "l5_under": 4})


def test_l8_is_bonus_not_a_gate():
    short = _row(l8_over=5, l8_window_n=8)
    full = _row(l8_over=6, l8_window_n=8)
    thin_window = _row(l8_over=6, l8_window_n=5)
    under = _row(
        prop="Rush Yards",
        pick_type="Standard",
        side="UNDER",
        cover=-10.0,
        dist_l5=-10.0,
        l8_under=6,
        l8_window_n=8,
        **{"def": "Elite"},
    )
    assert not cfb_l8_bonus(short)
    assert cfb_l8_bonus(full)
    assert not cfb_l8_bonus(thin_window)
    assert cfb_l8_bonus(under)
    assert goblin_70_eligible(short)
    assert goblin_70_eligible(full)


def test_rush_att_and_kick_pts_goblin_over_off_list():
    for prop in ("Rush Attempts", "Kicking Points"):
        gob = _row(prop=prop, cover=2.0, dist_l5=2.0)
        std = _row(prop=prop, pick_type="Standard", cover=2.0, dist_l5=2.0)
        assert not cfb_list_eligible(gob)
        assert not _clears_list_gate(gob)
        assert cfb_ticket_fade(gob)
        assert not goblin_70_eligible(gob)
        assert cfb_list_eligible(std)
        assert _clears_list_gate(std)
