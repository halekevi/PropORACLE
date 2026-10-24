"""Soccer keep props (list + Goblin-70): Shots / SOT / Saves per-prop gates."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from rank_best_props_today import _clears_list_gate  # noqa: E402
from utils.soccer_keep_gates import (  # noqa: E402
    soccer_keep_eligible,
    soccer_keep_prop,
    soccer_ticket_gate_passes,
)
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402


def _soc(**kwargs) -> dict:
    row = {
        "sport": "SOCCER",
        "player": "Test",
        "prop": "Shots",
        "side": "OVER",
        "line": 1.5,
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "cover": 1.2,
        "def": "Weak",
        "shot_volume": "HIGH_VOL",
        "starter_tier": "STARTER",
    }
    row.update(kwargs)
    return row


def test_shots_need_l5eq5_and_l10():
    assert soccer_keep_eligible(_soc())
    assert goblin_70_eligible(_soc())
    assert _clears_list_gate(_soc())
    assert not soccer_keep_eligible(_soc(l5_over=4))
    assert not soccer_keep_eligible(_soc(l10_over=7))
    assert not soccer_keep_eligible(_soc(prop="Goals", l5_over=5, l10_over=10))


def test_sot_needs_l5_and_off():
    sot = _soc(prop="Shots On Target", l5_over=4, l10_over=5)
    assert soccer_keep_eligible(sot)
    # L5>=4 is list/pool only — tickets need L5=5.
    assert not soccer_ticket_gate_passes(sot)
    assert not goblin_70_eligible(sot)
    sot5 = dict(sot, l5_over=5)
    assert soccer_ticket_gate_passes(sot5)
    assert goblin_70_eligible(sot5)
    assert not soccer_keep_eligible(dict(sot, shot_volume="", starter_tier="", team_top3_rank=9))
    assert soccer_keep_eligible(dict(sot, shot_volume="HIGH_VOL", starter_tier=""))
    assert not soccer_keep_eligible(dict(sot, l5_over=3))


def test_saves_list_without_defense_and_never_ticket():
    sv = _soc(
        prop="Goalie Saves",
        l5_over=4,
        l10_over=5,
        own_def_tier="Weak",
        opp_off_tier="Elite",
    )
    sv.pop("shot_volume", None)
    sv.pop("def", None)
    assert soccer_keep_eligible(sv)
    assert not soccer_ticket_gate_passes(sv)
    assert not goblin_70_eligible(sv)
    # Own-defense and opponent attack do not filter the list.
    assert soccer_keep_eligible(dict(sv, own_def_tier="Elite"))
    assert soccer_keep_eligible(dict(sv, own_def_tier=""))
    assert soccer_keep_eligible(dict(sv, opp_off_tier="Weak"))
    assert not soccer_keep_eligible(dict(sv, l5_over=3))
    # L5=5 still does not ticket.
    sv5 = dict(sv, l5_over=5)
    assert not soccer_ticket_gate_passes(sv5)
    assert not goblin_70_eligible(sv5)


def test_standard_shots_same_gate():
    std = _soc(pick_type="Standard", l5_over=5, l10_over=8)
    assert soccer_keep_eligible(std)
    assert standard_ticket_eligible(std)
    assert not soccer_keep_eligible(dict(std, l5_over=4))


def test_named_match_off_the_covered_board_is_ineligible(monkeypatch):
    monkeypatch.setattr(
        "utils.soccer_keep_gates.game_on_covered_board",
        lambda row, fetch=None: False,
    )
    cup = _soc(
        team="NORWICH",
        opp_team="MAN CITY",
        matchup="NORWICH vs MAN CITY (SOCCER)",
        start_time="2026-09-17T14:30:00-04:00",
    )
    assert not soccer_keep_eligible(cup)
    assert not soccer_ticket_gate_passes(dict(cup, l5_over=5))


def test_championship_pair_matches_that_scoreboard_event():
    from utils.soccer_board_coverage import event_matches_sides

    event = {
        "date": "2026-09-20T14:00:00Z",
        "competitions": [
            {
                "competitors": [
                    {"team": {"displayName": "Norwich City", "abbreviation": "NOR"}},
                    {"team": {"displayName": "Bolton Wanderers", "abbreviation": "BOL"}},
                ]
            }
        ],
    }
    assert event_matches_sides("NORWICH", "BOLTON", event)
    assert not event_matches_sides("NORWICH", "MAN CITY", event)


def test_goals_and_volume_faded():
    assert not soccer_keep_prop(_soc(prop="Goals"))
    assert not soccer_keep_prop(_soc(prop="Assists"))
    assert not soccer_keep_eligible(_soc(prop="Goal + Assist", l5_over=5, l10_over=10))
    assert not soccer_keep_eligible(_soc(pick_type="Demon"))
    assert not soccer_keep_eligible(_soc(pick_type="Goblin", side="UNDER"))
