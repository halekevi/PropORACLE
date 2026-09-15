"""Soccer keep props (list + Goblin-70): Shots / SOT / Saves per-prop gates."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from rank_best_props_today import _clears_list_gate  # noqa: E402
from utils.soccer_keep_gates import soccer_keep_eligible, soccer_keep_prop  # noqa: E402
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
    assert goblin_70_eligible(sot)
    assert not soccer_keep_eligible(dict(sot, shot_volume="", starter_tier="", team_top3_rank=9))
    assert soccer_keep_eligible(dict(sot, shot_volume="HIGH_VOL", starter_tier=""))
    assert not soccer_keep_eligible(dict(sot, l5_over=3))


def test_saves_need_l5_and_own_d():
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
    assert goblin_70_eligible(sv)
    # Own Elite defense fails OVER.
    assert not soccer_keep_eligible(dict(sv, own_def_tier="Elite"))
    assert not soccer_keep_eligible(dict(sv, l5_over=3, own_def_tier="Weak"))
    # Opp Weak attack fails OVER when OFF_TIER is present.
    assert not soccer_keep_eligible(dict(sv, opp_off_tier="Weak"))
    # Missing opp off still passes when own D is right (older boards).
    assert soccer_keep_eligible(dict(sv, opp_off_tier=""))
    # Fall back to opp def when own missing.
    assert soccer_keep_eligible(
        _soc(prop="Goalie Saves", l5_over=4, **{"def": "Weak", "own_def_tier": ""})
    )
    assert not soccer_keep_eligible(
        _soc(prop="Goalie Saves", l5_over=4, **{"def": "Elite", "own_def_tier": ""})
    )


def test_standard_shots_same_gate():
    std = _soc(pick_type="Standard", l5_over=5, l10_over=8)
    assert soccer_keep_eligible(std)
    assert standard_ticket_eligible(std)
    assert not soccer_keep_eligible(dict(std, l5_over=4))


def test_goals_and_volume_faded():
    assert not soccer_keep_prop(_soc(prop="Goals"))
    assert not soccer_keep_prop(_soc(prop="Assists"))
    assert not soccer_keep_eligible(_soc(prop="Goal + Assist", l5_over=5, l10_over=10))
    assert not soccer_keep_eligible(_soc(pick_type="Demon"))
    assert not soccer_keep_eligible(_soc(pick_type="Goblin", side="UNDER"))
