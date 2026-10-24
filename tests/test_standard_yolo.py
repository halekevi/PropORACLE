"""Standard YOLO: gate-cleared only, no per-sport quota."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))

from build_goblin70_tickets import pack_standard_yolo, pack_yolo  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    standard_ticket_eligible,
    standard_yolo_eligible,
    standard_yolo_kind,
)


def _soc(**kw):
    row = {
        "sport": "Soccer",
        "pick_type": "Standard",
        "side": "OVER",
        "prop": "Shots",
        "player": "Keeper A",
        "l5_over": 5,
        "l10_over": 8,
        "def": "Weak",
        "own_def_tier": "Weak",
        "cover": 2.0,
        "dist_l5": 2.0,
    }
    row.update(kw)
    return row


def _wnba_pts(**kw):
    row = {
        "sport": "WNBA",
        "pick_type": "Standard",
        "side": "OVER",
        "prop": "points",
        "player": "Star A",
        "l5_over": 5,
        "l10_over": 9,
        "def": "Weak",
        "cover": 5.0,
        "dist_l5": 5.0,
    }
    row.update(kw)
    return row


def test_mlb_and_generic_wnba_never_yolo():
    mlb = {
        "sport": "MLB",
        "pick_type": "Standard",
        "side": "OVER",
        "prop": "hits",
        "player": "Batter",
        "l5_over": 5,
        "l10_over": 9,
        "def": "Weak",
        "ba": 0.320,
        "cover": 2.0,
    }
    assert not standard_yolo_eligible(mlb)
    assert not standard_ticket_eligible(mlb)
    # Coin-flip WNBA points can clear Flex gate but not Standard YOLO.
    pts = _wnba_pts()
    assert not standard_yolo_eligible(pts)


def test_soccer_saves_yolo_and_three_pack():
    a = _soc(player="A")
    b = _soc(player="B")
    c = _soc(player="C")
    assert standard_yolo_kind(a) == "soccer_keep"
    assert standard_yolo_eligible(a)
    combos = pack_standard_yolo([a, b, c])
    assert len(combos) == 1
    assert len(combos[0]) == 3
    assert {r["player"] for r in combos[0]} == {"A", "B", "C"}


def test_standard_yolo_sits_when_under_three():
    assert pack_standard_yolo([_soc(player="A"), _soc(player="B")]) == []


def test_pack_yolo_stacks_multiple_standards_before_goblin():
    stds = [_soc(player=f"S{i}") for i in range(3)]
    gobs = [
        {
            "sport": "WNBA",
            "pick_type": "Goblin",
            "side": "OVER",
            "prop": "fga",
            "player": f"G{i}",
            "l5_over": 5,
            "l10_over": 9,
            "def": "Weak",
            "cover": 2.0,
            "prop_tier": "A",
            "promo": "Diamond",
        }
        for i in range(6)
    ]
    combos = pack_yolo(gobs, stds, max_tickets=1)
    assert len(combos) == 1
    n_std = sum(1 for r in combos[0] if r["pick_type"] == "Standard")
    assert n_std == 3
