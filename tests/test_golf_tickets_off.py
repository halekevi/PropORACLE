"""Golf stays off tickets until a keep-gate cell is pre-registered."""
from __future__ import annotations

from utils.golf_keep_gates import golf_ticket_eligible
from utils.ticket_70_pool import goblin_70_eligible, ticket_gate_passes
from utils.ticket_keep_registry import rule_for


def _golf(**kw):
    row = {
        "sport": "GOLF",
        "player": "Trace Crowe",
        "prop": "Strokes",
        "side": "OVER",
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "cover": 2.0,
        "def": "N/A",
    }
    row.update(kw)
    return row


def test_golf_unregistered_like_cbb():
    assert rule_for("GOLF") is None
    assert rule_for("PGA") is None
    assert rule_for("CBB") is None
    assert not golf_ticket_eligible(_golf())
    assert not ticket_gate_passes(_golf())
    assert not goblin_70_eligible(_golf())
    assert not ticket_gate_passes(_golf(l10_over=7))
