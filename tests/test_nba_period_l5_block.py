"""NBA period tickets stay off when the L5-ratio flag says the window is off."""
from __future__ import annotations

import json

import utils.ticket_70_pool as t70


def test_period_block_follows_flag(monkeypatch, tmp_path):
    flag = tmp_path / "nba_period_l5_ratio.json"
    monkeypatch.setattr(t70, "_NBA_PERIOD_FLAG", flag)
    row = {"sport": "NBA1Q", "pick_type": "Goblin", "direction": "OVER"}
    assert t70.nba_period_tickets_blocked(row) is False
    flag.write_text(json.dumps({"block": True}), encoding="utf-8")
    assert t70.goblin_70_eligible(row) is False
    assert t70.standard_ticket_eligible(
        {"sport": "NBA1H", "pick_type": "Standard", "direction": "OVER"}
    ) is False
    flag.write_text(json.dumps({"block": False}), encoding="utf-8")
    assert t70.nba_period_tickets_blocked(row) is False
