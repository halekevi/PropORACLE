"""Saves legs use past box coverage and posted lineups, not tonight's box."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from utils.soccer_board_coverage import (  # noqa: E402
    keeper_lineup_status,
    saves_leg_settleable,
)
from utils.soccer_keep_gates import soccer_keep_eligible  # noqa: E402


def _sv(**kwargs) -> dict:
    row = {
        "sport": "SOCCER",
        "player": "Luis Malagon",
        "prop": "Goalie Saves",
        "side": "OVER",
        "line": 2.5,
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "own_def_tier": "Weak",
        "opp_off_tier": "Elite",
        "team": "America",
        "opp_team": "Necaxa",
        "matchup": "America vs Necaxa (SOCCER)",
        "start_time": "2026-09-27T23:10:00-04:00",
    }
    row.update(kwargs)
    return row


def _patch(monkeypatch, *, history: bool, lineup: str) -> None:
    monkeypatch.setattr(
        "utils.soccer_board_coverage.competition_has_box_history",
        lambda row: history,
    )
    monkeypatch.setattr(
        "utils.soccer_board_coverage.keeper_lineup_status",
        lambda row: lineup,
    )
    monkeypatch.setattr(
        "utils.soccer_keep_gates.game_on_covered_board",
        lambda row, fetch=None: True,
    )


def test_unposted_lineup_passes_when_league_has_boxes(monkeypatch):
    _patch(monkeypatch, history=True, lineup="unposted")
    assert saves_leg_settleable(_sv()) is True
    assert soccer_keep_eligible(_sv()) is True


def test_posted_non_starter_blocks(monkeypatch):
    _patch(monkeypatch, history=True, lineup="not_starter")
    assert saves_leg_settleable(_sv()) is False
    assert soccer_keep_eligible(_sv()) is False


def test_posted_starter_passes(monkeypatch):
    _patch(monkeypatch, history=True, lineup="starter")
    assert saves_leg_settleable(_sv(player="Rodolfo Cota")) is True
    assert soccer_keep_eligible(_sv(player="Rodolfo Cota")) is True


def test_league_without_box_history_blocks(monkeypatch):
    _patch(monkeypatch, history=False, lineup="unposted")
    assert saves_leg_settleable(_sv()) is False


def test_lineup_status_from_roster():
    summary = {
        "rosters": [
            {
                "roster": [
                    {"starter": True, "athlete": {"displayName": "Rodolfo Cota"}},
                    {"starter": False, "athlete": {"displayName": "Angel Malagon"}},
                ]
            }
        ]
    }
    import utils.soccer_board_coverage as sbc

    old_find = sbc.find_covered_event
    old_sum = sbc._summary_for_event
    sbc.find_covered_event = lambda row, fetch=None: {"id": "1", "_league_id": "mex.1"}
    sbc._summary_for_event = lambda ev: summary
    try:
        assert keeper_lineup_status(_sv(player="Rodolfo Cota")) == "starter"
        assert keeper_lineup_status(_sv(player="Luis Malagon")) == "not_starter"
    finally:
        sbc.find_covered_event = old_find
        sbc._summary_for_event = old_sum
