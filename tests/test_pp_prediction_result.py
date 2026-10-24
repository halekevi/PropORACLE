"""PrizePicks prediction payloads keep the per-leg settlement string."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))

from capture_entries import (  # noqa: E402
    _parse_prediction_payload,
    _prediction_settlement,
    board_opponent_from,
    scraped_n_correct_multiplier,
)


def test_settlement_reads_result_and_skips_pending():
    assert _prediction_settlement({"result": "won"}) == "won"
    assert _prediction_settlement({"status": "pending"}) is None
    assert _prediction_settlement({"grade": {"name": "lost"}}) == "lost"


def test_parse_payload_stores_pp_result():
    payload = {
        "data": {
            "id": "99",
            "attributes": {
                "player_name": "Test Player",
                "stat_type": "Shots On Goal",
                "line_score": 2.5,
                "result": "correct",
            },
        }
    }
    parsed = _parse_prediction_payload(payload)
    assert parsed["pp_result"] == "correct"
    assert parsed["player_name"] == "Test Player"


def test_n_correct_multiplier_ignores_place_prize():
    attrs = {
        "parlay_count": 3,
        "payouts": {
            "3": {"multiplier": 6.0},
            "1st": {"multiplier": 10.0, "label": "1st place"},
        },
    }
    assert scraped_n_correct_multiplier(attrs) == 6.0
    assert scraped_n_correct_multiplier({"parlay_count": 3, "payouts": {}}) is None


def test_board_opponent_reads_prediction_attribute():
    assert board_opponent_from({"opponent_name": "Sinner"}) == "Sinner"
    assert board_opponent_from({"result": "won"}) is None
