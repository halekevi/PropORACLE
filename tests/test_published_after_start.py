from __future__ import annotations

from utils.published_after_start import (
    AFTER_START_SLATE_DATES,
    INVALID_REASON,
    annotate_published_after_start_ticket,
    ticket_is_published_after_start,
)
from utils.stale_board_tickets import annotate_ticket_payload
from utils.ticket_70_pool import ticket_excluded_from_winrate


def _nba_ticket(*, date: str = "2026-10-23") -> dict:
    return {
        "ticket_id": "G70-nba-after-start",
        "date": date,
        "legs": [
            {
                "sport": "NBA",
                "player": "Shai Gilgeous-Alexander",
                "team": "OKC",
                "opp": "DEN",
                "prop_type": "Points",
                "line": 28.5,
                "direction": "OVER",
                "pick_type": "Goblin",
            }
        ],
    }


def test_oct23_card_is_published_after_start():
    assert "2026-10-23" in AFTER_START_SLATE_DATES
    t = _nba_ticket()
    assert ticket_is_published_after_start(t, slate_date="2026-10-23")
    assert ticket_excluded_from_winrate(t, slate_date="2026-10-23")
    payload = {
        "date": "2026-10-23",
        "groups": [{"group_name": "NBA Goblin-70 Power 3", "tickets": [t]}],
        "tickets": [t],
    }
    n = annotate_ticket_payload(payload)
    assert n >= 1
    stamped = payload["groups"][0]["tickets"][0]
    assert stamped["invalid"] is True
    assert stamped["invalid_reason"] == INVALID_REASON
    assert stamped["exclude_from_winrate"] is True
    assert stamped["exclude_from_book"] is True
    assert stamped["exclude_from_forward"] is True
    assert str(stamped.get("result") or "").upper() != "VOID"


def test_oct24_is_not_auto_tagged():
    t = _nba_ticket(date="2026-10-24")
    assert not ticket_is_published_after_start(t, slate_date="2026-10-24")
    assert not ticket_excluded_from_winrate(t, slate_date="2026-10-24")


def test_does_not_overwrite_stale_board():
    t = _nba_ticket()
    t["invalid_reason"] = "stale_board"
    assert annotate_published_after_start_ticket(t, slate_date="2026-10-23") is False
    assert t["invalid_reason"] == "stale_board"
