"""NBA minutes / role helpers."""
from __future__ import annotations

from utils.nba_minutes_role import ticket_minutes_fail_closed


def test_ticket_minutes_fail_closed():
    assert ticket_minutes_fail_closed({}) is False
    assert ticket_minutes_fail_closed({"minutes_flag": ""}) is False
    assert ticket_minutes_fail_closed({"minutes_flag": "no_minutes_data"}) is True
    assert ticket_minutes_fail_closed({"l5_minutes_gap": "no_minutes_data"}) is True
    assert ticket_minutes_fail_closed({"minutes_flag": "minutes_up"}) is False
    assert ticket_minutes_fail_closed({"sport": "NFL"}) is False
    assert ticket_minutes_fail_closed({"sport": "CFB"}) is False
    assert ticket_minutes_fail_closed({"sport": "MLB"}) is False
    assert ticket_minutes_fail_closed({"sport": "NBA"}) is True
    assert ticket_minutes_fail_closed({"sport": "NBA", "Clean Minutes": ""}) is True
    assert ticket_minutes_fail_closed({"sport": "NBA", "clean_minutes": 28.4}) is False
    assert ticket_minutes_fail_closed({"League": "NBA", "clean_minutes": False}) is False
