"""Tests for slate-day cross-ticket anchor exposure ledger."""

from __future__ import annotations

from utils.slate_exposure_ledger import (
    SlateExposureLedger,
    max_total_tickets_per_player,
    pick_anchor_leg,
    trim_payload_slate_exposure,
)


def _leg(player: str, prop: str, direction: str = "OVER", **extra) -> dict:
    row = {
        "player": player,
        "prop_type": prop,
        "direction": direction,
        "pick_type": "Goblin",
        "line": 1.5,
    }
    row.update(extra)
    return row


def test_max_total_is_two_not_four():
    assert max_total_tickets_per_player() == 2


def test_mcgreevy_same_prop_blocked_on_second_ticket():
    led = SlateExposureLedger()
    t1 = [_leg("Michael McGreevy", "Pitches Thrown", badge="Gold", rank_score=9)]
    assert led.can_add(t1)[0]
    led.register(t1)
    t2 = [_leg("Michael McGreevy", "Pitches Thrown", badge="Gold", rank_score=8)]
    ok, reason = led.can_add(t2)
    assert not ok
    assert "same_prop" in reason or "anchor_cap" in reason


def test_boyd_second_prop_as_nonanchor_allowed_once():
    """Hits Allowed anchors ticket A; Pitching Outs as non-anchor on B is OK once."""
    led = SlateExposureLedger()
    a = [
        _leg("Matthew Boyd", "Hits Allowed", badge="Platinum", rank_score=10),
        _leg("Other Player", "Points", badge="Silver", rank_score=1),
    ]
    assert led.can_add(a)[0]
    led.register(a)
    # On B, Other2 is higher rank → Boyd is non-anchor with different prop
    b = [
        _leg("Fill Star", "Rebounds", badge="Diamond", rank_score=99),
        _leg("Matthew Boyd", "Pitching Outs", badge="Silver", rank_score=1),
    ]
    ok, reason = led.can_add(b)
    assert ok, reason


def test_boyd_second_prop_as_second_anchor_blocked():
    led = SlateExposureLedger()
    a = [_leg("Matthew Boyd", "Hits Allowed", badge="Gold", rank_score=10)]
    led.register(a)
    b = [_leg("Matthew Boyd", "Pitching Outs", badge="Gold", rank_score=10)]
    ok, reason = led.can_add(b)
    assert not ok
    assert "anchor_cap" in reason


def test_pick_anchor_prefers_higher_badge():
    legs = [
        _leg("A", "Points", badge="Silver", rank_score=99),
        _leg("B", "Assists", badge="Diamond", rank_score=1),
    ]
    assert pick_anchor_leg(legs)["player"] == "B"


def test_trim_payload_drops_later_duplicates_includes_order():
    payload = {
        "groups": [
            {
                "group_name": "STRONG 3-Leg",
                "tickets": [
                    {
                        "ticket_id": "t1",
                        "legs": [
                            _leg("Michael McGreevy", "Pitches Thrown", badge="Gold", rank_score=5)
                        ],
                    },
                    {
                        "ticket_id": "t2",
                        "legs": [
                            _leg("Michael McGreevy", "Pitches Thrown", badge="Gold", rank_score=4)
                        ],
                    },
                ],
            }
        ]
    }
    out = trim_payload_slate_exposure(payload)
    tickets = out["groups"][0]["tickets"]
    assert len(tickets) == 1
    assert tickets[0]["ticket_id"] == "t1"
    assert out.get("slate_exposure_dropped") == 1
