#!/usr/bin/env python3
"""Manual slip builder: moved-line abort + exposure preflight."""

from __future__ import annotations

from unittest.mock import MagicMock

from utils.manual_slip_exposure import ExposureBlocked
from utils.pp_manual_slip import (
    LegAddResult,
    add_cycled_leg,
    preflight_exposure,
)


def _leg(player: str, prop: str = "Ks", line: float = 3.5) -> dict:
    return {
        "sport": "MLB",
        "player": player,
        "prop_type": prop,
        "direction": "OVER",
        "line": line,
        "pick_type": "goblin",
    }


def test_preflight_uses_assert_clears_exposure(monkeypatch):
    called = {}

    def fake_assert(legs, *, slate_date, explicit_anchor_player):
        called["legs"] = list(legs)
        called["date"] = slate_date
        called["anchor"] = explicit_anchor_player

    monkeypatch.setattr("utils.pp_manual_slip.assert_clears_exposure", fake_assert)
    rows = [_leg("Cam Schlittler"), _leg("Tyler Glasnow", line=4.5)]
    preflight_exposure(
        rows, slate_date="2026-09-24", explicit_anchor_player="Cam Schlittler"
    )
    assert called["anchor"] == "Cam Schlittler"
    assert called["date"] == "2026-09-24"
    assert called["legs"][0]["player"] == "Cam Schlittler"


def test_preflight_hard_stops_on_exposure_block(monkeypatch):
    def boom(*_a, **_k):
        raise ExposureBlocked("anchor_cap:devon dampier:1>=1")

    monkeypatch.setattr("utils.pp_manual_slip.assert_clears_exposure", boom)
    try:
        preflight_exposure(
            [_leg("Devon Dampier"), _leg("Fill")],
            slate_date="2026-09-20",
            explicit_anchor_player="Devon Dampier",
        )
        assert False, "expected ExposureBlocked"
    except ExposureBlocked as e:
        assert "anchor_cap" in e.reason


def test_preflight_requires_anchor():
    try:
        preflight_exposure([_leg("X")], slate_date="2026-09-24", explicit_anchor_player="")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_moved_line_is_line_moved_not_ok():
    """Schlittler 3.5 planned / 5.5 live → hard fail, not skip-and-continue."""
    payout = MagicMock()
    payout._line_key.side_effect = lambda x: None if x is None else float(x)
    payout.cycle_card_to_pick_type.return_value = None
    payout.find_prizepicks_frame.return_value = object()
    live = {
        "player": "Cam Schlittler",
        "prop_type": "Ks",
        "line": 5.5,
        "pick_type": "goblin",
        "more_btn": MagicMock(),
        "more_index": 0,
    }
    payout.get_all_cards.return_value = [live]
    page = MagicMock()
    result = add_cycled_leg(
        payout, frame=object(), page=page, leg=_leg("Cam Schlittler", line=3.5)
    )
    assert result.ok is False
    assert result.reason == "line_moved"
    assert result.live_line == 5.5
    assert result.want_line == 3.5
    assert "LINE MOVED" in result.loud_fail()
    assert "5.5" in result.loud_fail()
    assert "3.5" in result.loud_fail()
    live["more_btn"].click.assert_not_called()


def test_bulk_board_over_clicks_by_index():
    payout = MagicMock()
    payout._line_key.side_effect = lambda x: None if x is None else float(x)
    payout.click_leg.return_value = True
    card = {
        "player": "Josiah Freeman",
        "prop_type": "Receiving Yards",
        "line": 53.5,
        "pick_type": "standard",
        "more_btn": None,
        "more_index": 40,
    }
    payout.get_all_cards.return_value = [card]
    result = add_cycled_leg(
        payout,
        frame=object(),
        page=MagicMock(),
        leg={
            "sport": "CFB",
            "player": "Josiah Freeman",
            "prop_type": "Receiving Yards",
            "direction": "OVER",
            "line": 53.5,
            "pick_type": "standard",
        },
    )
    assert result.ok is True
    payout.click_leg.assert_called_once()


def test_cycle_none_but_exact_face_is_clicked():
    """Swap control can miss while the Goblin 0.5 face is already on the board."""
    payout = MagicMock()
    payout._line_key.side_effect = lambda x: None if x is None else float(x)
    payout.cycle_card_to_pick_type.return_value = None
    payout.find_prizepicks_frame.return_value = object()
    std = {
        "player": "Kumar Rocker",
        "prop_type": "Walks Allowed",
        "line": 1.5,
        "pick_type": "standard",
        "more_btn": MagicMock(),
        "more_index": 0,
    }
    gob = {
        "player": "Kumar Rocker",
        "prop_type": "Walks Allowed",
        "line": 0.5,
        "pick_type": "goblin",
        "more_btn": MagicMock(),
        "more_index": 1,
    }
    payout.get_all_cards.side_effect = [[std], [gob]]
    page = MagicMock()
    result = add_cycled_leg(
        payout,
        frame=object(),
        page=page,
        leg=_leg("Kumar Rocker", prop="Walks Allowed", line=0.5),
    )
    assert result.ok is True
    gob["more_btn"].click.assert_called()
    std["more_btn"].click.assert_not_called()
    from utils.pp_manual_slip import prop_ok

    assert prop_ok("Walks Allowed", "Walks Allowed")
    assert prop_ok("Walks Allowed", "walks allowed")
    assert not prop_ok("Walks Allowed", "Ks")
    assert not prop_ok("Walks Allowed", "Pitcher Strikeouts")
    assert prop_ok("Pitcher Strikeouts", "Ks")
    assert prop_ok("Hitter Strikeouts", "Hitter Ks")
    assert not prop_ok("Hitter Strikeouts", "Ks")
    assert not prop_ok("Hitter Strikeouts", "Pitcher Strikeouts")
    assert prop_ok("Pitching Outs", "PO")
    assert prop_ok("3-PT Made", "3PTM")
    assert prop_ok("Rush Attempts", "Rush Atts")
    assert prop_ok("Pass Completions", "Pass Comp")
    assert prop_ok("Receiving Yards", "Rec Yards")
    assert prop_ok("Longest Reception", "Longest Rec")
    assert prop_ok("Receptions", "Recs")
    assert prop_ok("Tackles+Ast", "Tackles")
    assert not prop_ok("Pitching Outs", "Pitches Thrown")
    msg = LegAddResult(
        False,
        "line_moved",
        player="Cam Schlittler",
        live_line=5.5,
        want_line=3.5,
        live_pick="goblin",
    ).loud_fail()
    assert msg.startswith("DROPPED: LINE MOVED")
    assert "short slip" in msg
