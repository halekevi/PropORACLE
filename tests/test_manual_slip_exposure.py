#!/usr/bin/env python3
"""Manual path exposure guard — session register + submit hard gate."""

from __future__ import annotations

import json
from pathlib import Path

from utils.manual_slip_exposure import (
    ExposureBlocked,
    assert_clears_exposure,
    check_slip_exposure,
    load_session_slips,
    register_after_submit,
    session_register_path,
)
from utils.pp_submit_guard import submit_lineup_guarded


def _leg(player: str, prop: str, direction: str = "OVER", **extra) -> dict:
    row = {
        "player": player,
        "prop_type": prop,
        "direction": direction,
        "pick_type": "Goblin",
        "line": 1.5,
        "badge": "Gold",
        "rank_score": 5,
    }
    row.update(extra)
    return row


def test_session_register_blocks_second_dampier_style_anchor(tmp_path, monkeypatch):
    """Dampier anchors ticket A; second manual slip with Dampier as anchor fails."""
    import utils.manual_slip_exposure as M

    monkeypatch.setattr(M, "_SESSION_DIR", tmp_path)
    monkeypatch.setattr(
        M,
        "load_published_tickets",
        lambda _d: {"date": "2026-09-20", "groups": []},
    )
    d = "2026-09-20"
    a = [
        _leg("Devon Dampier", "Pass Yards", badge="Platinum", rank_score=10),
        _leg("Other A", "Points", badge="Silver", rank_score=1),
        _leg("Other B", "Assists", badge="Silver", rank_score=1),
    ]
    ok, _ = check_slip_exposure(a, slate_date=d, explicit_anchor_player="Devon Dampier")
    assert ok
    register_after_submit(a, slate_date=d, explicit_anchor_player="Devon Dampier")
    assert session_register_path(d).is_file()
    assert len(load_session_slips(d)) == 1

    b = [
        _leg("Devon Dampier", "Pass Yards", badge="Gold", rank_score=9),
        _leg("Fill C", "Rebounds", badge="Silver", rank_score=1),
        _leg("Fill D", "Steals", badge="Silver", rank_score=1),
    ]
    ok2, reason = check_slip_exposure(
        b, slate_date=d, explicit_anchor_player="Devon Dampier"
    )
    assert not ok2
    assert "anchor_cap" in reason or "same_prop" in reason or "total_cap" in reason


def test_assert_clears_raises():
    import utils.manual_slip_exposure as M
    from unittest.mock import patch

    with patch.object(M, "check_slip_exposure", return_value=(False, "anchor_cap:x")):
        try:
            assert_clears_exposure([_leg("X", "Y")], slate_date="2026-09-20")
            assert False, "expected ExposureBlocked"
        except ExposureBlocked as e:
            assert "anchor_cap" in e.reason


def test_submit_guard_never_clicks_when_blocked(monkeypatch):
    """Guarded submit must not click PrizePicks when ledger fails."""
    clicked = {"n": 0}

    def boom(*_a, **_k):
        clicked["n"] += 1
        return "clicked_submit"

    monkeypatch.setattr("utils.pp_submit_guard._click_submit", boom)
    monkeypatch.setattr(
        "utils.pp_submit_guard.assert_clears_exposure",
        lambda *a, **k: (_ for _ in ()).throw(ExposureBlocked("anchor_cap:test")),
    )
    try:
        submit_lineup_guarded(
            page=None,
            frame=None,
            legs=[_leg("Devon Dampier", "Pass Yards")],
            slate_date="2026-09-20",
            explicit_anchor_player="Devon Dampier",
        )
        assert False, "expected ExposureBlocked"
    except ExposureBlocked:
        pass
    assert clicked["n"] == 0
