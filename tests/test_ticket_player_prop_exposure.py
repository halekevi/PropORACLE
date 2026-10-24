#!/usr/bin/env python3
"""Player+prop exposure: at most one ticket."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.ticket_diversity import apply_diversity_filter  # noqa: E402


def _t(tid: str, player: str, prop: str, line: float = 8.0, other: str = "Other Player") -> dict:
    return {
        "ticket_id": tid,
        "n_legs": 2,
        "base_ev": 1.0 if tid.endswith("a") else 0.5,
        "legs": [
            {"player": player, "prop_type": prop, "line": line, "direction": "OVER", "edge": 0.6},
            {"player": other, "prop_type": "Points", "line": 20.5, "direction": "OVER", "edge": 0.55},
        ],
    }


def test_player_prop_capped_at_one():
    cfg = {
        "enabled": True,
        "max_leg_exposure": 5,
        "max_player_exposure": 5,
        "max_player_prop_exposure": 1,
        "max_jaccard_overlap": 1.0,
        "void_risk_min_sample": 0,
    }
    # Same player+prop, different lines — still one ticket only.
    tickets = [
        _t("a", "Sabrina Ionescu", "Rebounds", 8.0, other="Player A"),
        _t("b", "Sabrina Ionescu", "Rebounds", 7.5, other="Player B"),
    ]
    out = apply_diversity_filter(tickets, cfg)
    assert len(out) == 1
    assert out[0]["ticket_id"] == "a"


def test_different_props_allowed():
    """Same player on different props is OK across tickets; not within one ticket."""
    cfg = {
        "enabled": True,
        "max_leg_exposure": 5,
        "max_player_exposure": 5,
        "max_player_prop_exposure": 1,
        "block_same_player_multistat": True,
        "max_jaccard_overlap": 1.0,
        "void_risk_min_sample": 0,
    }
    tickets = [
        _t("a", "Sabrina Ionescu", "Rebounds", 8.0, other="Player A"),
        _t("b", "Sabrina Ionescu", "Assists", 5.5, other="Player B"),
    ]
    out = apply_diversity_filter(tickets, cfg)
    assert len(out) == 2


def test_same_player_multistat_blocked_within_ticket():
    cfg = {
        "enabled": True,
        "max_leg_exposure": 5,
        "max_player_exposure": 5,
        "max_player_prop_exposure": 1,
        "block_same_player_multistat": True,
        "max_jaccard_overlap": 1.0,
        "void_risk_min_sample": 0,
    }
    bad = {
        "ticket_id": "stack",
        "n_legs": 2,
        "base_ev": 2.0,
        "legs": [
            {"player": "Sabrina Ionescu", "prop_type": "Rebounds", "line": 8.0, "direction": "OVER", "edge": 0.7},
            {"player": "Sabrina Ionescu", "prop_type": "Assists", "line": 5.5, "direction": "OVER", "edge": 0.7},
        ],
    }
    ok = _t("ok", "A'ja Wilson", "Points", 22.5, other="Player C")
    out = apply_diversity_filter([bad, ok], cfg)
    assert len(out) == 1
    assert out[0]["ticket_id"] == "ok"


def test_same_player_multistat_can_be_disabled():
    cfg = {
        "enabled": True,
        "max_leg_exposure": 5,
        "max_player_exposure": 5,
        "max_player_prop_exposure": 1,
        "block_same_player_multistat": False,
        "max_jaccard_overlap": 1.0,
        "void_risk_min_sample": 0,
    }
    bad = {
        "ticket_id": "stack",
        "n_legs": 2,
        "base_ev": 2.0,
        "legs": [
            {"player": "Sabrina Ionescu", "prop_type": "Rebounds", "line": 8.0, "direction": "OVER", "edge": 0.7},
            {"player": "Sabrina Ionescu", "prop_type": "Assists", "line": 5.5, "direction": "OVER", "edge": 0.7},
        ],
    }
    out = apply_diversity_filter([bad], cfg)
    assert len(out) == 1


def test_same_prop_allowed_in_separate_sections():
    """Per-section filter: same player+prop OK once in each group."""
    cfg = {
        "enabled": True,
        "max_leg_exposure": 5,
        "max_player_exposure": 5,
        "max_player_prop_exposure": 1,
        "max_jaccard_overlap": 1.0,
        "void_risk_min_sample": 0,
    }
    groups = [
        ("STRONG 6-Leg", [_t("s6a", "Sabrina Ionescu", "Rebounds", 8.0, other="A1")], None),
        ("STRONG 5-Leg", [_t("s5a", "Sabrina Ionescu", "Rebounds", 8.0, other="B1")], None),
    ]
    out = []
    for gname, tickets, bg in groups:
        kept = apply_diversity_filter(tickets, cfg)
        if kept:
            out.append((gname, kept, bg))
    assert len(out) == 2
    assert len(out[0][1]) == 1 and len(out[1][1]) == 1
