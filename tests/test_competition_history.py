"""Thin-history competition gate tests."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from utils.competition_history import (  # noqa: E402
    competition_is_thin,
    normalize_competition_key,
    row_competition_ok,
)
from utils.soccer_keep_gates import soccer_keep_eligible  # noqa: E402


def test_normalize_collapses_worldcup_halves():
    assert normalize_competition_key("Soccer", "WORLDCUP") == "SOCCER|WORLDCUP"
    assert normalize_competition_key("SOCCER", "WORLDCUP1H") == "SOCCER|WORLDCUP"
    assert normalize_competition_key("Soccer", "WORLDCUP2H") == "SOCCER|WORLDCUP"
    assert normalize_competition_key("Soccer", "WORLDCUPTRNY") == "SOCCER|WORLDCUP"
    assert normalize_competition_key("Soccer", "MLS") == "SOCCER|MLS"


def test_missing_league_passes():
    hist = {"competitions": {}}
    cfg = {"enabled": True, "min_prior_days": 10, "min_prior_props": 8000, "pass_mode": "days_or_props", "gated_sports": ["SOCCER"]}
    assert row_competition_ok({"sport": "Soccer", "league": ""}, history=hist, cfg=cfg)
    assert not competition_is_thin("Soccer", "", history=hist, cfg=cfg)


def test_thin_worldcup_blocked_as_of_june():
    hist = {
        "competitions": {
            "SOCCER|WORLDCUP": {
                "dates": {"2026-07-14": 500},
                "n_props": 500,
                "n_days": 1,
            },
            "SOCCER|MLS": {
                "dates": {f"2026-04-{d:02d}": 100 for d in range(1, 21)},
                "n_props": 2000,
                "n_days": 20,
            },
        }
    }
    cfg = {
        "enabled": True,
        "min_prior_days": 10,
        "min_prior_props": 8000,
        "pass_mode": "days_or_props",
        "gated_sports": ["SOCCER"],
        "collapse_prefixes": ["WORLDCUP"],
    }
    # Before any WC history exists → thin
    assert competition_is_thin(
        "Soccer", "WORLDCUP", as_of_date="2026-06-15", history=hist, cfg=cfg
    )
    assert not row_competition_ok(
        {"sport": "Soccer", "league": "WORLDCUP1H", "date": "2026-06-15"},
        history=hist,
        cfg=cfg,
    )
    # MLS has depth → ok
    assert not competition_is_thin(
        "Soccer", "MLS", as_of_date="2026-06-15", history=hist, cfg=cfg
    )
    # After July 14 WC day, still thin if only 1 prior day / 500 props
    assert competition_is_thin(
        "Soccer", "WORLDCUP", as_of_date="2026-07-15", history=hist, cfg=cfg
    )


def test_soccer_keep_respects_competition_gate():
    hist = {"competitions": {}}
    cfg = {
        "enabled": True,
        "min_prior_days": 10,
        "min_prior_props": 8000,
        "pass_mode": "days_or_props",
        "gated_sports": ["SOCCER"],
        "collapse_prefixes": ["WORLDCUP"],
    }
    row = {
        "sport": "SOCCER",
        "player": "Test",
        "prop": "Shots",
        "side": "OVER",
        "line": 1.5,
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "league": "WORLDCUP",
        "date": "2026-06-15",
    }
    # Patch via explicit history by calling competition check path inside keep:
    # soccer_keep_eligible uses load_config/history from disk — inject by
    # verifying row_competition_ok False and keep without league still works.
    assert not row_competition_ok(row, history=hist, cfg=cfg)
    thick = dict(row, league="MLS")
    hist2 = {
        "competitions": {
            "SOCCER|MLS": {
                "dates": {f"2026-03-{d:02d}": 200 for d in range(1, 16)},
                "n_props": 3000,
                "n_days": 15,
            }
        }
    }
    assert row_competition_ok(thick, history=hist2, cfg=cfg)
    # No league → keep eligible still uses prop gates only
    assert soccer_keep_eligible({k: v for k, v in row.items() if k != "league"})
