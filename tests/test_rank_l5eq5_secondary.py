"""L5=5 remaining-gate ranking helpers."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))

from rank_l5eq5_secondary import (  # noqa: E402
    _aligned_cover,
    _l5_eq_5,
    row_on_list,
    score_remaining,
)


def test_l5_eq_5_is_directional():
    assert _l5_eq_5({"side": "OVER", "l5_over": 5, "l5_under": 0})
    assert not _l5_eq_5({"side": "OVER", "l5_over": 4, "l5_under": 5})
    assert _l5_eq_5({"side": "UNDER", "l5_over": 0, "l5_under": 5})
    assert not _l5_eq_5({"side": "UNDER", "l5_over": 5, "l5_under": 4})


def test_under_cover_aligns_distance():
    assert _aligned_cover({"side": "OVER", "cover": 2.4}) == 2.4
    assert _aligned_cover({"side": "UNDER", "cover": -1.8}) == 1.8
    assert _aligned_cover({"side": "UNDER", "cover": 1.8}) == -1.8


def test_remaining_gates_count_l10_d_distance():
    rec = {
        "sport": "WNBA",
        "prop": "Points",
        "side": "OVER",
        "line": 7.5,
        "cover": 4.0,
        "l10": 9,
        "season_avg": 11.5,
        "def": "Weak",
        "checks": {"D": True},
    }
    raw = {
        "team_pace": 100.0,
        "opp_pace": 102.0,
        "pace_delta": 2.0,
        "hhs_ts_pct": 0.56,
        "stat_def_tier": "Weak",
    }
    scored = score_remaining(rec, raw)
    assert scored["gates"]["L10"] is True
    assert scored["gates"]["Season"] is True
    assert scored["gates"]["Distance"] is True
    assert scored["gates"]["D"] is True
    assert scored["gates"]["Poss"] is True
    assert scored["gates"]["Off"] is True
    assert scored["gates"]["OppDef"] is True
    assert scored["n_pass"] == 7


def test_easy_prop_d_counts_as_weak():
    rec = {
        "sport": "WNBA",
        "prop": "Pts+Rebs+Asts",
        "side": "OVER",
        "line": 19.5,
        "cover": 8.0,
        "l10": 7,
        "def": "Weak",
        "checks": {"D": True},
    }
    raw = {"stat_def_tier": "EASY"}
    scored = score_remaining(rec, raw)
    assert scored["gates"]["OppDef"] is True


def test_nflp_list_uses_playing_time_not_l5():
    backup_d = {
        "sport": "NFL",
        "league": "NFLP",
        "pick_type": "Standard",
        "side": "OVER",
        "starter_policy": "backup",
        "l5_over": None,
        "checks": {"D": True},
    }
    assert row_on_list(backup_d)
    sit_over = dict(backup_d, starter_policy="sit", l5_over=5)
    assert not row_on_list(sit_over)
    kicker = {
        "sport": "NFL",
        "league": "NFLP",
        "pick_type": "Goblin",
        "side": "OVER",
        "starter_policy": "plays",
        "l5_over": 5,
        "checks": {"D": False},
    }
    assert row_on_list(kicker)
    assert not row_on_list(dict(kicker, l5_over=2))
