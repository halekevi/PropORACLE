"""MLB Goblin OVER keep props 1–10 (list + Goblin-70)."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from rank_best_props_today import _clears_list_gate  # noqa: E402
from utils.mlb_keep_gates import mlb_goblin_keep_eligible  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402


def _mlb(**kwargs) -> dict:
    row = {
        "sport": "MLB",
        "player": "Test Arm",
        "prop": "Walks Allowed",
        "side": "OVER",
        "line": 1.5,
        "pick_type": "Goblin",
        "l5_over": 4,
        "l10_over": 8,
        "cover": 1.2,
        "own_def_tier": "Weak",
    }
    row.update(kwargs)
    return row


def test_walks_pitches_era_pitcher_ks():
    assert mlb_goblin_keep_eligible(_mlb())
    assert goblin_70_eligible(_mlb())
    assert _clears_list_gate(_mlb())
    assert not mlb_goblin_keep_eligible(_mlb(own_def_tier="Elite"))
    assert mlb_goblin_keep_eligible(
        _mlb(prop="Pitches Thrown", l5_over=2, own_def_tier="Below Avg")
    )
    era = _mlb(prop="Earned Runs Allowed", line=0.5, l5_over=5, l10_over=8)
    assert mlb_goblin_keep_eligible(era)
    assert not mlb_goblin_keep_eligible(dict(era, l10_over=6))
    assert not mlb_goblin_keep_eligible(dict(era, line=1.5, l10_over=10))
    ks = _mlb(prop="Pitcher Strikeouts", l5_over=5, own_def_tier="Elite")
    assert mlb_goblin_keep_eligible(ks)
    assert not mlb_goblin_keep_eligible(dict(ks, own_def_tier="Weak"))


def test_hits_allowed_hrrbi_hitter_ks_counting():
    ha = _mlb(prop="Hits Allowed", l5_over=4, **{"def": "Weak"})
    assert mlb_goblin_keep_eligible(ha)
    outs = _mlb(prop="Pitching Outs", l5_over=5, own_def_tier="Elite")
    assert mlb_goblin_keep_eligible(outs)
    hrrbi = _mlb(
        prop="Hits+Runs+RBIs",
        l5_over=5,
        batting_avg=0.290,
        **{"def": "Weak"},
    )
    assert mlb_goblin_keep_eligible(hrrbi)
    assert goblin_70_eligible(hrrbi)
    assert not mlb_goblin_keep_eligible(dict(hrrbi, batting_avg=0.250))
    assert not mlb_goblin_keep_eligible(dict(hrrbi, l5_over=4))
    hk = _mlb(
        prop="Hitter Strikeouts",
        l5_over=2,
        l10_over=8,
        k_rate=0.30,
        **{"def": "Elite"},
    )
    assert mlb_goblin_keep_eligible(hk)
    assert not goblin_70_eligible(hk)  # ticket hard-fade; list keep-gate still on
    assert _clears_list_gate(hk)
    assert not mlb_goblin_keep_eligible(dict(hk, **{"def": "Weak"}))
    assert not mlb_goblin_keep_eligible(dict(hk, k_rate=0.20))
    assert not mlb_goblin_keep_eligible(dict(hk, l10_over=7))
    hits = _mlb(
        prop="Hits",
        l5_over=5,
        batting_avg=0.280,
        **{"def": "Weak"},
    )
    assert mlb_goblin_keep_eligible(hits)
    assert goblin_70_eligible(hits)
    assert mlb_goblin_keep_eligible(dict(hits, cover=0.4))
    assert not mlb_goblin_keep_eligible(dict(hits, l5_over=4))
    assert not mlb_goblin_keep_eligible(dict(hits, batting_avg=0.260))
    tb = _mlb(
        prop="Total Bases",
        l5_over=5,
        batting_avg=0.300,
        **{"def": "Below Avg"},
    )
    assert mlb_goblin_keep_eligible(tb)


def test_fade_runs_standard_and_list():
    runs = _mlb(prop="Runs", l5_over=5, l10_over=10, **{"def": "Weak"})
    assert not mlb_goblin_keep_eligible(runs)
    assert not goblin_70_eligible(runs)
    assert not _clears_list_gate(runs)
    std = _mlb(pick_type="Standard", l5_over=5, l10_over=10)
    assert not _clears_list_gate(std)
    assert not standard_ticket_eligible(std)
    assert not goblin_70_eligible(std)
