"""Slate explorer prop_tier (S–D) stamping."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from prop_hit_tiers import stamp_prop_tier_on_slate_row  # noqa: E402


def test_mlb_pitcher_ks_goblin_is_s():
    row = {
        "sport": "MLB",
        "tier": "A",
        "pick_type": "Goblin",
        "dir": "OVER",
        "prop": "Pitcher Strikeouts",
        "line": 2.5,
        "season_avg": 3.8,
        "def_tier": "Avg",
    }
    stamp_prop_tier_on_slate_row(row)
    assert row["edge_tier"] == "A"
    assert row["prop_tier"] == "S"


def test_wnba_goblin_points_is_a():
    row = {
        "sport": "WNBA",
        "tier": "B",
        "pick_type": "Goblin",
        "dir": "OVER",
        "prop": "Points",
        "line": 10.5,
        "season_avg": 18.0,
        "def_tier": "Weak",
    }
    stamp_prop_tier_on_slate_row(row)
    assert row["prop_tier"] == "A"
    assert row["edge_tier"] == "B"


def test_does_not_overwrite_existing_prop_tier():
    row = {
        "sport": "MLB",
        "tier": "D",
        "prop_tier": "S",
        "pick_type": "Standard",
        "dir": "OVER",
        "prop": "Hits",
    }
    stamp_prop_tier_on_slate_row(row)
    assert row["prop_tier"] == "S"
