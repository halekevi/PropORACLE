"""Soccer Excel Min Tier labels and CV% from game logs."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_MOD_PATH = ROOT / "Sports" / "Soccer" / "scripts" / "step8_add_direction_context_soccer.py"
_SPEC = importlib.util.spec_from_file_location("step8_soccer_ctx", _MOD_PATH)
_MOD = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(_MOD)

restore_soccer_minutes_tier = _MOD.restore_soccer_minutes_tier
attach_soccer_cv_pct = _MOD.attach_soccer_cv_pct


def test_restore_minutes_from_label_not_encoded_zero():
    df = pd.DataFrame(
        {
            "minutes_tier": [0, 0, 2],
            "minutes_tier_label": ["HIGH", "MEDIUM", "LOW"],
        }
    )
    out = restore_soccer_minutes_tier(df)
    assert list(out["minutes_tier"]) == ["HIGH", "MEDIUM", "LOW"]


def test_cv_pct_blank_without_enough_games():
    df = pd.DataFrame(
        {
            "stat_g1": [2.0, 4.0],
            "stat_g2": [2.0, 6.0],
            "stat_g3": [pd.NA, 8.0],
        }
    )
    out = attach_soccer_cv_pct(df, min_games=3)
    assert pd.isna(out["cv_pct"].iloc[0])
    assert float(out["cv_pct"].iloc[1]) > 0
