"""Tennis step1 joins the Standard sibling onto Goblin rows."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

_STEP1 = Path(__file__).resolve().parents[1] / "Sports" / "Tennis" / "scripts" / "step1_fetch_prizepicks_tennis.py"
_spec = importlib.util.spec_from_file_location("tennis_step1_gap", _STEP1)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
attach_goblin_standard_gap = _mod.attach_goblin_standard_gap


def test_goblin_gap_uses_highest_standard_sibling():
    df = pd.DataFrame(
        [
            {"player": "Novak Djokovic", "prop_type": "Total Games", "pick_type": "Standard", "line_score": 22.5, "standard_line": 22.5},
            {"player": "Novak Djokovic", "prop_type": "Total Games", "pick_type": "Standard", "line_score": 24.5, "standard_line": 24.5},
            {"player": "Novak Djokovic", "prop_type": "Total Games", "pick_type": "Goblin", "line_score": 19.5, "standard_line": pd.NA},
            {"player": "Novak Djokovic", "prop_type": "Aces", "pick_type": "Goblin", "line_score": 4.5, "standard_line": pd.NA},
            {"player": "Novak Djokovic", "prop_type": "Total Games", "pick_type": "Demon", "line_score": 28.5, "standard_line": pd.NA},
        ]
    )
    out, filled, missing = attach_goblin_standard_gap(df)
    gob = out[out["pick_type"].eq("Goblin") & out["prop_type"].eq("Total Games")].iloc[0]
    assert filled == 1
    assert missing == 1
    assert float(gob["standard_line"]) == 24.5
    assert float(gob["gap"]) == 5.0
    assert gob["standard_line_source"] == "sibling"
    aces = out[out["prop_type"].eq("Aces")].iloc[0]
    assert pd.isna(aces["gap"])
    assert pd.isna(aces["standard_line"])
    demon = out[out["pick_type"].eq("Demon")].iloc[0]
    assert pd.isna(demon["standard_line"])
    assert pd.isna(demon["gap"])
