from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_CBB = _ROOT / "Sports" / "CBB"
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_CBB) not in sys.path:
    sys.path.insert(0, str(_CBB))

from prizepicks_league_ids import classify_pp_league_name, pick_league_id_from_rows
from scripts.sport_league_map import league_to_sport


def test_ncaabb_mens_maps_to_cbb():
    assert league_to_sport("NCAABB Mens") == "CBB"
    assert league_to_sport("NCAAB") == "CBB"
    assert league_to_sport("CBB") == "CBB"


def test_ncaabb_women_does_not_map_to_mens_cbb():
    assert league_to_sport("NCAABB Women") is None
    assert league_to_sport("NCAABB Womens") is None
    assert league_to_sport("WCBB") is None
    assert league_to_sport("CWBB") is None


def test_classify_pp_display_names():
    assert classify_pp_league_name("CBB") == "cbb"
    assert classify_pp_league_name("NCAAB") == "cbb"
    assert classify_pp_league_name("NCAABB Mens") == "cbb"
    assert classify_pp_league_name("NCAABB Women") == "wcbb"
    assert classify_pp_league_name("WCBB") == "wcbb"
    assert classify_pp_league_name("CBB1H") == "cbb1h"
    assert classify_pp_league_name("CBB2H") == "cbb2h"


def test_pick_league_id_prefers_live_rows():
    rows = [
        {"id": "20", "attributes": {"name": "CBB", "projections_count": 0}},
        {"id": "176", "attributes": {"name": "NCAABB Women", "projections_count": 12}},
    ]
    assert pick_league_id_from_rows("cbb", rows) == "20"
    assert pick_league_id_from_rows("wcbb", rows) == "176"


def test_cbb_shooting_prop_norm_and_boxscore_value():
    import importlib.util

    step2 = _ROOT / "Sports" / "CBB" / "scripts" / "pipeline" / "step2_normalize.py"
    spec = importlib.util.spec_from_file_location("cbb_step2_normalize", step2)
    mod2 = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod2)
    assert mod2.norm_prop("FG Attempted") == "fga"
    assert mod2.norm_prop("FG Made") == "fgm"
    assert mod2.norm_prop("3-PT Attempted") == "fg3a"
    assert mod2.norm_prop("Free Throws Made") == "ftm"
    assert mod2.norm_prop("Two Pointers Attempted") == "fg2a"

    step5 = _ROOT / "Sports" / "CBB" / "scripts" / "pipeline" / "step5b_attach_boxscore_stats.py"
    spec5 = importlib.util.spec_from_file_location("cbb_step5b_stats", step5)
    mod5 = importlib.util.module_from_spec(spec5)
    assert spec5.loader is not None
    spec5.loader.exec_module(mod5)
    row = {
        "PTS": 20, "REB": 5, "AST": 4, "STL": 1, "BLK": 0, "TO": 2,
        "3PM": 2, "3PA": 6, "FGM": 8, "FGA": 15, "FTM": 2, "FTA": 3,
        "FG2M": 6, "FG2A": 9,
    }
    assert mod5.prop_value("fga", row) == 15
    assert mod5.prop_value("fg3a", row) == 6
    assert mod5.prop_value("fta", row) == 3
    assert mod5.prop_value("fg2m", row) == 6
    assert mod5.prop_value("3-pt attempted", row) == 6
