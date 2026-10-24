"""Feature-gap wiring: per-stat D, season avg vs line, prop codes, tail caps."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from edge_feature_engineering import (  # noqa: E402
    FEATURE_COLUMNS,
    SPORT_ENCODING,
    build_feature_vector,
)
from edge_predict_utils import apply_ml_prob_post_calibration, load_unified_edge_model  # noqa: E402
from utils.ml_prob_tail_caps import apply_ml_prob_tail_caps  # noqa: E402


def test_pickle_features_still_built():
    loaded = load_unified_edge_model(ROOT / "models")
    if loaded is None:
        return
    _model, feats = loaded
    df = pd.DataFrame(
        {
            "player": ["A"],
            "prop_type": ["Points"],
            "line": [20.5],
            "stat_last5_avg": [22.0],
            "stat_last10_avg": [21.0],
            "stat_season_avg": [19.5],
            "pick_type": ["goblin"],
            "direction": ["OVER"],
            "OVERALL_DEF_RANK": [8],
            "DEF_TIER": ["Avg"],
        }
    )
    out = build_feature_vector(df, "WNBA")
    for c in ("def_rank", "hit_rate_L5", "avg_L5_vs_line", "minutes_tier", "pick_type_encoded"):
        assert c in out.columns
    for c in feats:
        if c not in out.columns:
            out[c] = np.nan
        ser = pd.to_numeric(out[c], errors="coerce")
        assert len(ser) == 1


def test_stat_def_rank_preferred_for_live_def_rank():
    df = pd.DataFrame(
        {
            "prop_type": ["Points"],
            "line": [18.5],
            "OVERALL_DEF_RANK": [12.0],
            "DEF_TIER": ["Elite"],
            "stat_def_rank": [3.0],
            "stat_def_tier": ["Weak"],
        }
    )
    out = build_feature_vector(df, "WNBA")
    assert float(out["def_rank"].iloc[0]) == 3.0
    assert float(out["stat_def_rank"].iloc[0]) == 3.0
    assert float(out["def_tier_encoded"].iloc[0]) == 0.0


def test_season_avg_vs_line_and_prop_code():
    df = pd.DataFrame(
        {
            "prop_type": ["Rebounds"],
            "line": [10.0],
            "stat_last5_avg": [12.0],
            "stat_season_avg": [8.0],
        }
    )
    out = build_feature_vector(df, "WNBA")
    assert abs(float(out["avg_season_vs_line"].iloc[0]) - (-0.2)) < 1e-9
    assert float(out["prop_encoded"].iloc[0]) == 1.0


def test_tennis_surface_features_nan_on_mlb():
    mlb = build_feature_vector(
        pd.DataFrame(
            {
                "prop_type": ["Hits"],
                "line": [0.5],
                "surface_encoded": [1],
                "aces_per_match_mean": [7.0],
            }
        ),
        "MLB",
    )
    assert pd.isna(mlb["aces_per_match_mean"].iloc[0])
    ten = build_feature_vector(
        pd.DataFrame(
            {
                "prop_type": ["Aces"],
                "line": [6.5],
                "surface": ["Clay"],
                "aces_per_match_mean": [8.2],
                "surface_specialist": ["true"],
                "win_rate_on_surface": [0.7],
            }
        ),
        "TENNIS",
    )
    assert float(ten["surface_encoded"].iloc[0]) == 1.0
    assert float(ten["aces_per_match_mean"].iloc[0]) == 8.2
    assert float(ten["surface_specialist"].iloc[0]) == 1.0
    assert "surface_encoded" in FEATURE_COLUMNS


def test_golf_not_encoded_as_nba():
    assert SPORT_ENCODING["GOLF"] != SPORT_ENCODING["NBA"]
    df = pd.DataFrame({"prop_type": ["Fairways"], "line": [6.5]})
    out = build_feature_vector(df, "GOLF")
    assert float(out["sport_encoded"].iloc[0]) == float(SPORT_ENCODING["GOLF"])


def test_nfl_cfb_not_encoded_as_mlb():
    assert SPORT_ENCODING["NFL"] != SPORT_ENCODING["MLB"]
    assert SPORT_ENCODING["CFB"] != SPORT_ENCODING["MLB"]
    assert SPORT_ENCODING["NFL"] != SPORT_ENCODING["NBA"]
    nfl = build_feature_vector(
        pd.DataFrame(
            {
                "prop_type": ["Passing Yards"],
                "line": [245.5],
                "snap_pct_L3": [82.0],
                "minutes_tier": ["HIGH"],
                "role_stability_score": [0.7],
            }
        ),
        "NFL",
    )
    assert float(nfl["sport_encoded"].iloc[0]) == float(SPORT_ENCODING["NFL"])
    assert float(nfl["snap_pct_L3"].iloc[0]) == 82.0
    assert float(nfl["minutes_tier"].iloc[0]) == 2.0
    assert float(nfl["role_stability_score"].iloc[0]) == 0.7
    mlb = build_feature_vector(
        pd.DataFrame({"prop_type": ["Hits"], "line": [0.5], "snap_pct_L3": [82.0]}),
        "MLB",
    )
    assert pd.isna(mlb["snap_pct_L3"].iloc[0])
    assert "snap_pct_L3" in FEATURE_COLUMNS
    cfb = build_feature_vector(pd.DataFrame({"prop_type": ["Rush Yards"], "line": [70.5]}), "CFB")
    assert float(cfb["sport_encoded"].iloc[0]) == float(SPORT_ENCODING["CFB"])
    cbb = build_feature_vector(pd.DataFrame({"prop_type": ["Points"], "line": [18.5]}), "CBB")
    assert float(cbb["sport_encoded"].iloc[0]) == float(SPORT_ENCODING["CBB"])
    cfb1h = build_feature_vector(pd.DataFrame({"prop_type": ["Pass Yards"], "line": [120.5]}), "CFB1H")
    assert float(cfb1h["sport_encoded"].iloc[0]) == float(SPORT_ENCODING["CFB"])


def test_mlb_goblin_tail_cap():
    p = np.array([0.61, 0.99, 0.40])
    pick = pd.Series(["goblin", "goblin", "goblin"])
    direction = pd.Series(["OVER", "OVER", "OVER"])
    capped = apply_ml_prob_tail_caps(p, sport="MLB", pick_lower=pick, dir_upper=direction)
    assert capped[0] == 0.61
    assert capped[1] == 0.72
    assert capped[2] == 0.40
    tennis = apply_ml_prob_tail_caps(p, sport="TENNIS", pick_lower=pick, dir_upper=direction)
    assert tennis[1] == 0.99


def test_post_cal_applies_tail_cap(tmp_path):
    p = np.array([0.95])
    pick = pd.Series(["goblin"])
    direction = pd.Series(["OVER"])
    out = apply_ml_prob_post_calibration(p, "MLB", pick, direction, tmp_path)
    assert out[0] <= 0.72 + 1e-9
