"""Goblin specialist clean-retrain: analysis_legs dataset + locked date cut. No promote."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from build_retrain_dataset import (  # noqa: E402
    analysis_legs_to_retrain_frame,
    drop_source_board_date_mismatches,
    expand_ledger_extra_json,
)
from edge_feature_engineering import FEATURE_COLUMNS, build_feature_vector  # noqa: E402
from train_edge_model import (  # noqa: E402
    _rung_label,
    _split_by_test_from,
    _sport_rung_metrics,
)


def test_build_from_analysis_legs_uses_data_ledger_not_repo_root(tmp_path, monkeypatch):
    import build_retrain_dataset as brd

    seen: dict[str, object] = {}
    (tmp_path / "data" / "ledger").mkdir(parents=True)

    def fake_analysis_legs(root=None, **kwargs):
        seen["legs_root"] = root
        return pd.DataFrame()

    def fake_read_table(table, root=None):
        seen["feat_root"] = root
        return pd.DataFrame()

    monkeypatch.setattr("proporacle.data.leg_ledger.analysis_legs", fake_analysis_legs)
    monkeypatch.setattr("proporacle.data.leg_ledger.read_table_df", fake_read_table)
    out = brd.build_from_analysis_legs(tmp_path, output=tmp_path / "x.csv")
    assert out.empty
    assert seen.get("legs_root") is not None
    assert Path(seen["legs_root"]).name == "ledger"
    assert Path(seen["legs_root"]).parent.name == "data"


def test_clean_features_are_in_feature_columns():
    assert "l5_current_season_n" in FEATURE_COLUMNS
    assert "clean_minutes" in FEATURE_COLUMNS


def test_build_feature_vector_passes_clean_columns():
    df = pd.DataFrame(
        {
            "prop_type": ["Points"],
            "line": [20.5],
            "l5_current_season_n": [5],
            "clean_minutes": [True],
            "pick_type": ["goblin"],
            "direction": ["OVER"],
        }
    )
    out = build_feature_vector(df, "NBA")
    assert float(out["l5_current_season_n"].iloc[0]) == 5.0
    assert float(out["clean_minutes"].iloc[0]) == 1.0


def test_expand_extra_json_skips_postgame():
    feat = pd.DataFrame(
        {
            "leg_id": ["a"],
            "extra_json": [json.dumps({"l5_current_season_n": 3, "actual_value": 99, "clean_minutes": 1})],
        }
    )
    out = expand_ledger_extra_json(feat)
    assert float(out["l5_current_season_n"].iloc[0]) == 3.0
    assert float(out["clean_minutes"].iloc[0]) == 1.0
    assert "actual_value" not in out.columns or pd.isna(out.get("actual_value", pd.Series([np.nan])).iloc[0])


def test_analysis_legs_to_retrain_frame_maps_hit_and_l5():
    legs = pd.DataFrame(
        {
            "leg_id": ["1", "2"],
            "sport": ["NBA", "WNBA"],
            "player": ["A", "B"],
            "prop": ["Points", "Points"],
            "line": [20.5, 12.5],
            "direction": ["OVER", "OVER"],
            "pick_type": ["goblin", "standard"],
            "result": ["HIT", "MISS"],
            "hit": [1, 0],
            "slate_date": ["2026-08-15", "2026-09-10"],
            "game_date": ["2026-08-15", "2026-09-10"],
            "face_verified": [True, True],
        }
    )
    feat = pd.DataFrame(
        {
            "leg_id": ["1", "2"],
            "l5_over": [5.0, 4.0],
            "l10_over": [8.0, 7.0],
            "edge": [2.0, 1.0],
            "ml_prob": [0.7, 0.55],
            "extra_json": [
                json.dumps({"l5_current_season_n": 5, "clean_minutes": True}),
                json.dumps({"l5_current_season_n": 2, "clean_minutes": False}),
            ],
        }
    )
    out = analysis_legs_to_retrain_frame(legs, feat)
    assert len(out) == 2
    assert list(out["result_binary"].astype(int)) == [1, 0]
    assert "l5_current_season_n" in out.columns
    assert float(out.loc[out["player"].eq("A"), "l5_current_season_n"].iloc[0]) == 5.0
    assert float(out.loc[out["player"].eq("A"), "hit_rate_over_L5"].iloc[0]) == 1.0
    assert str(out["file_date"].iloc[0])[:10] == "2026-08-15"
    assert "sport_x" not in out.columns
    assert "sport" in out.columns


def test_source_board_date_mismatch_dropped():
    df = pd.DataFrame(
        {
            "sport": ["NBA", "NBA"],
            "slate_date": ["2026-10-22", "2026-10-22"],
            "game_date": ["2026-10-22", "2026-05-28"],
            "SOURCE_BOARD_DATE": ["2026-10-22", "2026-05-28"],
            "result_binary": [1, 0],
        }
    )
    out = drop_source_board_date_mismatches(df)
    assert len(out) == 1
    assert str(out["game_date"].iloc[0])[:10] == "2026-10-22"


def test_split_by_test_from_respects_cut():
    n = 20
    dates = pd.date_range("2026-08-20", periods=n, freq="D")
    df = pd.DataFrame(
        {
            "_stress_event_dt": dates,
            "y": [i % 2 for i in range(n)],
            "sport": ["NBA"] * n,
        }
    )
    tr, cal, te = _split_by_test_from(df, "2026-09-01")
    assert tr["_stress_event_dt"].max() < pd.Timestamp("2026-09-01")
    assert cal["_stress_event_dt"].max() < pd.Timestamp("2026-09-01")
    assert te["_stress_event_dt"].min() >= pd.Timestamp("2026-09-01")
    assert len(tr) + len(cal) + len(te) == n


def test_split_by_test_from_empty_test_raises():
    df = pd.DataFrame(
        {
            "_stress_event_dt": pd.to_datetime(["2026-08-01", "2026-08-02"]),
            "y": [1, 0],
        }
    )
    with pytest.raises(ValueError, match="test n=0"):
        _split_by_test_from(df, "2026-09-01")


def test_rung_label_and_sport_rung_missing_n():
    assert _rung_label("Goblin") == "Goblin"
    assert _rung_label("") == "Standard"
    te = pd.DataFrame({"sport": ["NBA"] * 10, "pick_type": ["goblin"] * 10})
    y = np.array([1, 0] * 5, dtype=float)
    p = np.linspace(0.4, 0.7, 10)
    rows = _sport_rung_metrics(te, y, p, min_auc_n=50, min_corr_n=40)
    assert rows[0]["n"] == 10
    assert rows[0]["corr"] is None
    assert rows[0]["auc"] is None
    assert rows[0]["mean_p"] is not None
