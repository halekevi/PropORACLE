"""QB mobility / red-zone attach + sacks_taken L5 mapping."""

import importlib.util
import sys
from pathlib import Path

import pandas as pd

from utils.nfl_context_layers import attach_nfl_context_layers, attach_nfl_qb_mobility

_REPO = Path(__file__).resolve().parents[1]
_STEP5B = _REPO / "Sports" / "CFB" / "scripts" / "pipeline" / "step5b_attach_boxscore_stats.py"


def _load_prop_value():
    spec = importlib.util.spec_from_file_location("step5b_box", _STEP5B)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["step5b_box"] = mod
    spec.loader.exec_module(mod)
    return mod.prop_value


def test_sacks_taken_uses_pass_sacks_not_def_sack():
    prop_value = _load_prop_value()
    row = {"PASS_SACKS": 2.0, "SACK": 5.0}
    assert prop_value("sacks_taken", row) == 2.0
    assert prop_value("sacks", row) == 5.0


def test_rush_rec_tds_aliases():
    prop_value = _load_prop_value()
    row = {"RUSH_TD": 1.0, "REC_TD": 2.0}
    assert prop_value("rush_tds", row) == 1.0
    assert prop_value("rec_tds", row) == 2.0
    assert prop_value("rushing_tds", row) == 1.0


def test_attach_qb_mobility_soft(tmp_path):
    data = tmp_path / "Sports" / "NFL" / "data"
    data.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "player_norm": "lamar jackson",
                "player": "Lamar Jackson",
                "team": "BAL",
                "qb_mobility_tier": "Mobile",
                "scramble_rate": 0.12,
                "sack_rate": 0.05,
                "rush_att_pg": 8.0,
                "dropbacks": 200,
                "sacks_taken": 10,
            }
        ]
    ).to_csv(data / "nfl_qb_mobility.csv", index=False)
    slate = pd.DataFrame([{"player": "Lamar Jackson", "team": "BAL", "prop": "sacks_taken"}])
    out = attach_nfl_qb_mobility(slate, root=tmp_path)
    assert out.iloc[0]["qb_mobility_tier"] == "Mobile"
    out2 = attach_nfl_context_layers(slate, root=tmp_path)
    assert out2.iloc[0]["qb_mobility_tier"] == "Mobile"
