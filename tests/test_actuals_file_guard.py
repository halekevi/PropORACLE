"""Existing actuals rows survive a later fetch."""
from pathlib import Path

import pandas as pd

from utils.actuals_file_guard import write_actuals_csv


def test_patch_appends_a_missing_team_and_keeps_the_rest(tmp_path: Path):
    path = tmp_path / "actuals_nfl_2026-09-27.csv"
    pd.DataFrame(
        [
            {"player": "A", "team": "BUF", "prop_type": "rushing_yards", "actual": 10},
            {"player": "B", "team": "BUF", "prop_type": "rushing_yards", "actual": 4},
        ]
    ).to_csv(path, index=False)
    fresh = pd.DataFrame(
        [
            {"player": "A", "team": "BUF", "prop_type": "rushing_yards", "actual": 99},
            {"player": "C", "team": "DEN", "prop_type": "rushing_yards", "actual": 49},
        ]
    )
    assert write_actuals_csv(path, fresh) == "patched"
    out = pd.read_csv(path)
    assert len(out) == 3
    kept = out[(out["player"] == "A") & (out["team"] == "BUF")]
    assert float(kept["actual"].iloc[0]) == 10
    assert set(out["team"]) == {"BUF", "DEN"}
    assert list(path.parent.glob("*.bak_*"))


def test_empty_fetch_does_not_wipe_rows(tmp_path: Path):
    path = tmp_path / "actuals_nfl_2026-09-27.csv"
    pd.DataFrame(
        [{"player": "A", "team": "BUF", "prop_type": "rushing_yards", "actual": 10}]
    ).to_csv(path, index=False)
    fresh = pd.DataFrame(columns=["player", "team", "prop_type", "actual"])
    assert write_actuals_csv(path, fresh) == "kept"
    assert len(pd.read_csv(path)) == 1
