"""Floor-persistence table: as-of only, n≥50, frozen NFL tracks untouched."""
from __future__ import annotations

import pandas as pd

from ml.floor_persistence import (
    K_VALUES,
    _asof_l5_min,
    _score_floor,
    assert_prereg,
    finalize,
)


def test_prereg_locked_before_results():
    spec = assert_prereg()
    assert spec["k"] == list(K_VALUES)
    assert spec["min_n"] == 50


def test_asof_l5_min_ignores_this_game():
    player = pd.Series(["A"] * 6)
    vals = pd.Series([10.0, 9.0, 8.0, 11.0, 7.0, 12.0])
    floor = _asof_l5_min(vals, player)
    assert pd.isna(floor.iloc[4])
    assert floor.iloc[5] == 7.0


def test_k_cushion_counts():
    actual = pd.Series([8.0, 9.0, 6.0])
    floor = pd.Series([9.0, 9.0, 9.0])
    mask = pd.Series([True, True, True])
    by_k = {r["k"]: r for r in _score_floor(actual, floor, mask)}
    assert by_k[0]["hits"] == 1
    assert by_k[1]["hits"] == 2
    assert by_k[2]["hits"] == 2


def test_floor_zero_is_not_a_floor():
    actual = pd.Series([0.0, 1.0])
    floor = pd.Series([0.0, 0.0])
    mask = pd.Series([True, True])
    rec = _score_floor(actual, floor, mask)[0]
    assert rec["n"] == 0


def test_does_not_import_frozen_nfl_tracks():
    import inspect

    import ml.floor_persistence as fp

    src = inspect.getsource(fp)
    assert "from utils.nfl_demon" not in src
    assert "from utils.nfl_std_under" not in src
    assert "from utils.nfl_demon_rec_floor" not in src


def test_finalize_marks_thin_and_bh():
    cells = [
        {
            "sport": "NBA",
            "prop": "reb",
            "prop_canon": "rebounds",
            "role": "lead",
            "kind": "floor",
            "k": 0,
            "hits": 90,
            "n": 100,
        },
        {
            "sport": "NBA",
            "prop": "reb",
            "prop_canon": "rebounds",
            "role": "lead",
            "kind": "floor",
            "k": 0,
            "hits": 2,
            "n": 4,
        },
    ]
    out = finalize(cells)
    by_n = {c["n"]: c for c in out}
    assert by_n[4]["thin"] is True
    assert by_n[100]["thin"] is False
    assert by_n[100]["hr"] == 0.9
