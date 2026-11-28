import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "Sports" / "NBA" / "scripts"))
import step2_attach_picktypes as s2  # noqa: E402


def _board():
    return pd.DataFrame({
        "pp_game_id": ["1", "1", "2", "3"],
        "team": ["ORL", "WAS", "CHI", "OKC"],
        "is_combo_player": [0, 0, 0, 0],
        "start_time": ["2026-11-28T17:00:00.000-05:00"] * 2 + ["2026-11-28T20:00:00.000-05:00"] * 2,
    })


def test_split_game_ids_use_espn_opponent(monkeypatch):
    monkeypatch.setattr(s2, "_espn_opponents", lambda d: {"CHI": "IND", "IND": "CHI", "OKC": "MIL", "MIL": "OKC"})
    assert list(s2.build_opp_team_from_gameid(_board())) == ["WAS", "ORL", "IND", "MIL"]


def test_espn_down_leaves_orphans_blank(monkeypatch):
    monkeypatch.setattr(s2, "_espn_opponents", lambda d: None)
    assert list(s2.build_opp_team_from_gameid(_board())) == ["WAS", "ORL", "", ""]


def test_espn_maps_abbreviations():
    assert s2._pp_abbr("WSH") == "WAS" and s2._pp_abbr("GS") == "GSW" and s2._pp_abbr("UTAH") == "UTA"
