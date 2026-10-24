"""CBB copy-paste in the CFB pipeline: no-op vs active mis-score.

Rank penalty joins on (fold_player_name, canon_team_abbr) — same name + shared
abbr (MICH, USC, …) would apply a basketball injury to a football row.

Usage redistribution is team-first, then fuzzy name. A CBB Out on MICH with no
matching football roster name still boosts every MICH CFB player on the slate.

Today there is no injuries_cbb_*.csv on disk, so both paths are empty no-ops.
This file locks the join shape and the step6 sport strings so November overlap
cannot silently attach the wrong sport.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import pandas as pd

from espn_injuries import (
    auto_injuries_csv_from_outputs,
    canon_team_abbr,
    fold_player_name,
    penalty_series_for_slate,
)
from build_player_consistency import _normalize_prop_type as bpc_normalize_prop_type
from usage_redistribution import apply_usage_redistribution

_STEP6 = _REPO / "Sports" / "CFB" / "scripts" / "pipeline" / "step6_rank_props_cfb.py"


def _load_step6():
    import importlib.util

    spec = importlib.util.spec_from_file_location("step6_rank_props_cfb_under_test", _STEP6)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _injury_csv(path: Path, team: str, player: str, status: str = "Out", penalty: str = "-0.45") -> Path:
    path.write_text(
        "sport,team,player,injury_status,injury_type,rank_penalty\n"
        f"CBB,{team},{player},{status},O,{penalty}\n",
        encoding="utf-8",
    )
    return path


def test_rank_penalty_join_is_player_and_team(tmp_path):
    csv = _injury_csv(tmp_path / "injuries_cbb_2026-11-15.csv", "MICH", "John Smith")
    slate = pd.DataFrame(
        {
            "player": ["John Smith", "Jane Doe", "John Smith"],
            "team": ["MICH", "MICH", "USC"],
        }
    )
    ser = penalty_series_for_slate(slate, "player", "team", "CFB", csv)
    assert list(ser) == [-0.45, 0.0, 0.0]
    assert fold_player_name("John Smith")
    assert canon_team_abbr("CFB", "MICH") == canon_team_abbr("CBB", "MICH") == "MICH"


def test_cbb_injury_same_name_shared_abbr_is_active_wrong_not_empty(tmp_path):
    """If step6 loaded injuries_cbb onto a CFB slate, this row would take a real penalty."""
    csv = _injury_csv(tmp_path / "injuries_cbb.csv", "USC", "Quinn Ewers")
    slate = pd.DataFrame({"player": ["Quinn Ewers"], "team": ["USC"]})
    ser = penalty_series_for_slate(slate, "player", "team", "CFB", csv)
    assert float(ser.iloc[0]) == -0.45


def test_usage_redistribution_team_first_boosts_without_name_match(tmp_path):
    date = "2026-11-15"
    inj_dir = tmp_path / "outputs" / date
    inj_dir.mkdir(parents=True)
    _injury_csv(inj_dir / f"injuries_cbb_{date}.csv", "MICH", "Basketball Star")
    slate = pd.DataFrame({"player": ["Football RB"], "team": ["MICH"]})

    via_cbb = apply_usage_redistribution(slate, "CBB", date, str(tmp_path))
    assert float(via_cbb["usage_boost"].iloc[0]) > 0.0

    via_cfb = apply_usage_redistribution(slate, "CFB", date, str(tmp_path))
    assert float(via_cfb["usage_boost"].iloc[0]) == 0.0


def test_auto_injuries_cfb_does_not_resolve_cbb_file(tmp_path):
    date = "2026-11-15"
    out_dir = tmp_path / "outputs" / date
    out_dir.mkdir(parents=True)
    (out_dir / f"injuries_cbb_{date}.csv").write_text("player,team\nX,MICH\n", encoding="utf-8")
    assert auto_injuries_csv_from_outputs(tmp_path, date, "CBB") is not None
    assert auto_injuries_csv_from_outputs(tmp_path, date, "CFB") is None

    cfb = out_dir / f"injuries_cfb_{date}.csv"
    cfb.write_text("player,team\nY,MICH\n", encoding="utf-8")
    found = auto_injuries_csv_from_outputs(tmp_path, date, "CFB")
    assert found == cfb


def test_cfb_step6_no_longer_hardcodes_cbb_injury_or_usage_sport():
    src = _STEP6.read_text(encoding="utf-8")
    assert 'auto_injuries_csv_from_outputs(_repo_cbb, slate_game_date, "CFB")' in src
    assert 'auto_injuries_csv_from_outputs(_repo_cbb, slate_game_date, "CBB")' not in src
    assert '_sport_usage = "CFB"' in src
    assert 'else "CBB"' not in src
    assert 'model_keys = ["cfb"]' in src
    assert 'model_keys = ["cbb"]' not in src
    assert 'default="cfb_boxscore_cache.csv"' in src
    assert "injuries_cbb_" not in src
    assert 'm._normalize_prop_type(str(raw), "CFB")' in src
    assert '            "cfb",' in src
    assert '            "cbb",' not in src


def test_cfb_normalize_prop_type_does_not_remap_through_basketball_map():
    """Source-grep is not enough — call the step6 wrapper.

    sport=CBB runs the NBA/CBB map. sport=CFB title-cases and skips it.
    "points" is a same-string coincidence (map and .title() both yield
    "Points"). The live misses are abbreviated keys on the CFB board:
    fantasy → Fantasy Score, pts → Points, fga → FG Attempted.
    """
    assert bpc_normalize_prop_type("points", "CBB") == "Points"
    assert bpc_normalize_prop_type("points", "CFB") == "Points"
    assert bpc_normalize_prop_type("pts", "CBB") == "Points"
    assert bpc_normalize_prop_type("pts", "CFB") == "Pts"
    assert bpc_normalize_prop_type("fantasy", "CBB") == "Fantasy Score"
    assert bpc_normalize_prop_type("fantasy", "CFB") == "Fantasy"
    assert bpc_normalize_prop_type("fga", "CBB") == "FG Attempted"
    assert bpc_normalize_prop_type("fga", "CFB") == "Fga"
    assert bpc_normalize_prop_type("pra", "CBB") == "PRA"
    assert bpc_normalize_prop_type("pra", "CFB") == "Pra"

    step6 = _load_step6()
    assert step6._normalize_prop_type("pts") == "Pts"
    assert step6._normalize_prop_type("fantasy") == "Fantasy"
    assert step6._normalize_prop_type("fga") == "Fga"
    assert step6._normalize_prop_type("pra") == "Pra"
    assert step6._normalize_prop_type("points") == "Points"
    assert step6._normalize_prop_type("pass_yds") == "Pass_Yds"
    assert step6._normalize_prop_type("kick_pts") == "Kick_Pts"
