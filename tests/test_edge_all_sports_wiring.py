"""NFL / CFB / CBB / Golf / WCBB are first-class in edge scoring and retrain joins."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from build_retrain_dataset import (  # noqa: E402
    has_dated_step8_snapshot,
    load_step8_dated_snapshot,
    load_step8_sport,
)
from step7b_edge_score import SPORT_ALIASES, _norm_sport, resolve_step7_path  # noqa: E402
from train_edge_model import (  # noqa: E402
    INACTIVE_SPORTS,
    _ALLOWED_TRAINING_SPORTS,
    _infer_sport_from_graded_filename,
    _normalize_sport_series,
    _sanitize_sport_with_hint,
)
import pandas as pd  # noqa: E402


def test_training_allowlist_includes_football_and_hoops():
    for sp in ("NFL", "CFB", "CBB", "WNBA", "TENNIS", "GOLF"):
        assert sp in _ALLOWED_TRAINING_SPORTS
    assert "CBB" not in INACTIVE_SPORTS


def test_sanitize_keeps_nfl_in_mixed_workbook():
    s = pd.Series(["NFL", "CFB", "WNBA", "13.0"])
    out = _sanitize_sport_with_hint(s, "NBA")
    assert list(out) == ["NFL", "CFB", "WNBA", "NBA"]


def test_normalize_pools_aliases():
    s = pd.Series(["NFLP", "CFB1H", "WCBB", "Golf"])
    out = _normalize_sport_series(s)
    assert list(out) == ["NFL", "CFB", "CBB", "GOLF"]


def test_graded_filename_inference():
    assert _infer_sport_from_graded_filename(Path("graded_nfl_2026-09-24.xlsx")) == "NFL"
    assert _infer_sport_from_graded_filename(Path("graded_cfb_2026-09-24.xlsx")) == "CFB"
    assert _infer_sport_from_graded_filename(Path("graded_cbb_2026-03-01.xlsx")) == "CBB"
    assert _infer_sport_from_graded_filename(Path("graded_golf_2026-04-12.xlsx")) == "GOLF"
    assert _infer_sport_from_graded_filename(Path("graded_wnba_2026-06-01.xlsx")) == "WNBA"


def test_step7b_aliases_and_feature_sport():
    for sp in ("NFL", "CFB", "CBB", "GOLF", "NFLP", "CFB1H", "WCBB"):
        assert sp in SPORT_ALIASES
    assert _norm_sport("NFL") == "NFL"
    assert _norm_sport("NFLP") == "NFL"
    assert _norm_sport("CFB1H") == "CFB"
    assert _norm_sport("WCBB") == "CBB"
    assert _norm_sport("Golf") == "GOLF"


def test_step7b_resolves_dated_nfl_cfb_cbb(tmp_path: Path):
    d = tmp_path / "outputs" / "2026-09-24"
    (d / "nfl").mkdir(parents=True)
    (d / "cfb").mkdir()
    (d / "cbb").mkdir()
    (d / "golf").mkdir()
    for rel in (
        "nfl/step7_nfl_ranked.xlsx",
        "cfb/step6_ranked_cfb.xlsx",
        "cbb/step6_ranked_cbb.xlsx",
        "golf/step7_golf_ranked.xlsx",
    ):
        p = d / rel
        p.write_bytes(b"PK\x03\x04" + b"\x00" * 64)
    assert resolve_step7_path(tmp_path, "NFL", "2026-09-24") == d / "nfl" / "step7_nfl_ranked.xlsx"
    assert resolve_step7_path(tmp_path, "CFB", "2026-09-24") == d / "cfb" / "step6_ranked_cfb.xlsx"
    assert resolve_step7_path(tmp_path, "CBB", "2026-09-24") == d / "cbb" / "step6_ranked_cbb.xlsx"
    assert resolve_step7_path(tmp_path, "Golf", "2026-09-24") == d / "golf" / "step7_golf_ranked.xlsx"


def test_retrain_step8_loaders_nfl_cfb_golf(tmp_path: Path):
    d = tmp_path / "outputs" / "2026-09-24"
    (d / "nfl").mkdir(parents=True)
    (d / "cfb").mkdir()
    (d / "golf").mkdir()
    pd.DataFrame({"player": ["N"], "rank_score": [1.0]}).to_excel(
        d / "nfl" / "step8_nfl_direction_clean.xlsx", index=False
    )
    pd.DataFrame({"player": ["C"], "rank_score": [3.0]}).to_excel(
        d / "cfb" / "step8_cfb_direction_clean.xlsx", index=False
    )
    pd.DataFrame({"player": ["G"], "rank_score": [2.0]}).to_csv(
        d / "golf" / "step8_golf_direction.csv", index=False
    )

    assert has_dated_step8_snapshot(tmp_path, "NFL", "2026-09-24")
    assert has_dated_step8_snapshot(tmp_path, "CFB", "2026-09-24")
    assert has_dated_step8_snapshot(tmp_path, "GOLF", "2026-09-24")
    df_nfl, static_nfl = load_step8_dated_snapshot(tmp_path, "NFL", "2026-09-24")
    assert df_nfl is not None and not static_nfl
    assert "N" in set(df_nfl["player"].astype(str))
    df_cfb, static_cfb = load_step8_dated_snapshot(tmp_path, "CFB", "2026-09-24")
    assert df_cfb is not None and not static_cfb
    df_golf, static_golf = load_step8_dated_snapshot(tmp_path, "GOLF", "2026-09-24")
    assert df_golf is not None and not static_golf
    assert load_step8_sport(tmp_path, "NFL") is None
