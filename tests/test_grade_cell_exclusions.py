from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT, ROOT / "scripts"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from utils.grade_cell_exclusions import (  # noqa: E402
    REASON_FACEOFF,
    REASON_PASS_ATT,
    REASON_TENNIS_SERVE,
    drop_excluded,
    exclusion_reason,
    exclusion_reasons,
)


def test_tennis_serve_stats_excluded_through_cutoff():
    assert exclusion_reason("Tennis", "Aces", 0.0, "2026-09-30") == REASON_TENNIS_SERVE
    assert exclusion_reason("Tennis", "double_faults", 0.0, "2026-08-02") == REASON_TENNIS_SERVE
    assert exclusion_reason("Tennis", "1st Set Aces", 0.0, "2026-09-17") == REASON_TENNIS_SERVE


def test_tennis_serve_real_stat_after_cutoff_is_kept():
    assert exclusion_reason("Tennis", "Aces", 7.0, "2026-10-05") == ""
    # A real 0 double faults after the zero-fill era is a legitimate result.
    assert exclusion_reason("Tennis", "Double Faults", 0.0, "2026-10-05") == ""


def test_tennis_serve_undated_falls_back_to_zero_or_missing():
    assert exclusion_reason("Tennis", "Aces", 0.0, None) == REASON_TENNIS_SERVE
    assert exclusion_reason("Tennis", "Aces", np.nan, None) == REASON_TENNIS_SERVE
    assert exclusion_reason("Tennis", "Aces", 5.0, None) == ""


def test_tennis_games_won_not_excluded():
    assert exclusion_reason("Tennis", "Total Games Won", 0.0, "2026-09-01") == ""
    assert exclusion_reason("Tennis", "Total Games", 22.0, "2026-09-01") == ""


def test_football_pass_attempts_only_when_zero():
    assert exclusion_reason("CFB", "Pass Attempts", 0.0, "2026-10-10") == REASON_PASS_ATT
    assert exclusion_reason("NFL", "pass_attempts", 0.0, "2026-10-04") == REASON_PASS_ATT
    assert exclusion_reason("NFL", "Pass Attempts", 34.0, "2026-10-04") == ""
    assert exclusion_reason("NFL", "Rush Attempts", 0.0, "2026-10-04") == ""


def test_nhl_faceoffs_excluded_regardless_of_actual():
    assert exclusion_reason("NHL", "Faceoffs Won", 55.0, "2026-10-10") == REASON_FACEOFF
    assert exclusion_reason("NHL", "faceoffs_won", 8.0, "2026-10-10") == REASON_FACEOFF
    assert exclusion_reason("NHL", "Shots On Goal", 3.0, "2026-10-10") == ""


def test_vectorised_matches_scalar_and_drop_excluded():
    df = pd.DataFrame(
        [
            {"sport": "Tennis", "prop": "Aces", "actual_value": 0, "_date": "2026-09-01"},
            {"sport": "Tennis", "prop": "Aces", "actual_value": 9, "_date": "2026-10-09"},
            {"sport": "CFB", "prop": "Pass Attempts", "actual_value": 0, "_date": "2026-10-10"},
            {"sport": "NHL", "prop": "Faceoffs Won", "actual_value": 40, "_date": "2026-10-10"},
            {"sport": "NHL", "prop": "Saves", "actual_value": 25, "_date": "2026-10-10"},
        ]
    )
    vec = exclusion_reasons(df).tolist()
    sc = [
        exclusion_reason(r["sport"], r["prop"], r["actual_value"], r["_date"])
        for _, r in df.iterrows()
    ]
    assert vec == sc
    assert len(drop_excluded(df)) == 2


def test_football_zero_fill_skips_pass_attempts():
    from grading.slate_grader import _FOOTBALL_ZERO_FILL_PROPS

    assert "pass attempts" not in _FOOTBALL_ZERO_FILL_PROPS
    assert "rushing yards" in _FOOTBALL_ZERO_FILL_PROPS


def test_tennis_rows_from_match_does_not_default_sets_won():
    from fetch_tennis_actuals import rows_from_match

    rows = rows_from_match({"player": "A B", "games_won": 12, "match_total_games": 22})
    props = {r["prop_type"] for r in rows}
    assert "sets won" not in props
    assert "aces" not in props and "double faults" not in props
    rows2 = rows_from_match({"player": "A B", "sets_won": 2, "aces": 4})
    assert {"sets won", "aces"} <= {r["prop_type"] for r in rows2}


def test_football_att_from_c_att():
    from fetch_football_actuals import _att_from_stat_map

    assert _att_from_stat_map({"C/ATT": "22/31"}) == 31.0
    assert _att_from_stat_map({"ATT": "12"}) == 12.0
    assert _att_from_stat_map({}) is None
