"""Soccer missing actual must VOID — never coerce NaN/None into a MISS."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "Sports" / "Soccer" / "scripts"))

from soccer_grader import grade_prop  # noqa: E402


def test_none_actual_is_void_not_miss():
    result, reason = grade_prop(None, 12.5, "OVER")
    assert result == "VOID"
    assert reason == "no_actuals"


def test_nan_actual_is_void_not_miss():
    result, reason = grade_prop(float("nan"), 12.5, "OVER")
    assert result == "VOID"
    assert reason == "no_actuals"
    result2, _ = grade_prop(np.nan, 4.5, "OVER")
    assert result2 == "VOID"


def test_real_zero_still_grades_miss_on_over():
    result, _ = grade_prop(0.0, 12.5, "OVER")
    assert result == "MISS"


def test_nan_line_is_void():
    result, reason = grade_prop(3.0, float("nan"), "OVER")
    assert result == "VOID"
    assert reason == "bad_line"
    assert not math.isnan(3.0)  # sanity: actual itself is fine
