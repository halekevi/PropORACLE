from pathlib import Path

import pandas as pd

from scripts.check_cbb_opener_graded import check_opener_graded, row_is_nd_villanova
from scripts.grading.grade_cbb_full_slate import grade_row


def test_nd_villanova_row_match():
    row = pd.Series(
        {
            "player": "Markus Burton",
            "team": "Notre Dame",
            "opponent": "Villanova",
            "prop_type": "Points",
        }
    )
    assert row_is_nd_villanova(row) is True
    other = pd.Series({"player": "X", "team": "Florida", "opponent": "Miami"})
    assert row_is_nd_villanova(other) is False


def test_push_and_void_are_handled():
    assert grade_row(10.5, 10.5, "OVER") == "PUSH"
    assert grade_row(float("nan"), 10.5, "OVER") == "VOID"
    assert grade_row(12.0, 10.5, "OVER") == "HIT"
    assert grade_row(9.0, 10.5, "OVER") == "MISS"


def test_check_opener_graded_ok(tmp_path: Path):
    date_dir = tmp_path / "outputs" / "2026-11-01"
    date_dir.mkdir(parents=True)
    path = date_dir / "graded_cbb_2026-11-01.xlsx"
    df = pd.DataFrame(
        [
            {
                "player": "Markus Burton",
                "team": "Notre Dame",
                "opponent": "Villanova",
                "prop_type": "Points",
                "line_num": 12.5,
                "actual_value": 18.0,
                "actual_status": "OK",
                "result": "HIT",
            },
            {
                "player": "Wooga Poplar",
                "team": "Villanova",
                "opponent": "Notre Dame",
                "prop_type": "Rebounds",
                "line_num": 4.5,
                "actual_value": 4.5,
                "actual_status": "OK",
                "result": "PUSH",
            },
            {
                "player": "Sit",
                "team": "Notre Dame",
                "opponent": "Villanova",
                "prop_type": "Points",
                "line_num": 8.5,
                "actual_value": float("nan"),
                "actual_status": "NO_ACTUAL_FOUND",
                "result": "VOID",
            },
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        df.to_excel(xl, sheet_name="Box Raw", index=False)
    report = check_opener_graded("2026-11-01", root=tmp_path)
    assert report["ok"] is True
    assert report["n_opener"] == 3
    assert report["n_with_actuals"] == 2
    assert report["result_counts"]["HIT"] == 1
    assert report["result_counts"]["PUSH"] == 1
    assert report["result_counts"]["VOID"] == 1


def test_check_opener_graded_missing_file(tmp_path: Path):
    report = check_opener_graded("2026-11-01", root=tmp_path)
    assert report["ok"] is False
    assert "missing graded_cbb" in report["errors"][0]
