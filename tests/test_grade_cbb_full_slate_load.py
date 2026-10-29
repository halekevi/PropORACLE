from pathlib import Path

import pandas as pd

from scripts.grading.grade_cbb_full_slate import _load_table


def test_load_table_prefers_all_sheet(tmp_path: Path):
    path = tmp_path / "step8.xlsx"
    banner = pd.DataFrame([["PropOracle CBB", "banner"]])
    board = pd.DataFrame(
        [{"player": "Markus Burton", "prop_type": "Points", "line": "12.5", "direction": "OVER"}]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        banner.to_excel(xl, sheet_name="Banner", index=False, header=False)
        board.to_excel(xl, sheet_name="ALL", index=False)
    df = _load_table(path)
    assert list(df["player"]) == ["Markus Burton"]
    assert list(df["prop_type"]) == ["Points"]
