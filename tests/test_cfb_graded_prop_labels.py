"""CFB graded_props must carry real prop labels from Box Raw prop_type_norm."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))

from grading.build_grades_html import load_graded, prop_row_for_api  # noqa: E402


def test_cfb_box_raw_prop_survives_to_api_row(tmp_path):
    import pandas as pd

    box = pd.DataFrame(
        [
            {
                "ticket_id": "t1",
                "player": "Dante Moore",
                "prop_type_norm": "Pass Yards",
                "pick_type": "Goblin",
                "line": 174.5,
                "bet_direction": "OVER",
                "tier": "A",
                "actual": 210.0,
                "result": "HIT",
                "margin": 35.5,
            }
        ]
    )
    path = tmp_path / "graded_cfb_2026-09-18.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        pd.DataFrame({"note": ["summary"]}).to_excel(xl, sheet_name="Summary", index=False)
        box.to_excel(xl, sheet_name="Box Raw", index=False)

    rows = load_graded(path, "cfb")
    assert len(rows) == 1
    assert rows[0].get("Prop Type") == "Pass Yards"
    api = prop_row_for_api(rows[0], "CFB")
    assert api is not None
    assert api["prop"] == "Pass Yards"
    assert api["prop"] != "—"


def test_blank_prop_row_dropped_not_dashed():
    api = prop_row_for_api(
        {
            "player": "Ghost",
            "Prop Type": "",
            "pick_type": "Goblin",
            "line": 1.5,
            "Direction": "OVER",
            "result": "HIT",
            "actual": 2.0,
        },
        "CFB",
    )
    assert api is None
