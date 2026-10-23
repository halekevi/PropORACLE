from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from ticket_leg_index import union_ticket_faces_into_slate  # noqa: E402


def test_union_appends_standard_face_missing_from_board(tmp_path, monkeypatch):
    date = "2026-08-28"
    runs = tmp_path / "ui_runner" / "data" / "ticket_runs" / date / "r1"
    runs.mkdir(parents=True)
    payload = {
        "date": date,
        "groups": [
            {
                "sport": "TENNIS",
                "tickets": [
                    {
                        "ticket_id": "t1",
                        "legs": [
                            {
                                "sport": "TENNIS",
                                "player": "Tristan Schoolkate",
                                "prop_type": "Total Games Won",
                                "line": 12.5,
                                "direction": "OVER",
                                "pick_type": "standard",
                            }
                        ],
                    }
                ],
            }
        ],
    }
    (runs / "tickets.json").write_text(json.dumps(payload), encoding="utf-8")
    board = pd.DataFrame(
        [
            {
                "player": "Tristan Schoolkate",
                "prop_type": "Total Games Won",
                "line": 13.5,
                "direction": "OVER",
                "pick_type": "goblin",
                "sport": "TENNIS",
            }
        ]
    )
    monkeypatch.delenv("PROPORACLE_TICKET_RUN_ROOTS", raising=False)
    out = union_ticket_faces_into_slate(board, date_str=date, sport="TENNIS", repo=tmp_path)
    assert len(out) == 2
    added = out[out["from_ticket_face"] == 1]
    assert len(added) == 1
    assert float(added.iloc[0]["line"]) == 12.5
    assert str(added.iloc[0]["pick_type"]).lower() == "standard"
