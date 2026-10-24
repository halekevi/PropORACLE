"""YOLO grade_history side tracks stay off card win-rate."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))

import build_ticket_eval as te  # noqa: E402
from utils.ticket_70_pool import yolo_ticket_track  # noqa: E402


def test_yolo_ticket_track_splits_standard_vs_goblin():
    assert (
        yolo_ticket_track(
            {"exclude_from_winrate": True, "family": "standard", "id": "YS3-1"},
            "YOLO Standard Power 3",
        )
        == "standard_yolo"
    )
    assert (
        yolo_ticket_track(
            {"exclude_from_winrate": True, "family": "mix", "id": "Y6-1"},
            "YOLO Goblin-70 Power 6",
        )
        == "goblin70_yolo"
    )
    assert yolo_ticket_track({"id": "P3-1", "product": "Power"}, "X-Sport Goblin-70 Power 3") is None


def test_build_yolo_grade_history_records_counts():
    payload = {
        "date": "2026-09-21",
        "groups": [
            {
                "group_name": "YOLO Standard Power 3",
                "tickets": [
                    {
                        "id": "YS3-1",
                        "family": "standard",
                        "product": "Power",
                        "play": "Power",
                        "n_legs": 3,
                        "exclude_from_winrate": True,
                        "ticket_track": "standard_yolo",
                        "legs": [
                            {
                                "player": "A",
                                "prop": "saves",
                                "direction": "OVER",
                                "line": 2.5,
                                "sport": "Soccer",
                            },
                            {
                                "player": "B",
                                "prop": "saves",
                                "direction": "OVER",
                                "line": 2.5,
                                "sport": "Soccer",
                            },
                            {
                                "player": "C",
                                "prop": "saves",
                                "direction": "OVER",
                                "line": 2.5,
                                "sport": "Soccer",
                            },
                        ],
                    }
                ],
            },
            {
                "group_name": "YOLO Goblin-70 Power 4",
                "tickets": [
                    {
                        "id": "Y4-1",
                        "family": "mix",
                        "product": "Power",
                        "play": "Power",
                        "n_legs": 4,
                        "exclude_from_winrate": True,
                        "ticket_track": "goblin70_yolo",
                        "legs": [
                            {
                                "player": f"G{i}",
                                "prop": "points",
                                "direction": "OVER",
                                "line": 10.5,
                                "sport": "WNBA",
                            }
                            for i in range(4)
                        ],
                    }
                ],
            },
        ],
    }

    # Force every leg HIT so both slips win.
    def _hit_grade(*_a, **_k):
        return "HIT"

    with (
        patch.object(te, "_load_actuals_indices", return_value={}),
        patch.object(te, "_injury_dnp_by_sport", return_value={}),
        patch.object(te, "_match_leg_to_row_multi", return_value=None),
        patch.object(te, "_leg_grade", side_effect=_hit_grade),
        patch.object(te, "_resolve_void_pending_if_injury_dnp", side_effect=lambda g, *a, **k: g),
    ):
        recs = te._build_yolo_grade_history_records(payload, "2026-09-21", {}, ["2026-09-21"])

    by = {r["track"]: r for r in recs}
    assert by["standard_yolo"]["n_slips"] == 1
    assert by["standard_yolo"]["wins"] == 1
    assert by["standard_yolo"]["losses"] == 0
    assert by["standard_yolo"]["avg_legs"] == 3.0
    assert by["standard_yolo"]["exclude_from_winrate"] is True
    assert by["goblin70_yolo"]["n_slips"] == 1
    assert by["goblin70_yolo"]["wins"] == 1
    assert by["goblin70_yolo"]["avg_legs"] == 4.0
