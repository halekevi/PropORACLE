"""Gated Goblin / Standard / Mixed construction tracks stay off card win-rate."""
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
from utils.gated_construction_tracks import (  # noqa: E402
    GATED_GOBLIN,
    GATED_MIXED,
    GATED_STANDARD,
    construction_track,
    efficiency_track,
)
from utils.income_tracks import is_card_track  # noqa: E402


def test_construction_track_splits_families():
    assert (
        construction_track(
            {"family": "goblin", "ticket_track": "goblin70", "id": "G3-1"},
            "NFL Goblin-70 Power 3",
        )
        == GATED_GOBLIN
    )
    assert (
        construction_track(
            {"family": "standard", "ticket_track": "standard_ev", "id": "S3-1"},
            "X-Sport Standard Power 3",
        )
        == GATED_STANDARD
    )
    assert (
        construction_track(
            {"family": "mix", "ticket_track": "mixed_ev", "id": "M3-1"},
            "X-Sport Mixed Power 3",
        )
        == GATED_MIXED
    )
    # YOLO must not land in gated construction.
    assert (
        construction_track(
            {
                "family": "standard",
                "ticket_track": "standard_yolo",
                "exclude_from_winrate": True,
                "id": "YS3-1",
            },
            "YOLO Standard Power 3",
        )
        is None
    )
    assert (
        efficiency_track(
            {
                "family": "standard",
                "ticket_track": "standard_yolo",
                "exclude_from_winrate": True,
                "id": "YS3-1",
            },
            "YOLO Standard Power 3",
        )
        == "standard_yolo"
    )


def test_gated_tracks_off_income_card():
    assert not is_card_track(GATED_GOBLIN)
    assert not is_card_track(GATED_STANDARD)
    assert not is_card_track(GATED_MIXED)


def test_build_gated_construction_grade_history_records_counts():
    payload = {
        "date": "2026-09-21",
        "groups": [
            {
                "group_name": "NFL Goblin-70 Power 3",
                "tickets": [
                    {
                        "id": "G3-1",
                        "family": "goblin",
                        "product": "Power",
                        "play": "Power",
                        "n_legs": 3,
                        "ticket_track": "goblin70",
                        "legs": [
                            {
                                "player": f"G{i}",
                                "prop": "rushing_yards",
                                "side": "OVER",
                                "direction": "OVER",
                                "line": 29.5,
                                "pick_type": "Goblin",
                                "sport": "NFL",
                            }
                            for i in range(3)
                        ],
                    }
                ],
            },
            {
                "group_name": "X-Sport Mixed Power 3",
                "tickets": [
                    {
                        "id": "M3-1",
                        "family": "mix",
                        "product": "Power",
                        "play": "Power",
                        "n_legs": 3,
                        "ticket_track": "mixed_ev",
                        "legs": [
                            {
                                "player": "A",
                                "prop": "saves",
                                "direction": "OVER",
                                "line": 2.5,
                                "pick_type": "Goblin",
                                "sport": "Soccer",
                            },
                            {
                                "player": "B",
                                "prop": "shots",
                                "direction": "OVER",
                                "line": 1.5,
                                "pick_type": "Standard",
                                "sport": "Soccer",
                            },
                            {
                                "player": "C",
                                "prop": "shots",
                                "direction": "OVER",
                                "line": 1.5,
                                "pick_type": "Goblin",
                                "sport": "Soccer",
                            },
                        ],
                    }
                ],
            },
        ],
    }

    def _hit_grade(*_a, **_k):
        return "HIT"

    with (
        patch.object(te, "_load_actuals_indices", return_value={}),
        patch.object(te, "_injury_dnp_by_sport", return_value={}),
        patch.object(te, "_match_leg_to_row_multi", return_value=None),
        patch.object(te, "_leg_grade", side_effect=_hit_grade),
        patch.object(te, "_resolve_void_pending_if_injury_dnp", side_effect=lambda g, *a, **k: g),
    ):
        recs = te._build_gated_construction_grade_history_records(
            payload, "2026-09-21", {}, ["2026-09-21"]
        )

    by = {r["track"]: r for r in recs}
    assert by[GATED_GOBLIN]["n_slips"] == 1
    assert by[GATED_GOBLIN]["wins"] == 1
    assert by[GATED_GOBLIN]["exclude_from_winrate"] is True
    assert by[GATED_MIXED]["n_slips"] == 1
    assert by[GATED_MIXED]["wins"] == 1
    assert GATED_STANDARD not in by
