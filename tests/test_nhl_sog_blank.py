"""Blank NHL Shots On Goal cells stay missing. Explicit 0 stays 0."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))

from fetch_actuals import parse_nhl_boxscore  # noqa: E402


def _box() -> dict:
    return {
        "boxscore": {
            "players": [
                {
                    "team": {"abbreviation": "BOS"},
                    "statistics": [
                        {
                            "labels": ["G", "A", "S", "TOI"],
                            "athletes": [
                                {
                                    "athlete": {"displayName": "Blank Shot"},
                                    "stats": ["1", "0", "--", "18:00"],
                                },
                                {
                                    "athlete": {"displayName": "Zero Shot"},
                                    "stats": ["0", "0", "0", "12:00"],
                                },
                                {
                                    "athlete": {"displayName": "Three Shot"},
                                    "stats": ["0", "1", "3", "20:11"],
                                },
                            ],
                        }
                    ],
                }
            ]
        }
    }


def test_shootout_label_does_not_replace_shots_total():
    box = _box()
    group = box["boxscore"]["players"][0]["statistics"][0]
    group["labels"] = ["G", "A", "SOG", "S", "TOI"]
    group["keys"] = ["goals", "assists", "shootoutGoals", "shotsTotal", "timeOnIce"]
    group["athletes"] = [
        {
            "athlete": {"displayName": "Real Shots"},
            "stats": ["0", "0", "0", "4", "18:00"],
        }
    ]
    rows = parse_nhl_boxscore(box)
    sog = [r for r in rows if r["prop_type"] == "Shots On Goal"]
    assert len(sog) == 1
    assert sog[0]["actual"] == 4.0


def test_blank_sog_is_not_written_as_zero():
    rows = parse_nhl_boxscore(_box())
    sog = {
        r["player"]: r["actual"]
        for r in rows
        if r["prop_type"] == "Shots On Goal"
    }
    assert "Blank Shot" not in sog
    assert sog["Zero Shot"] == 0.0
    assert sog["Three Shot"] == 3.0
