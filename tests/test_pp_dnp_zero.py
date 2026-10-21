"""PP chart DNP-as-zero holds (Jaren-style)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from utils.pp_dnp_zero import (
    VOID_REASON,
    apply_pp_dnp_zero_holds,
    check_pp_dnp_zeros,
)


def test_jaren_leading_zero_no_boxscore_row():
    chart = [
        {"value": 0, "date": "2026-04-10"},
        {"value": 4, "date": "2026-02-11"},
        {"value": 5, "date": "2026-02-09"},
        {"value": 4, "date": "2026-02-07"},
        {"value": 6, "date": "2026-02-02"},
    ]
    boxes = [
        {"game_date": "2026-02-11", "minutes": 22, "value": 4},
        {"game_date": "2026-02-09", "minutes": 25, "value": 5},
        {"game_date": "2026-02-07", "minutes": 25, "value": 4},
        {"game_date": "2026-02-02", "minutes": 31, "value": 6},
        {"game_date": "2026-01-30", "minutes": 34, "value": 9},
    ]
    hits = check_pp_dnp_zeros(chart, boxes)
    assert any(h.reason == "no_boxscore_row" and h.chart_date == "2026-04-10" for h in hits)


def test_zero_with_no_minutes_is_hit():
    chart = [{"value": 0, "date": "2026-04-10"}]
    boxes = [{"game_date": "2026-04-10", "minutes": 0, "value": 0}]
    hits = check_pp_dnp_zeros(chart, boxes)
    assert any(h.reason == "zero_no_minutes" for h in hits)


def test_real_zero_with_minutes_ok():
    chart = [{"value": 0, "date": "2026-03-04"}]
    boxes = [{"game_date": "2026-03-04", "minutes": 2, "value": 0}]
    assert check_pp_dnp_zeros(chart, boxes) == []


def test_value_only_leading_dnp_shape():
    # Card 0,4,5,4,6 vs played 4,5,4,6,9
    chart = [0, 4, 5, 4, 6]
    boxes = [{"value": v, "minutes": 20} for v in (4, 5, 4, 6, 9)]
    hits = check_pp_dnp_zeros(chart, boxes)
    assert any(h.reason == "leading_dnp_zero" for h in hits)


def test_apply_hold_from_fixture(tmp_path: Path):
    fixture = {
        "legs": [
            {
                "player": "Jaren Jackson",
                "prop_type": "Rebounds",
                "line": 6.0,
                "values": [0, 4, 5, 4, 6],
                "dates": [
                    "2026-04-10",
                    "2026-02-11",
                    "2026-02-09",
                    "2026-02-07",
                    "2026-02-02",
                ],
            }
        ]
    }
    path = tmp_path / "pp.json"
    path.write_text(json.dumps(fixture), encoding="utf-8")
    df = pd.DataFrame(
        [
            {
                "player": "Jaren Jackson",
                "prop_type": "Rebounds",
                "line": 6.0,
                "ESPN_ATHLETE_ID": "4277961",
                "stat_g1": 4,
                "stat_g2": 5,
                "stat_g3": 4,
                "stat_g4": 6,
                "stat_g5": 9,
                "final_bet_direction": "UNDER",
                "void_reason": "",
            }
        ]
    )
    # No DB con → uses stat_g fallback + dated chart still flags missing Apr 10
    # when boxes lack that date (stat_g fallback has no dates → value-only path).
    # Provide empty con substitute by skipping DB; value-only leading zero hits.
    out = apply_pp_dnp_zero_holds(df, boxscore_con=None, fixture_path=path)
    assert int(out.loc[0, "pp_shows_dnp_zero"]) == 1
    assert out.loc[0, "void_reason"] == VOID_REASON
    assert out.loc[0, "final_bet_direction"] == ""


def test_no_chart_skips_hold():
    df = pd.DataFrame(
        [
            {
                "player": "Nobody",
                "prop_type": "Rebounds",
                "line": 6.0,
                "void_reason": "",
                "final_bet_direction": "UNDER",
            }
        ]
    )
    out = apply_pp_dnp_zero_holds(df, fixture_path=Path("/nonexistent.json"))
    assert int(out.loc[0, "pp_shows_dnp_zero"]) == 0
    assert out.loc[0, "void_reason"] == ""
    assert out.loc[0, "final_bet_direction"] == "UNDER"
