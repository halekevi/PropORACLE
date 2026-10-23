"""Played-but-no-stat football props settle as 0, matching PrizePicks."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "scripts"), str(ROOT / "scripts" / "grading")]

from slate_grader import apply_actuals  # noqa: E402


def _slate(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_rush_over_is_a_miss_when_he_passed_and_did_not_rush(tmp_path):
    act = tmp_path / "actuals_cfb_2026-09-05.csv"
    pd.DataFrame(
        [
            {
                "player": "Gunner Stockton",
                "team": "UGA",
                "prop_type": "passing_yards",
                "actual": 202,
            }
        ]
    ).to_csv(act, index=False)
    df = _slate(
        [
            {
                "player": "Gunner Stockton",
                "team": "UGA",
                "prop_type_norm": "Rush Yards",
                "line": 36.5,
                "bet_direction": "OVER",
            }
        ]
    )
    out = apply_actuals(df, str(act))
    assert float(out.iloc[0]["actual"]) == 0.0
    assert out.iloc[0]["result"] == "MISS"
    assert "NO_ACTUAL" not in str(out.iloc[0]["void_reason_grade"])


def test_under_is_a_hit_when_the_stat_is_missing_and_he_played(tmp_path):
    act = tmp_path / "actuals_cfb_2026-09-05.csv"
    pd.DataFrame(
        [
            {
                "player": "Gunner Stockton",
                "team": "UGA",
                "prop_type": "passing_yards",
                "actual": 202,
            }
        ]
    ).to_csv(act, index=False)
    df = _slate(
        [
            {
                "player": "Gunner Stockton",
                "team": "UGA",
                "prop_type_norm": "Rush Yards",
                "line": 10.5,
                "bet_direction": "UNDER",
            }
        ]
    )
    out = apply_actuals(df, str(act))
    assert float(out.iloc[0]["actual"]) == 0.0
    assert out.iloc[0]["result"] == "HIT"


def test_nfl_under_zero_without_snaps_voids(tmp_path):
    """NFL Standard UNDER actual=0 needs snap/play evidence — silent zero is VOID."""
    act = tmp_path / "actuals_nfl_2026-09-05.csv"
    pd.DataFrame(
        [
            {
                "player": "Quiet Zero",
                "team": "KC",
                "prop_type": "receiving_yards",
                "actual": 0,
            }
        ]
    ).to_csv(act, index=False)
    df = _slate(
        [
            {
                "player": "Quiet Zero",
                "team": "KC",
                "prop_type_norm": "Rec Yards",
                "line": 24.5,
                "bet_direction": "UNDER",
                "pick_type": "Standard",
            }
        ]
    )
    out = apply_actuals(df, str(act))
    assert float(out.iloc[0]["actual"]) == 0.0
    assert out.iloc[0]["result"] == "VOID"
    assert "NFL_UNDER_ZERO_NO_SNAPS" in str(out.iloc[0]["void_reason_grade"])


def test_nfl_under_zero_with_snaps_is_hit(tmp_path):
    act = tmp_path / "actuals_nfl_2026-09-05.csv"
    pd.DataFrame(
        [
            {
                "player": "Targeted Zero",
                "team": "BUF",
                "prop_type": "receiving_yards",
                "actual": 0,
                "snaps": 22,
            }
        ]
    ).to_csv(act, index=False)
    df = _slate(
        [
            {
                "player": "Targeted Zero",
                "team": "BUF",
                "prop_type_norm": "Rec Yards",
                "line": 24.5,
                "bet_direction": "UNDER",
                "pick_type": "Standard",
            }
        ]
    )
    out = apply_actuals(df, str(act))
    assert float(out.iloc[0]["actual"]) == 0.0
    assert out.iloc[0]["result"] == "HIT"


def test_nfl_under_zero_from_other_box_stat_is_hit(tmp_path):
    """Played (other offensive stat present) → actual 0 UNDER is a real HIT."""
    act = tmp_path / "actuals_nfl_2026-09-05.csv"
    pd.DataFrame(
        [
            {
                "player": "Gunner Stockton",
                "team": "KC",
                "prop_type": "passing_yards",
                "actual": 202,
            }
        ]
    ).to_csv(act, index=False)
    df = _slate(
        [
            {
                "player": "Gunner Stockton",
                "team": "KC",
                "prop_type_norm": "Rush Yards",
                "line": 10.5,
                "bet_direction": "UNDER",
            }
        ]
    )
    out = apply_actuals(df, str(act))
    assert float(out.iloc[0]["actual"]) == 0.0
    assert out.iloc[0]["result"] == "HIT"


def test_no_box_row_stays_void(tmp_path):
    act = tmp_path / "actuals_cfb_2026-09-05.csv"
    pd.DataFrame(
        [
            {
                "player": "Someone Else",
                "team": "UGA",
                "prop_type": "passing_yards",
                "actual": 100,
            }
        ]
    ).to_csv(act, index=False)
    df = _slate(
        [
            {
                "player": "Tony Kinsler",
                "team": "UGA",
                "prop_type_norm": "Rec Yards",
                "line": 17.5,
                "bet_direction": "OVER",
            }
        ]
    )
    out = apply_actuals(df, str(act))
    assert out.iloc[0]["result"] == "VOID"
    assert out.iloc[0]["void_reason_grade"] == "NOT_ON_GAME_ROSTER"


def test_posted_rush_actual_is_not_replaced_with_zero(tmp_path):
    act = tmp_path / "actuals_cfb_2026-09-05.csv"
    pd.DataFrame(
        [
            {
                "player": "Gunner Stockton",
                "team": "UGA",
                "prop_type": "passing_yards",
                "actual": 202,
            },
            {
                "player": "Gunner Stockton",
                "team": "UGA",
                "prop_type": "rushing_yards",
                "actual": 40,
            },
        ]
    ).to_csv(act, index=False)
    df = _slate(
        [
            {
                "player": "Gunner Stockton",
                "team": "UGA",
                "prop_type_norm": "Rush Yards",
                "line": 36.5,
                "bet_direction": "OVER",
            }
        ]
    )
    out = apply_actuals(df, str(act))
    assert float(out.iloc[0]["actual"]) == 40.0
    assert out.iloc[0]["result"] == "HIT"
