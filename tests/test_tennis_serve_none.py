"""Missing ESPN serve stats must stay None, not 0.0."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

_REPO = Path(__file__).resolve().parents[1]
_TENNIS = _REPO / "Sports" / "Tennis" / "scripts"
sys.path.insert(0, str(_TENNIS))
sys.path.insert(0, str(_REPO))

from tennis_grader import _grade, _opt_float  # noqa: E402
from tennis_shared import fill_missing_serve_from_sackmann, lookup_sackmann_match  # noqa: E402


def test_opt_float_none_and_nan_stay_none():
    assert _opt_float(None) is None
    assert _opt_float("") is None
    assert _opt_float(float("nan")) is None
    assert _opt_float(0) == 0.0
    assert _opt_float("7") == 7.0


def test_grade_none_is_void_zero_is_real_miss():
    assert _grade("OVER", 2.5, None)[0] == "VOID"
    assert _grade("OVER", 2.5, 0.0)[0] == "MISS"
    assert _grade("OVER", 2.5, 4.0)[0] == "HIT"
    assert _grade("UNDER", 2.5, 0.0)[0] == "HIT"


def test_lookup_sackmann_prefers_games_won_hint():
    idx = {
        "ann li": [
            {
                "date": "20260810",
                "match_num": 1,
                "round": "R32",
                "games_won": 9.0,
                "aces": 1.0,
                "double_faults": 3.0,
            },
            {
                "date": "20260824",
                "match_num": 2,
                "round": "R16",
                "games_won": 13.0,
                "aces": 7.0,
                "double_faults": 2.0,
            },
        ]
    }
    rec = lookup_sackmann_match(idx, "Ann Li", date(2026, 8, 24), games_won_hint=13.0)
    assert rec is not None
    assert rec["aces"] == 7.0
    assert rec["double_faults"] == 2.0


def test_lookup_true_zero_aces_not_dropped():
    idx = {
        "ann li": [
            {
                "date": "20260824",
                "match_num": 1,
                "round": "R32",
                "games_won": 10.0,
                "aces": 0.0,
                "double_faults": 4.0,
            }
        ]
    }
    rec = lookup_sackmann_match(idx, "Ann Li", date(2026, 8, 24), games_won_hint=10.0)
    assert rec is not None
    assert rec["aces"] == 0.0


def test_lookup_serve_prefers_ta_match_date():
    from tennis_shared import lookup_serve_actual

    ta = {
        "ann li": [
            {
                "date": "20260824",
                "match_num": 1,
                "round": "R16",
                "games_won": 13.0,
                "aces": 8.0,
                "double_faults": 1.0,
            }
        ]
    }
    sack = {
        "ann li": [
            {
                "date": "20260810",
                "match_num": 1,
                "round": "R32",
                "games_won": 13.0,
                "aces": 1.0,
                "double_faults": 9.0,
            }
        ]
    }
    rec, src = lookup_serve_actual(
        "Ann Li", date(2026, 8, 24), games_won_hint=13.0, ta_index=ta, sack_index=sack
    )
    assert src == "tennis_abstract"
    assert rec is not None
    assert rec["aces"] == 8.0

    by_player = {
        "ann li": {
            "player": "Ann Li",
            "games_won": 13.0,
            "aces": None,
            "double_faults": None,
        }
    }
    idx = {
        "ann li": [
            {
                "date": "20260824",
                "match_num": 1,
                "round": "R16",
                "games_won": 13.0,
                "aces": 6.0,
                "double_faults": 1.0,
            }
        ]
    }
    n = fill_missing_serve_from_sackmann(
        by_player, date(2026, 8, 24), player_index=idx, ta_index={}
    )
    assert n == 1
    assert by_player["ann li"]["aces"] == 6.0
    by_player["ann li"]["aces"] = 9.0
    n2 = fill_missing_serve_from_sackmann(
        by_player, date(2026, 8, 24), player_index=idx, ta_index={}
    )
    assert n2 == 0
    assert by_player["ann li"]["aces"] == 9.0
