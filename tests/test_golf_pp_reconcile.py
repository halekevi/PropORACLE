"""Golf PP chart reconcile (fixture path)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from utils.golf_pp_reconcile import (
    apply_pp_reconcile,
    compare_chart_values,
    reconcile_leg,
)


def test_compare_exact_match_and_mismatch():
    assert compare_chart_values([11, 12, 11], [11, 12, 11]) == ("match", [])
    status, bad = compare_chart_values([10, 8, 11], [12, 9, 7, 14])
    assert status == "mismatch"
    assert 0 in bad


def test_reconcile_course_filter_vs_all(tmp_path: Path):
    rounds = pd.DataFrame(
        [
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-05", "round": 1, "course_id": "port_royal", "pars": 12},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-04", "round": 1, "course_id": "port_royal", "pars": 9},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-03", "round": 1, "course_id": "port_royal", "pars": 7},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-02", "round": 1, "course_id": "port_royal", "pars": 14},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-01", "round": 1, "course_id": "other", "pars": 10},
        ]
    )
    fixture_leg = {
        "player": "Andrew Novak",
        "prop_type": "Pars",
        "filter": "course",
        "course_id": "port_royal",
        "values": [12, 9, 7, 14],
        "birdies_or_better": False,
    }
    got = reconcile_leg(rounds, prop_type="Pars", line=11.0, fixture_leg=fixture_leg)
    assert got["golf_pp_reconcile"] == "match"

    # Wrong window (all-courses) vs a Port Royal-only PP chart that disagrees.
    # Simulate the live bug: all-courses L5 was 10/8/11/11/7 while PP showed 12/9/7/14.
    rounds_mixed = pd.DataFrame(
        [
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-05", "round": 1, "course_id": "other", "pars": 10},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-04", "round": 1, "course_id": "other", "pars": 8},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-03", "round": 1, "course_id": "other", "pars": 11},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-02", "round": 1, "course_id": "other", "pars": 11},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-01", "round": 1, "course_id": "other", "pars": 7},
        ]
    )
    all_leg = {
        "player": "Andrew Novak",
        "prop_type": "Pars",
        "filter": "all",
        "course_id": "",
        "values": [12, 9, 7, 14],
        "birdies_or_better": False,
    }
    got2 = reconcile_leg(rounds_mixed, prop_type="Pars", line=11.0, fixture_leg=all_leg)
    assert got2["golf_pp_reconcile"] == "mismatch"


def test_apply_hold_on_mismatch(tmp_path: Path):
    fixture = {
        "legs": [
            {
                "player": "Andrew Novak",
                "prop_type": "Pars",
                "line": 11.0,
                "filter": "course",
                "course_id": "port_royal",
                "values": [12, 9, 7, 14],
            }
        ]
    }
    path = tmp_path / "pp.json"
    path.write_text(json.dumps(fixture), encoding="utf-8")
    props = pd.DataFrame(
        [{"player": "Andrew Novak", "prop_type": "Pars", "line": 11.0, "void_reason": ""}]
    )
    # Wrong pars at Port Royal → mismatch hold
    rounds = pd.DataFrame(
        [
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-05", "round": 1, "course_id": "port_royal", "pars": 10},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-04", "round": 1, "course_id": "port_royal", "pars": 8},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-03", "round": 1, "course_id": "port_royal", "pars": 11},
            {"player_name": "Andrew Novak", "player_key": "andrew novak",
             "tournament_date": "2026-10-02", "round": 1, "course_id": "port_royal", "pars": 11},
        ]
    )
    out = apply_pp_reconcile(props, rounds, fixture_path=path)
    assert out.loc[0, "golf_pp_reconcile"] == "mismatch"
    assert out.loc[0, "void_reason"] == "golf_pp_mismatch"


def test_no_pp_chart_is_flag_only(tmp_path: Path):
    path = tmp_path / "empty.json"
    path.write_text(json.dumps({"legs": []}), encoding="utf-8")
    props = pd.DataFrame(
        [{"player": "Nobody", "prop_type": "Pars", "line": 11.0, "void_reason": ""}]
    )
    out = apply_pp_reconcile(props, pd.DataFrame(), fixture_path=path)
    assert out.loc[0, "golf_pp_reconcile"] == "no_pp_chart"
    assert out.loc[0, "void_reason"] == ""
