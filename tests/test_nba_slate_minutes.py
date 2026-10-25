"""Tonight starter / slate minutes — formula and attach, no live ESPN."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from utils.nba_slate_minutes import (
    apply_pre_tip_slate_minutes,
    attach_slate_minutes_columns,
    classify_injury_status,
    game_window,
    load_pre_tip_lookup,
    project_slate_minutes,
)


def test_out_is_zero():
    m, lo, hi, note = project_slate_minutes(
        role_mpg=32.0, tonight="projected", injury="OUT"
    )
    assert m == 0.0 and lo == 0.0 and hi == 0.0
    assert note == "out"


def test_projected_starter_floor_and_b2b():
    m, _, _, note = project_slate_minutes(
        role_mpg=22.0, tonight="projected", injury="", b2b=True
    )
    assert "projected" in note and "b2b" in note
    # floor 26, then * 0.90
    assert abs(m - 23.4) < 0.05


def test_questionable_haircut():
    m, _, _, note = project_slate_minutes(
        role_mpg=30.0, tonight="projected", injury="QUESTIONABLE"
    )
    assert abs(m - 24.0) < 0.05
    assert "questionable" in note


def test_star_out_bump_caps():
    m, _, _, note = project_slate_minutes(
        role_mpg=36.0, tonight="confirmed", injury="", teammate_star_out=True
    )
    assert m == 38.0
    assert "star_out_bump" in note


def test_bench_capped():
    m, _, _, _ = project_slate_minutes(
        role_mpg=30.0, tonight="bench", injury=""
    )
    assert m == 24.0


def test_classify_injury():
    assert classify_injury_status("Out") == "OUT"
    assert classify_injury_status("Day-To-Day") == "QUESTIONABLE"
    assert classify_injury_status("Doubtful") == "DOUBTFUL"
    assert classify_injury_status("") == ""


def test_attach_uses_snapshot_not_network():
    snap = {
        "confirmed": {},
        "projected": {"BKN": ["Michael Porter Jr."], "LAL": ["Austin Reaves", "Luka Doncic"]},
        "injuries": {"LAL": {"lebron james": "OUT"}},
        "b2b_teams": ["LAL"],
    }
    df = pd.DataFrame(
        [
            {"player": "Austin Reaves", "team": "LAL", "role_mpg": 34.0, "b2b_flag": True},
            {"player": "LeBron James", "team": "LAL", "role_mpg": 32.0, "b2b_flag": True},
            {"player": "Dalton Knecht", "team": "LAL", "role_mpg": 18.0, "b2b_flag": False},
            {"player": "Michael Porter", "team": "BKN", "role_mpg": 32.0, "b2b_flag": False},
        ]
    )
    out = attach_slate_minutes_columns(df, snapshot=snap, write_log=False)
    reaves = out.loc[out["player"] == "Austin Reaves"].iloc[0]
    assert reaves["tonight_starter"] == "projected"
    assert reaves["tonight_starter_source"] == "espn_depth"
    assert float(reaves["slate_minutes"]) < 34.0  # B2B haircut
    lebron = out.loc[out["player"] == "LeBron James"].iloc[0]
    assert lebron["tonight_starter"] == "out"
    assert float(lebron["slate_minutes"]) == 0.0
    knecht = out.loc[out["player"] == "Dalton Knecht"].iloc[0]
    assert knecht["tonight_starter"] == "bench"
    porter = out.loc[out["player"] == "Michael Porter"].iloc[0]
    assert porter["tonight_starter"] == "projected"


def test_game_window_tminus_before_tip():
    tip = datetime(2026, 10, 25, 21, 0, tzinfo=timezone.utc)
    assert game_window(tip, tip - timedelta(hours=4)) == "early"
    assert game_window(tip, tip - timedelta(minutes=30)) == "tminus30"
    assert game_window(tip, tip + timedelta(minutes=5)) == "live"


def test_pre_tip_lookup_drops_post_tip(tmp_path: Path):
    log = tmp_path / "snap.jsonl"
    rows = [
        {
            "as_of": "2026-10-25T16:00:00+00:00",
            "slate": "2026-10-25",
            "pre_tip": True,
            "player": "Austin Reaves",
            "player_norm": "austin reaves",
            "team": "LAL",
            "tonight_starter": "projected",
            "player_injury_status": "healthy",
            "slate_minutes": 30.2,
        },
        {
            "as_of": "2026-10-25T21:10:00+00:00",
            "slate": "2026-10-25",
            "pre_tip": False,
            "player": "Austin Reaves",
            "player_norm": "austin reaves",
            "team": "LAL",
            "tonight_starter": "confirmed",
            "player_injury_status": "healthy",
            "slate_minutes": 34.0,
        },
        {
            "as_of": "2026-10-25T16:05:00+00:00",
            "slate": "2026-10-25",
            "pre_tip": True,
            "player": "Austin Reaves",
            "player_norm": "austin reaves",
            "team": "LAL",
            "tonight_starter": "projected",
            "player_injury_status": "healthy",
            "slate_minutes": 29.8,
        },
    ]
    log.write_text("\n".join(__import__("json").dumps(r) for r in rows) + "\n", encoding="utf-8")
    lookup = load_pre_tip_lookup(log)
    rec = lookup[("2026-10-25", "LAL", "austin reaves")]
    assert rec["slate_minutes"] == 29.8
    assert rec["tonight_starter"] == "projected"


def test_apply_pre_tip_skips_missing_snapshot():
    df = pd.DataFrame(
        [
            {
                "file_date": "2026-10-20",
                "player": "Austin Reaves",
                "team": "LAL",
                "line": 16.5,
                "direction": "OVER",
                "proj_mu": 18.0,
                "proj_minutes": 32.0,
                "proj_var": 20.0,
                "proj_n": 12,
                "base_rate": 0.55,
                "proj_hold": 0.0,
            }
        ]
    )
    out = apply_pre_tip_slate_minutes(df, lookup={})
    assert int(out.loc[0, "slate_minutes_used"]) == 0
    assert float(out.loc[0, "proj_mu"]) == 18.0


def test_apply_pre_tip_rescales_only_joined_row():
    lookup = {
        ("2026-10-25", "BKN", "michael porter"): {
            "slate_minutes": 27.0,
            "tonight_starter": "projected",
            "player_injury_status": "healthy",
            "pre_tip": True,
        }
    }
    df = pd.DataFrame(
        [
            {
                "file_date": "2026-10-25",
                "player": "Michael Porter Jr.",
                "team": "BKN",
                "line": 16.5,
                "direction": "OVER",
                "proj_mu": 20.0,
                "proj_minutes": 32.0,
                "proj_var": 22.0,
                "proj_n": 10,
                "base_rate": 0.55,
                "proj_hold": 0.0,
            },
            {
                "file_date": "2026-10-20",
                "player": "Austin Reaves",
                "team": "LAL",
                "line": 16.5,
                "direction": "OVER",
                "proj_mu": 18.0,
                "proj_minutes": 32.0,
                "proj_var": 20.0,
                "proj_n": 12,
                "base_rate": 0.55,
                "proj_hold": 0.0,
            },
        ]
    )
    out = apply_pre_tip_slate_minutes(df, lookup=lookup)
    porter = out.loc[out["player"].str.contains("Porter")].iloc[0]
    reaves = out.loc[out["player"].str.contains("Reaves")].iloc[0]
    assert int(porter["slate_minutes_used"]) == 1
    assert abs(float(porter["proj_mu"]) - 20.0 * (27.0 / 32.0)) < 1e-6
    assert int(reaves["slate_minutes_used"]) == 0
    assert float(reaves["proj_mu"]) == 18.0


def test_attach_marks_pre_tip_from_game_tip():
    snap = {
        "fetched_at": "2026-10-25T16:00:00+00:00",
        "confirmed": {},
        "projected": {"LAL": ["Austin Reaves"]},
        "injuries": {},
        "b2b_teams": [],
        "games": [
            {
                "home": "UTA",
                "away": "LAL",
                "tip_utc": "2026-10-25T21:00:00+00:00",
            }
        ],
    }
    df = pd.DataFrame([{"player": "Austin Reaves", "team": "LAL", "role_mpg": 34.0}])
    out = attach_slate_minutes_columns(df, snapshot=snap, write_log=False)
    row = out.iloc[0]
    assert bool(row["slate_pre_tip"]) is True
    assert row["game_window"] == "early"
