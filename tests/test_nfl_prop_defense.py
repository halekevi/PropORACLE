"""Tests for NFL prop-aware defense axis + snap→minutes_tier."""

import pandas as pd

from utils.nfl_prop_defense import (
    assign_prop_aware_def_tier,
    fill_opp_team_from_game,
    prop_def_axis,
    select_opp_def_rank,
    snap_pct_to_minutes_tier,
)


def test_prop_def_axis_pass_rush_rec():
    assert prop_def_axis("passing_yards") == "pass"
    assert prop_def_axis("rushing_yards") == "rush"
    assert prop_def_axis("receiving_yards") == "pass"
    assert prop_def_axis("receptions") == "pass"
    assert prop_def_axis("fg_made") == "kick"
    assert prop_def_axis("kicking_points") == "kick"
    assert prop_def_axis("tackles_ast") == "opp_rush_off"
    assert prop_def_axis("Tackles + Ast") == "opp_rush_off"
    assert prop_def_axis("solo_tackles") == "opp_rush_off"


def test_select_opp_def_rank_uses_rush_for_rush_props():
    row = {"opp_pass_def_rank": 5, "opp_rush_def_rank": 28, "prop_type_normalized": "rushing_yards"}
    assert select_opp_def_rank(row) == 28.0
    row2 = {"opp_pass_def_rank": 5, "opp_rush_def_rank": 28, "prop_type_normalized": "passing_yards"}
    assert select_opp_def_rank(row2) == 5.0
    row3 = {
        "opp_pass_def_rank": 5,
        "opp_rush_def_rank": 28,
        "opp_fg_def_rank": 12,
        "prop_type_normalized": "fg_made",
    }
    assert select_opp_def_rank(row3) == 12.0
    row4 = {
        "opp_pass_def_rank": 5,
        "opp_rush_def_rank": 28,
        "opp_rush_off_rank": 3,
        "prop_type_normalized": "tackles_ast",
    }
    assert select_opp_def_rank(row4) == 3.0


def test_fill_opp_team_from_game():
    df = pd.DataFrame(
        {
            "team": ["TEN", "SEA", "TEN"],
            "opp_team": [float("nan"), float("nan"), ""],
            "game_id": ["174855", "174855", "174855"],
        }
    )
    out = fill_opp_team_from_game(df)
    assert out.loc[0, "opp_team"] == "SEA"
    assert out.loc[1, "opp_team"] == "TEN"
    assert out.loc[2, "opp_team"] == "SEA"


def test_fill_opp_from_home_away_single_team():
    df = pd.DataFrame(
        {
            "team": ["CHI"],
            "opp_team": [""],
            "game_id": ["176193"],
            "home_team": ["TEN"],
            "away_team": ["CHI"],
        }
    )
    out = fill_opp_team_from_game(df)
    assert out.loc[0, "opp_team"] == "TEN"


def test_fill_opp_from_game_text_and_espn_map():
    from utils.nfl_prop_defense import opp_from_game_text

    assert opp_from_game_text("CHI @ TEN", "CHI") == "TEN"
    assert opp_from_game_text("TEN vs CHI", "CHI") == "TEN"
    df = pd.DataFrame({"team": ["WAS"], "opp_team": [""], "game_id": ["1"]})
    out = fill_opp_team_from_game(df, espn_opp_map={"WSH": "NYG"})
    assert out.loc[0, "opp_team"] == "NYG"


def test_nfl_weather_flag_and_context_score():
    from utils.nfl_espn_context import expected_snaps_from_slot, game_context_score, weather_flag

    assert weather_flag(indoor=True, wind_mph=25, precip_mm=0) == "dome"
    assert weather_flag(indoor=False, wind_mph=16, precip_mm=0) == "wind"
    assert weather_flag(indoor=False, wind_mph=4, precip_mm=0) == "calm"
    assert expected_snaps_from_slot("QB1") == "starter"
    assert expected_snaps_from_slot("PK1") == "starter"
    assert expected_snaps_from_slot("RB2") == "rotation"
    score = game_context_score(
        axis="pass", wind_mph=18, weather_flag_val="wind", blowout=True, low_total=False, spread=-9.0
    )
    assert score < 0


def test_parse_depth_chart_qb_order():
    from utils.nfl_espn_context import parse_depth_chart_payload

    payload = {
        "depthchart": [
            {
                "name": "3WR 1TE",
                "positions": {
                    "qb": {
                        "position": {"abbreviation": "QB"},
                        "athletes": [
                            {"id": "1", "displayName": "Sam Darnold"},
                            {"id": "2", "displayName": "Drew Lock"},
                        ],
                    },
                    "wr1": {
                        "position": {"abbreviation": "WR"},
                        "athletes": [{"id": "3", "displayName": "Jaxon Smith-Njigba"}],
                    },
                },
            }
        ]
    }
    rows = parse_depth_chart_payload(payload, "SEA")
    by_name = {r["player"]: r for r in rows}
    assert by_name["Sam Darnold"]["depth_slot"] == "QB1"
    assert by_name["Drew Lock"]["depth_slot"] == "QB2"
    assert by_name["Jaxon Smith-Njigba"]["depth_slot"] == "WR1"


def test_fill_opp_ignores_combo_team_rows():
    df = pd.DataFrame(
        {
            "team": ["CLE", "NE", "CLE/NE", "CLE"],
            "opp_team": ["", "", "", ""],
            "game_id": ["175717", "175717", "175717", "175717"],
        }
    )
    out = fill_opp_team_from_game(df)
    assert out.loc[0, "opp_team"] == "NE"
    assert out.loc[1, "opp_team"] == "CLE"
    assert out.loc[3, "opp_team"] == "NE"


def test_assign_prop_aware_def_tier():
    df = pd.DataFrame(
        {
            "prop_type_normalized": [
                "passing_yards",
                "rushing_yards",
                "receiving_yards",
                "tackles_ast",
            ],
            "opp_pass_def_rank": [1, 1, 1, 1],
            "opp_rush_def_rank": [32, 32, 32, 32],
            "opp_rush_off_rank": [4, 4, 4, 4],
        }
    )
    out = assign_prop_aware_def_tier(df, n_teams=32)
    assert out.loc[0, "opp_def_rank_prop"] == 1.0
    assert out.loc[1, "opp_def_rank_prop"] == 32.0
    assert out.loc[2, "opp_def_rank_prop"] == 1.0  # rec → pass
    assert out.loc[3, "opp_def_rank_prop"] == 4.0  # tackles → opp rush OFF
    assert out.loc[0, "def_tier"] == "Elite"
    assert out.loc[1, "def_tier"] == "Weak"
    assert out.loc[3, "def_tier"] == "Elite"  # rank 4/32 → Elite


def test_snap_pct_to_minutes_tier():
    assert snap_pct_to_minutes_tier(80) == "HIGH"
    assert snap_pct_to_minutes_tier(0.55) == "MEDIUM"
    assert snap_pct_to_minutes_tier(20) == "LOW"
    assert snap_pct_to_minutes_tier(None) == "UNKNOWN"


def test_last_completed_nfl_season_before_and_after_september():
    from datetime import date

    from utils.nfl_prop_defense import last_completed_nfl_season

    assert last_completed_nfl_season(date(2026, 8, 23)) == 2025
    assert last_completed_nfl_season(date(2026, 9, 1)) == 2026


def test_defense_table_is_complete_and_sync(tmp_path):
    from utils.nfl_prop_defense import defense_table_is_complete, sync_nfl_reference_defense

    empty = pd.DataFrame({"team": ["SEA"], "pass_def_rank": [None]})
    assert not defense_table_is_complete(empty, min_teams=28)
    step4 = pd.DataFrame(
        {
            "team": ["SEA", "TEN"],
            "pass_def_rank": [10, 23],
            "rush_def_rank": [3, 15],
            "fg_def_rank": [9, 20],
            "pass_yards_allowed_pg": [193.9, 230.5],
            "rush_yards_allowed_pg": [91.9, 114.6],
            "points_allowed_pg": [17.2, 28.1],
        }
    )
    assert not defense_table_is_complete(step4, min_teams=28)
    ref = tmp_path / "nfl_team_defense.csv"
    sync_nfl_reference_defense(step4, season=2025, reference_path=ref)
    out = pd.read_csv(ref)
    assert set(out["team_abbr"]) == {"SEA", "TEN"}
    sea = out.set_index("team_abbr").loc["SEA"]
    assert int(sea["pass_def_rank"]) == 10
    assert int(sea["rush_def_rank"]) == 3
    assert int(sea["fg_def_rank"]) == 9
    assert int(sea["season"]) == 2025
