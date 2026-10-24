"""PASS/RUN/FG identifiers and player share % of team."""

from pathlib import Path

import pandas as pd

from utils.nfl_scoring_vehicles import attach_nfl_identity_and_share, vehicle_flags
from utils.nfl_box_volume import parse_volume_from_summary
from utils.team_share import _norm_prop


def test_vehicle_flags():
    run = vehicle_flags("RUN+FG")
    assert run["is_run_team"] is True
    assert run["is_fg_team"] is True
    assert run["is_pass_team"] is False
    assert run["off_identity"] == "Rush + FG team"
    pas = vehicle_flags("PASS")
    assert pas["is_pass_team"] is True
    assert pas["is_run_team"] is False


def test_prop_aliases_to_share_names():
    assert _norm_prop("rushing_yards") == "Rush Yards"
    assert _norm_prop("rushing_attempts") == "Rush Attempts"
    assert _norm_prop("receiving_yards") == "Receiving Yards"
    assert _norm_prop("targets") == "Rec Targets"
    assert _norm_prop("kicking_points") == "Kick Pts"


def test_attach_identity_and_share(tmp_path: Path, monkeypatch):
    nfl = tmp_path / "Sports" / "NFL" / "data"
    nfl.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "team": "SEA",
                "vehicle": "RUN+FG",
                "off_identity": "Rush + FG team",
                "is_run_team": True,
                "is_pass_team": False,
                "is_fg_team": True,
                "is_balanced": False,
                "pass_share": 0.62,
                "rush_share": 0.38,
                "fg_pg": 2.45,
            }
        ]
    ).to_csv(nfl / "nfl_scoring_vehicles.csv", index=False)
    share = {
        "applicable": True,
        "by_player": {
            "SEA|kenneth walker iii|Rush Yards": {
                "share_pct": 48.2,
                "player_avg": 67.0,
                "team_avg": 139.0,
            },
            "SEA|kenneth walker iii|Rush Attempts": {
                "share_pct": 51.0,
                "player_avg": 16.0,
                "team_avg": 31.4,
            },
            "SEA|kenneth walker iii|Receiving Yards": {
                "share_pct": 8.1,
                "player_avg": 18.0,
                "team_avg": 222.0,
            },
        },
        "team_aliases": {},
    }
    monkeypatch.setattr(
        "utils.team_share.load_share_payload",
        lambda sport, repo=None: share,
    )
    slate = pd.DataFrame(
        [
            {
                "player_name": "Kenneth Walker III",
                "team": "SEA",
                "prop_type_normalized": "rushing_yards",
            }
        ]
    )
    out = attach_nfl_identity_and_share(slate, tmp_path)
    assert out.loc[0, "off_identity"] == "Rush + FG team"
    assert str(out.loc[0, "is_run_team"]).lower() in ("true", "1")
    assert str(out.loc[0, "is_fg_team"]).lower() in ("true", "1")
    assert float(out.loc[0, "prop_share_pct"]) == 48.2
    assert float(out.loc[0, "rush_att_share_pct"]) == 51.0
    assert float(out.loc[0, "rec_yds_share_pct"]) == 8.1


def test_parse_volume_from_espn_box():
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"id": "26"},
                    "statistics": [
                        {
                            "name": "rushing",
                            "labels": ["CAR", "YDS", "TD"],
                            "athletes": [
                                {
                                    "athlete": {"id": "1", "displayName": "Kenneth Walker III"},
                                    "stats": ["18", "80", "1"],
                                }
                            ],
                        },
                        {
                            "name": "passing",
                            "labels": ["C/ATT", "YDS"],
                            "athletes": [
                                {
                                    "athlete": {"id": "2", "displayName": "Sam Darnold"},
                                    "stats": ["22/31", "280"],
                                }
                            ],
                        },
                    ],
                }
            ]
        }
    }
    vol = parse_volume_from_summary(summary)
    assert vol[("1", "26")]["RUSH_ATT"] == 18.0
    assert vol[("2", "26")]["PASS_ATT"] == 31.0
