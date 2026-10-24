"""NFL division membership, unit ranks, and divisional tightness."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_divisions import nfl_division, same_conference, same_division
from utils.nfl_team_unit_ranks import (
    analyze_divisional_tightness,
    attach_nfl_unit_ranks,
    build_nfl_team_unit_ranks,
)


def test_same_division():
    assert same_division("BUF", "MIA")
    assert same_division("was", "PHI")
    assert same_division("LA", "SF")
    assert not same_division("BUF", "KC")
    assert not same_division("SEA", "NE")
    assert same_conference("BUF", "KC")
    assert not same_conference("BUF", "PHI")
    assert nfl_division("WSH") == "NFC East"


def test_div_ranks_one_through_four(tmp_path: Path):
    def_path = tmp_path / "data" / "reference"
    nfl_data = tmp_path / "Sports" / "NFL" / "data" / "cache"
    def_path.mkdir(parents=True)
    nfl_data.mkdir(parents=True)
    teams = [
        ("BUF", 1, 30.0, 15.0, 180.0, 90.0),
        ("MIA", 2, 24.0, 20.0, 210.0, 100.0),
        ("NE", 3, 22.0, 18.0, 250.0, 110.0),
        ("NYJ", 4, 16.0, 28.0, 190.0, 80.0),
    ]
    # Minimal defense CSV for all 32 is not required if we only pass 4 teams...
    # build_nfl_team_unit_ranks starts from defense CSV teams.
    pd.DataFrame(
        {
            "team_abbr": [t[0] for t in teams],
            "pass_def_rank": [t[1] for t in teams],
            "rush_def_rank": [4, 3, 2, 1],
            "pa_rank": [4, 3, 2, 1],
            "opp_pass_ypg": [180, 200, 220, 240],
            "opp_rush_ypg": [90, 110, 100, 130],
        }
    ).to_csv(def_path / "nfl_team_defense.csv", index=False)
    (tmp_path / "Sports" / "NFL" / "data" / "cache" / "nfl_boxscore_cache.csv").write_text(
        "team_id,player_norm,game_date,event_id,PASS_YDS,RUSH_YDS\n",
        encoding="utf-8",
    )
    games = pd.DataFrame(
        [
            {"season": 2025, "week": 1, "home": a, "away": b, "home_score": pf, "away_score": pa,
             "margin": abs(pf - pa), "total": pf + pa, "divisional": True, "conference": True,
             "home_division": "AFC East", "away_division": "AFC East", "event_id": f"{a}{b}", "date": "2025-09-07"}
            for (a, _, pf, pa, _, _), (b, *_) in zip(teams, teams[1:] + teams[:1])
        ]
    )
    # Scoring from home/away: BUF 30, MIA 24, NE 22, NYJ 16 if we also add reverse... 
    # Simpler: inject scoring via games that give unique PF.
    games = pd.DataFrame(
        {
            "season": [2025] * 4,
            "week": [1] * 4,
            "event_id": ["1", "2", "3", "4"],
            "date": ["2025-09-07"] * 4,
            "home": ["BUF", "MIA", "NE", "NYJ"],
            "away": ["MIA", "NE", "NYJ", "BUF"],
            "home_score": [35, 21, 24, 10],
            "away_score": [14, 17, 20, 28],
            "margin": [21, 4, 4, 18],
            "total": [49, 38, 44, 38],
            "divisional": [True] * 4,
            "conference": [True] * 4,
            "home_division": ["AFC East"] * 4,
            "away_division": ["AFC East"] * 4,
        }
    )
    ranks = build_nfl_team_unit_ranks(tmp_path, season=2025, games=games)
    east = ranks[ranks["division"] == "AFC East"].set_index("team")
    assert set(east["div_score_off_rank"].astype(int)) <= {1, 2, 3, 4}
    assert int(east["div_score_off_rank"].max()) <= 4
    assert int(east.loc["BUF", "div_score_off_rank"]) == 1  # 35+28 = 63 PF / 2
    assert int(east.loc["NYJ", "div_score_off_rank"]) == 4  # 10+20 = 30


def test_tightness_div_vs_nondiv():
    games = pd.DataFrame(
        {
            "season": [2025] * 6,
            "margin": [3, 6, 7, 17, 21, 24],
            "total": [40, 44, 41, 51, 55, 60],
            "divisional": [True, True, True, False, False, False],
            "conference": [True, True, True, True, False, False],
            "home_division": ["AFC East"] * 3 + ["AFC East"] * 3,
            "away_division": ["AFC East"] * 3 + ["AFC West"] * 3,
        }
    )
    out = analyze_divisional_tightness(games)
    assert out["divisional"]["mean_margin"] < out["non_divisional"]["mean_margin"]
    assert out["divisional"]["pct_one_score"] == 100.0
    assert out["delta"]["tighter_if_negative_margin"] is True


def test_attach_flags(tmp_path: Path):
    out_dir = tmp_path / "Sports" / "NFL" / "data"
    out_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "team": ["SEA", "NE"],
            "div_score_off_rank": [1, 2],
            "div_score_def_rank": [1, 3],
            "score_off_rank": [4, 12],
            "score_def_rank": [1, 8],
            "pass_off_rank": [10, 6],
            "rush_off_rank": [8, 20],
        }
    ).to_csv(out_dir / "nfl_team_unit_ranks.csv", index=False)
    slate = pd.DataFrame({"team": ["SEA"], "opp_team": ["NE"], "prop_type_normalized": ["passing_yards"]})
    att = attach_nfl_unit_ranks(slate, tmp_path)
    assert att.loc[0, "team_division"] == "NFC West"
    assert att.loc[0, "opp_division"] == "AFC East"
    assert not bool(att.loc[0, "divisional_game"])
    assert int(att.loc[0, "team_div_score_off_rank"]) == 1
    assert int(att.loc[0, "opp_score_def_rank"]) == 8
