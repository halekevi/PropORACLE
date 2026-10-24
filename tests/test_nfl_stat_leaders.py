"""NFL team/league leader ranks from boxscores (role floors, not zeros)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_stat_leaders import (
    attach_nfl_stat_ranks,
    build_nfl_leader_tables,
    season_from_game_date,
    write_nfl_leader_artifacts,
)


def _seed_repo(tmp: Path) -> Path:
    cache_dir = tmp / "Sports" / "NFL" / "data" / "cache"
    ref_dir = tmp / "data" / "reference"
    cache_dir.mkdir(parents=True)
    ref_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "team_id": [26, 14],
            "team_abbr": ["SEA", "LAR"],
            "team": ["SEA", "LAR"],
        }
    ).to_csv(ref_dir / "nfl_team_defense.csv", index=False)
    rows = []
    dates = [f"2025090{d}" for d in range(1, 6)]  # 5 regular-season games

    def add(tid: int, name: str, col: str, vals: list[float]) -> None:
        for gd, v in zip(dates, vals):
            rec = {
                "team_id": tid,
                "player_norm": name,
                "espn_athlete_id": "1",
                "game_date": gd,
                "event_id": gd,
                "PASS_YDS": 0.0,
                "PASS_TD": 0.0,
                "RUSH_YDS": 0.0,
                "RUSH_TD": 0.0,
                "REC_YDS": 0.0,
                "REC": 0.0,
                "REC_TD": 0.0,
                "KICK_PTS": 0.0,
            }
            rec[col] = v
            rows.append(rec)

    # SEA receiving: 8 eligible + 1 zero-volume specialist
    add(26, "puka nacua", "REC_YDS", [110, 100, 90, 95, 105])
    add(26, "jaxon smith-njigba", "REC_YDS", [80, 75, 70, 85, 90])
    add(26, "cooper kupp", "REC_YDS", [55, 50, 48, 52, 60])
    add(26, "tory horton", "REC_YDS", [35, 32, 30, 28, 33])
    add(26, "aj barner", "REC_YDS", [28, 26, 24, 25, 27])
    add(26, "elijah arroyo", "REC_YDS", [22, 21, 23, 20, 22])
    add(26, "dareke young", "REC_YDS", [20.5, 20, 21, 20, 20])
    add(26, "jake bobo", "REC_YDS", [20, 20, 20, 21, 20])
    add(26, "practice squad wr", "REC_YDS", [0, 0, 0, 0, 2])  # below 20 yds/g floor

    add(26, "kenneth walker iii", "RUSH_YDS", [90, 80, 70, 85, 75])
    add(26, "zach charbonnet", "RUSH_YDS", [40, 35, 38, 42, 36])
    add(26, "sam darnold", "PASS_YDS", [250, 240, 230, 260, 255])

    add(14, "puka nacua", "REC_YDS", [40, 40, 40, 40, 40])  # different team, not SEA #1
    add(14, "davante adams", "REC_YDS", [95, 90, 88, 92, 100])
    pd.DataFrame(rows).to_csv(cache_dir / "nfl_boxscore_cache.csv", index=False)
    return tmp


def test_season_skips_preseason():
    assert season_from_game_date("20250815") is None
    assert season_from_game_date("20250907") == 2025
    assert season_from_game_date("20260208") == 2025


def test_team_top_bottom_and_floors(tmp_path: Path):
    repo = _seed_repo(tmp_path)
    tables = build_nfl_leader_tables(repo, season=2025)
    sea_rec = tables["team_blocks"]["SEA|rec_yds"]
    tops = [p for p in sea_rec if p["leader_slice"] == "top"]
    bots = [p for p in sea_rec if p["leader_slice"] == "bottom"]
    names_top = [p["player_norm"] for p in tops]
    names_bot = [p["player_norm"] for p in bots]
    assert names_top[0] == "puka nacua"
    assert "practice squad wr" not in names_top
    assert "practice squad wr" not in names_bot
    assert "jake bobo" in names_bot
    assert "puka nacua" not in names_bot
    lg = tables["league_boards"]["rec_yds"]["top"]
    assert lg[0]["player_norm"] == "puka nacua"
    assert lg[0]["team"] == "SEA"


def test_write_and_attach(tmp_path: Path):
    repo = _seed_repo(tmp_path)
    write_nfl_leader_artifacts(repo, season=2025)
    slate = pd.DataFrame(
        {
            "player_name": ["Puka Nacua", "Jake Bobo", "Practice Squad Wr"],
            "team": ["SEA", "SEA", "SEA"],
            "prop_type_normalized": ["receiving_yards", "receiving_yards", "receiving_yards"],
        }
    )
    out = attach_nfl_stat_ranks(slate, repo)
    assert int(out.loc[0, "team_stat_rank"]) == 1
    assert int(out.loc[0, "league_stat_rank"]) == 1
    assert out.loc[0, "leader_slice"] == "top"
    assert int(out.loc[0, "team_top3_rank"]) == 1
    assert out.loc[1, "leader_slice"] == "bottom"
    assert pd.isna(out.loc[2, "team_stat_rank"])
