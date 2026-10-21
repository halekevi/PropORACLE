"""Tests for nflverse snap-cache aggregation + soccer shots_conceded helpers."""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, rel: str):
    path = _ROOT / rel
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


_nfl = _load("build_nfl_snap_pct_cache", "Sports/NFL/scripts/build_nfl_snap_pct_cache.py")
_soc = _load("soccer_defense_report", "Sports/Soccer/scripts/soccer_defense_report.py")


def test_infer_default_season_before_sept_uses_prior_year():
    assert _nfl.infer_default_season(datetime(2026, 8, 22, tzinfo=timezone.utc)) == 2025
    assert _nfl.infer_default_season(datetime(2026, 9, 10, tzinfo=timezone.utc)) == 2026


def test_aggregate_snap_cache_l3_and_season():
    df = pd.DataFrame(
        [
            {"player": "A Player", "pfr_player_id": "PlayA00", "week": 1, "game_type": "REG", "offense_pct": 0.5},
            {"player": "A Player", "pfr_player_id": "PlayA00", "week": 2, "game_type": "REG", "offense_pct": 0.6},
            {"player": "A Player", "pfr_player_id": "PlayA00", "week": 3, "game_type": "REG", "offense_pct": 0.7},
            {"player": "A Player", "pfr_player_id": "PlayA00", "week": 4, "game_type": "REG", "offense_pct": 0.8},
            {"player": "B Player", "pfr_player_id": "PlayB00", "week": 1, "game_type": "REG", "offense_pct": 0.2},
        ]
    )
    players = _nfl.aggregate_snap_cache(df)
    a = players["a player"]
    assert a["snap_pct_season"] == 65.0
    assert a["snap_pct_L3"] == 70.0
    assert players["PlayA00"]["snap_pct_L3"] == 70.0
    assert players["b player"]["snap_pct_season"] == 20.0


def test_shots_conceded_pg_from_shots_faced():
    assert _soc.shots_conceded_pg_from_stat_map({"shotsFaced": 310.0, "appearances": 38.0}) == 8.158
    assert _soc.shots_conceded_pg_from_stat_map({"shotsFaced": 10.0}) is None
    assert _soc.shots_conceded_pg_from_stat_map({}) is None


def test_add_ranks_emits_shots_def_tiers():
    df = pd.DataFrame(
        [
            {"league": "EPL", "gp": 10, "goals_conceded_pg": 0.8, "shots_conceded_pg": 8.0, "pp_name": "A"},
            {"league": "EPL", "gp": 10, "goals_conceded_pg": 1.2, "shots_conceded_pg": 12.0, "pp_name": "B"},
            {"league": "EPL", "gp": 10, "goals_conceded_pg": 1.5, "shots_conceded_pg": 10.0, "pp_name": "C"},
            {"league": "EPL", "gp": 10, "goals_conceded_pg": 1.8, "shots_conceded_pg": 14.0, "pp_name": "D"},
            {"league": "EPL", "gp": 10, "goals_conceded_pg": 2.0, "shots_conceded_pg": 16.0, "pp_name": "E"},
        ]
    )
    out = _soc.add_ranks_and_tiers(df)
    assert "SHOTS_DEF_RANK" in out.columns and "SHOTS_DEF_TIER" in out.columns
    assert "GOALS_DEF_RANK" in out.columns and "GOALS_DEF_TIER" in out.columns
    # Lowest shots conceded → best (rank 1) shots defense.
    assert int(out.loc[out["pp_name"] == "A", "SHOTS_DEF_RANK"].iloc[0]) == 1
    assert out.loc[out["pp_name"] == "A", "SHOTS_DEF_TIER"].iloc[0] == "Elite"
    assert out.loc[out["pp_name"] == "E", "SHOTS_DEF_TIER"].iloc[0] == "Weak"
    # Goals overall still ranks on goals_conceded_pg.
    assert int(out.loc[out["pp_name"] == "A", "OVERALL_DEF_RANK"].iloc[0]) == 1


def test_soccer_prop_def_metric_mapping():
    from utils.soccer_prop_defense import assign_prop_aware_def_tier, prop_def_metric

    assert prop_def_metric("shots") == "shots_conceded"
    assert prop_def_metric("Shots On Target") == "shots_conceded"
    assert prop_def_metric("shots_assisted") == "shots_conceded"
    assert prop_def_metric("goals") == "goals_conceded"
    assert prop_def_metric("assists") == "goals_conceded"
    assert prop_def_metric("saves") == "goals_conceded"

    df = pd.DataFrame(
        [
            {
                "prop_norm": "shots",
                "OVERALL_DEF_RANK": 10,
                "DEF_TIER": "Weak",
                "SHOTS_DEF_RANK": 2,
                "SHOTS_DEF_TIER": "Elite",
                "league": "EPL",
            },
            {
                "prop_norm": "goals",
                "OVERALL_DEF_RANK": 10,
                "DEF_TIER": "Weak",
                "SHOTS_DEF_RANK": 2,
                "SHOTS_DEF_TIER": "Elite",
                "league": "EPL",
            },
            {
                "prop_norm": "shots_on_target",
                "OVERALL_DEF_RANK": 10,
                "DEF_TIER": "Weak",
                "SHOTS_DEF_RANK": pd.NA,
                "SHOTS_DEF_TIER": "",
                "league": "EPL",
            },
        ]
    )
    out = assign_prop_aware_def_tier(df)
    assert out.loc[0, "def_metric"] == "shots_conceded"
    assert out.loc[0, "DEF_TIER"] == "Elite"
    assert int(out.loc[0, "OVERALL_DEF_RANK"]) == 2
    assert out.loc[1, "def_metric"] == "goals_conceded"
    assert out.loc[1, "DEF_TIER"] == "Weak"
    # Missing shots rank → fall back to goals.
    assert out.loc[2, "def_metric"] == "goals_conceded"
    assert out.loc[2, "DEF_TIER"] == "Weak"
