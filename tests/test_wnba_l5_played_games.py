"""WNBA L5 is last 5 box-score games, including unlabeled July SEASON rows."""
from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_STEP4 = _ROOT / "Sports" / "WNBA" / "step4_fetch_player_stats.py"
_spec = importlib.util.spec_from_file_location("wnba_step4_fetch_player_stats", _STEP4)
assert _spec and _spec.loader
_step4 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_step4)

from utils.hit_tracking_columns import fill_l5_from_stat_games  # noqa: E402


def test_unlabeled_july_august_games_fill_l5_five_and_four_hits():
    """PrizePicks-style recency: blank SEASON on July games must still count."""
    ath = "4432807"
    cache = pd.DataFrame(
        {
            "game_date": [
                "2026-08-08",
                "2026-08-05",
                "2026-08-03",
                "2026-07-31",
                "2026-07-28",
                "2026-07-22",
            ],
            "ESPN_ATHLETE_ID": [ath] * 6,
            "PLAYER_NAME": ["Flau'jae Johnson"] * 6,
            "PLAYER_NORM": ["flaujae johnson"] * 6,
            "PTS": [8, 11, 25, 9, 16, 16],
            "REB": [4, 4, 3, 0, 3, 6],
            "AST": [1, 3, 3, 3, 2, 4],
            # Only the most recent fetch tagged the season — historical rows blank.
            "SEASON": ["2026", "", "", "", "", ""],
            "MIN": [25, 27, 30, 26, 24, 29],
        }
    )
    stat_target = datetime(2026, 8, 16)
    cutoff = stat_target - timedelta(days=35)
    filt = _step4.filter_wnba_cache_for_rolling(
        cache,
        season_labels={"2026"},
        cutoff_stat=cutoff,
        stat_target=stat_target,
        allstar_breaks=[("2026-07-25", "2026-07-25")],
    )
    vals, games = _step4._stat_values_for_athlete(filt, ath, "pra", 10, 0.0)
    assert len(games) >= 5
    assert sum(1 for v in vals[:5] if pd.notna(v)) == 5
    # 13, 18, 31, 12, 21 vs 23.5 → 4 unders (PrizePicks Flau'jae PRA Under)
    df = pd.DataFrame(
        {
            "line": [23.5],
            "stat_g1": [vals[0]],
            "stat_g2": [vals[1]],
            "stat_g3": [vals[2]],
            "stat_g4": [vals[3]],
            "stat_g5": [vals[4]],
        }
    )
    out = fill_l5_from_stat_games(df, line_col="line")
    g5 = df[["stat_g1", "stat_g2", "stat_g3", "stat_g4", "stat_g5"]].apply(
        pd.to_numeric, errors="coerce"
    )
    played = int(g5.notna().sum(axis=1).iloc[0])
    assert played == 5
    assert float(out.loc[0, "l5_under"]) == 4.0
    assert float(out.loc[0, "l5_over"]) == 1.0


def test_explicit_other_season_still_excluded():
    cache = pd.DataFrame(
        {
            "game_date": ["2026-08-10", "2025-08-10"],
            "ESPN_ATHLETE_ID": ["1", "1"],
            "PTS": [10, 99],
            "REB": [0, 0],
            "AST": [0, 0],
            "SEASON": ["2026", "2025"],
        }
    )
    filt = _step4.filter_wnba_cache_for_rolling(
        cache,
        season_labels={"2026"},
        cutoff_stat=datetime(2025, 1, 1),
        stat_target=datetime(2026, 8, 16),
        allstar_breaks=[],
    )
    assert len(filt) == 1
    assert int(filt.iloc[0]["PTS"]) == 10
