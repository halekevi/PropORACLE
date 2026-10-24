"""NFLP preseason playing-time gates (not 2025 L5)."""

from __future__ import annotations

import pandas as pd

from utils.nflp_playing_time import (
    apply_nflp_playing_time,
    d_aligned_side,
    expected_snaps_bucket,
    infer_starter_policy,
    is_nflp,
    minutes_tier_for_policy,
    nflp_list_eligible,
    snap_pct_value,
)


def test_is_nflp():
    assert is_nflp("NFLP")
    assert is_nflp(league_id="44")
    assert not is_nflp("NFL", "9")


def test_sit_from_high_snap():
    assert infer_starter_policy(league="NFLP", prop="rushing_yards", snap_pct=85) == "sit"
    assert infer_starter_policy(league="NFLP", prop="passing_yards", snap_pct=0.80) == "sit"
    assert infer_starter_policy(league="NFLP", prop="rushing_yards", snap_pct=50) == "cameo"
    assert infer_starter_policy(league="NFLP", prop="rushing_yards", snap_pct=15) == "backup"


def test_backup_qb_not_cameo_from_mid_snap():
    # 2025 spot-start snap% is not week-3 playing time.
    assert infer_starter_policy(league="NFLP", prop="passing_yards", snap_pct=45) == "backup"
    assert infer_starter_policy(league="NFLP", prop="passing_tds", snap_pct=45) == "backup"


def test_sit_from_l5_when_snap_missing():
    assert infer_starter_policy(league="NFLP", prop="passing_yards", l5_over=4) == "sit"
    assert infer_starter_policy(league="NFLP", prop="passing_yards") == "backup"


def test_kickers_play():
    assert infer_starter_policy(league="NFLP", prop="fg_made", snap_pct=90, l5_over=4) == "plays"
    assert infer_starter_policy(league="NFLP", prop="pat_made", l5_over=4) == "plays"
    assert expected_snaps_bucket("plays") == "30+"


def test_regular_nfl_unchanged():
    assert infer_starter_policy(league="NFL", prop="passing_yards", snap_pct=90, l5_over=5) == "normal"


def test_nflp_list_sit_over_dropped():
    assert not nflp_list_eligible(policy="sit", side="OVER", d_ok=True, l5_over=5)
    assert not nflp_list_eligible(policy="cameo", side="OVER", d_ok=True, l5_over=4)
    assert nflp_list_eligible(policy="sit", side="UNDER", pick_type="Standard", d_ok=False)


def test_nflp_list_backup_needs_d_on_over():
    assert nflp_list_eligible(policy="backup", side="OVER", d_ok=True, l5_over=None)
    assert not nflp_list_eligible(policy="backup", side="OVER", d_ok=False, l5_over=None)
    assert not nflp_list_eligible(policy="backup", side="OVER", d_ok=False, l5_over=5)


def test_nflp_kicker_still_needs_l5():
    assert nflp_list_eligible(policy="plays", side="OVER", l5_over=4)
    assert not nflp_list_eligible(policy="plays", side="OVER", l5_over=2)
    assert nflp_list_eligible(policy="plays", side="UNDER", l5_under=4)


def test_minutes_tier_nflp():
    assert minutes_tier_for_policy("sit") == "LOW"
    assert minutes_tier_for_policy("backup") == "HIGH"
    assert minutes_tier_for_policy("normal", 85) == "HIGH"


def test_apply_writes_columns():
    df = pd.DataFrame(
        {
            "league": ["NFLP", "NFLP", "NFL"],
            "prop_type": ["rushing_yards", "passing_yards", "passing_yards"],
            "snap_pct_L3": [88.0, None, 88.0],
            "l5_over": [5, None, 5],
            "l5_under": [0, None, 0],
        }
    )
    out = apply_nflp_playing_time(df)
    assert list(out["starter_policy"]) == ["sit", "backup", "normal"]
    assert out.loc[0, "minutes_tier"] == "LOW"
    assert out.loc[1, "minutes_tier"] == "HIGH"
    assert out.loc[2, "minutes_tier"] == "HIGH"


def test_best_props_pool_drops_nflp_sit_over():
    from utils.best_props_pool import row_to_best_props_rec

    sit = {
        "sport": "NFL",
        "league": "NFLP",
        "player": "Tony Pollard",
        "prop_type": "rushing_yards",
        "pick_type": "Standard",
        "final_bet_direction": "OVER",
        "line": 24.5,
        "l5_over": 5,
        "l5_under": 0,
        "def_tier": "Elite",
        "stat_season_avg": 100.0,
    }
    backup = {
        "sport": "NFL",
        "league": "NFLP",
        "player": "Drew Lock",
        "prop_type": "passing_yards",
        "pick_type": "Standard",
        "final_bet_direction": "OVER",
        "line": 91.5,
        "l5_over": None,
        "l5_under": None,
        "def_tier": "Below Avg",
        "OVERALL_DEF_RANK": 23,
        "stat_season_avg": 91.5,
        "model_dir": "OVER",
    }
    assert row_to_best_props_rec(sit) is None
    rec = row_to_best_props_rec(backup)
    assert rec is not None
    assert rec["player"] == "Drew Lock"
    assert rec["starter_policy"] == "backup"


def test_snap_pct_value_skips_nan():
    assert snap_pct_value(float("nan"), 12) == 12.0


def test_d_aligned_side():
    assert d_aligned_side("Weak") == "OVER"
    assert d_aligned_side("Below Avg") == "OVER"
    assert d_aligned_side("Elite") == "UNDER"
    assert d_aligned_side("Avg") is None
