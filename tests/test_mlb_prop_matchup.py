"""Pitcher props use opponent offense; hitter counting stays vs pitching."""

from __future__ import annotations

import pandas as pd

from utils.mlb_prop_matchup import (
    batting_strength_label,
    format_mlb_matchup,
    overlay_pitcher_offense_matchup,
    pitcher_offense_axis,
)
from utils.slate_context_fill import strip_pitcher_ks_hitter_defense


def test_pitcher_props_map_to_offense_axes():
    assert pitcher_offense_axis("Hits Allowed", "pitcher") == "opp_offense_hits"
    assert pitcher_offense_axis("Earned Runs Allowed", "P") == "opp_offense_runs"
    assert pitcher_offense_axis("Pitcher Strikeouts", "pitcher") == "opp_offense_ks"
    assert pitcher_offense_axis("Total Bases", "hitter") is None
    assert pitcher_offense_axis("Hits+Runs+RBIs", "hitter") is None
    assert pitcher_offense_axis("Hitter Strikeouts", "hitter") is None


def test_overlay_swaps_pitcher_d_keeps_hitter_pitching():
    df = pd.DataFrame(
        {
            "player": ["Hunter Brown", "Mookie Betts", "Tarik Skubal"],
            "player_type": ["pitcher", "hitter", "pitcher"],
            "prop_type": ["Hits Allowed", "Hits+Runs+RBIs", "Pitcher Strikeouts"],
            "OVERALL_DEF_RANK": [19, 23, 23],
            "DEF_TIER": ["Below Avg", "Below Avg", "Below Avg"],
            "def_rank": [19, 23, 23],
            "def_tier": ["Below Avg", "Below Avg", "Below Avg"],
            "OFF_HITS_RANK": [8, 12, 12],
            "OFF_HITS_TIER": ["Above Avg", "Avg", "Avg"],
            "OFF_RUNS_RANK": [9, 14, 14],
            "OFF_RUNS_TIER": ["Above Avg", "Avg", "Avg"],
            "OFF_SO_RANK": [22, 5, 5],
            "OFF_SO_TIER": ["Below Avg", "Elite", "Elite"],
        }
    )
    out = overlay_pitcher_offense_matchup(df)
    assert int(out.loc[0, "OVERALL_DEF_RANK"]) == 8
    assert out.loc[0, "DEF_TIER"] == "Above Avg"
    assert out.loc[0, "def_axis"] == "opp_offense_hits"
    assert int(out.loc[1, "OVERALL_DEF_RANK"]) == 23
    assert out.loc[1, "DEF_TIER"] == "Below Avg"
    assert out.loc[1, "def_axis"] == "opp_pitching"
    assert int(out.loc[2, "OVERALL_DEF_RANK"]) == 5
    assert out.loc[2, "DEF_TIER"] == "Elite"
    assert out.loc[2, "def_axis"] == "opp_offense_ks"


def test_strip_keeps_offense_ks_axis():
    df = pd.DataFrame(
        {
            "player": ["Tanner Scott", "Tanner Scott"],
            "prop_type": ["Strikeouts", "Strikeouts"],
            "player_type": ["pitcher", "pitcher"],
            "opponent_def_rank": [5, 28],
            "def_tier": ["Elite", "WEAK"],
            "def_axis": ["opp_offense_ks", ""],
        }
    )
    out = strip_pitcher_ks_hitter_defense(df)
    assert float(out["opponent_def_rank"].iloc[0]) == 5
    assert pd.isna(out["opponent_def_rank"].iloc[1]) or str(out["opponent_def_rank"].iloc[1]) == "<NA>"


def test_batting_strength_inverts_production_scale():
    assert batting_strength_label("Weak") == "Strong"
    assert batting_strength_label("Below Avg") == "Above Avg"
    assert batting_strength_label("Avg") == "Avg"
    assert batting_strength_label("Above Avg") == "Below Avg"
    assert batting_strength_label("Elite") == "Weak"
    assert batting_strength_label("") == ""


def test_hitter_matchup_labels_bat_then_opp_pitch():
    text = format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Hits+Runs+RBIs",
            "player_type": "hitter",
            "own_off_hits_tier": "Weak",
            "opp_pitching_tier": "Weak",
            "def_rank": 28,
        }
    )
    assert text == "Bat Strong · Opp pitch Weak#28"
    with_ba = format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Hits+Runs+RBIs",
            "player_type": "hitter",
            "own_off_hits_tier": "Weak",
            "opp_pitching_tier": "Weak",
            "def_rank": 28,
            "batting_avg": 0.312,
        }
    )
    assert with_ba == "BA .312 · Bat Strong · Opp pitch Weak#28"


def test_tb_avg_lineup_still_shows_opp_pitch():
    text = format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Total Bases",
            "player_type": "hitter",
            "own_off_hits_tier": "Avg",
            "opp_pitching_tier": "Weak",
            "def_rank": 30,
        }
    )
    assert text == "Bat Avg · Opp pitch Weak#30"


def test_hits_allowed_labels_opp_bats():
    text = format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Hits Allowed",
            "player_type": "pitcher",
            "def": "Weak",
            "def_axis": "opp_offense_hits",
            "def_rank": 8,
        }
    )
    assert text == "Opp bats Strong#8"


def test_all_keep_props_have_labeled_matchup():
    assert format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Walks Allowed",
            "player_type": "pitcher",
            "own_def_tier": "Weak",
        }
    ) == "Own pitch Weak"
    assert format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Pitches Thrown",
            "player_type": "pitcher",
            "own_def_tier": "Below Avg",
        }
    ) == "Own pitch Below Avg"
    assert "Opp scoring Strong" in format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Earned Runs Allowed",
            "player_type": "pitcher",
            "def": "Weak",
            "def_axis": "opp_offense_runs",
        }
    )
    assert format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Pitcher Strikeouts",
            "player_type": "pitcher",
            "own_def_tier": "Elite",
            "def": "Weak",
            "def_axis": "opp_offense_ks",
        }
    ) == "Own pitch Elite · Opp Ks High"
    assert format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Pitching Outs",
            "player_type": "pitcher",
            "own_def_tier": "Elite",
        }
    ) == "Own pitch Elite"
    hk = format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Hitter Strikeouts",
            "player_type": "hitter",
            "own_off_hits_tier": "Weak",
            "opp_pitching_tier": "Elite",
            "def_rank": 3,
        }
    )
    assert hk == "Bat Strong · Opp pitch Elite#3"
    hits = format_mlb_matchup(
        {
            "sport": "MLB",
            "prop": "Hits",
            "player_type": "hitter",
            "own_off_hits_tier": "Below Avg",
            "opp_pitching_tier": "Weak",
            "def_rank": 29,
        }
    )
    assert hits == "Bat Above Avg · Opp pitch Weak#29"
