"""Grade-time stat_def_tier stamp is logging only — tickets stay on overall D."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from grading.build_grades_html import prop_row_for_api  # noqa: E402
from utils.stat_def_stamp import lookup_stat_def_for_row, merge_stat_def_fields  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402


def test_wnba_assists_uses_ast_axis_not_overall():
    # LV overall_rank is Avg; Assists allowed is Elite. That's the mixup this stamp closes.
    ast = lookup_stat_def_for_row("WNBA", "LV", "Assists")
    pts = lookup_stat_def_for_row("WNBA", "LV", "Points")
    assert ast["stat_def_category"] == "ast"
    assert ast["stat_def_tier"] == "Elite"
    assert pts["stat_def_category"] == "pts"
    assert pts["stat_def_tier"] == "Avg"
    assert ast["stat_def_tier"] != pts["stat_def_tier"]


def test_wnba1h_and_nba_dispatch():
    wnba1h = lookup_stat_def_for_row("WNBA1H", "LV", "Assists")
    assert wnba1h["stat_def_tier"] == "Elite"
    mlb = lookup_stat_def_for_row("MLB", "NYY", "Hits")
    assert mlb["stat_def_tier"] == ""
    assert mlb["stat_def_category"] == ""


def test_existing_row_snapshot_wins():
    out = merge_stat_def_fields(
        sport="WNBA",
        opp="LV",
        prop="Assists",
        existing_tier="Weak",
        existing_rank=18,
        existing_category="ast",
    )
    assert out["stat_def_tier"] == "Weak"
    assert out["stat_def_rank"] == 18


def test_prop_row_for_api_stamps_without_touching_def_tier():
    row = {
        "Player": "Arike Ogunbowale",
        "Team": "DAL",
        "opp_team": "LV",
        "Prop Type": "Assists",
        "Line": 4.5,
        "Dir": "OVER",
        "Pick Type": "Goblin",
        "def_tier": "Above Avg",
        "Actual": 6,
        "result": "HIT",
    }
    api = prop_row_for_api(row, "WNBA")
    assert api is not None
    assert api["def_tier"] == "Above Avg"
    assert api["stat_def_tier"] == "Elite"
    assert api["stat_def_category"] == "ast"
    assert api["stat_def_rank"]


def test_goblin_70_still_reads_overall_def_not_stat_axis():
    row = {
        "sport": "WNBA",
        "player": "Arike Ogunbowale",
        "prop": "Assists",
        "side": "OVER",
        "line": 1.5,
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "cover": 2.0,
        "def": "Avg",
        "def_tier": "Avg",
        "stat_def_tier": "Weak",
        "poss_g": 10.0,
    }
    assert not goblin_70_eligible(row)
    row["def"] = "Weak"
    row["def_tier"] = "Weak"
    assert goblin_70_eligible(row)


def test_enrichment_fills_empty_stat_def_leaves_def_tier():
    from utils.graded_enrichment import backfill_stat_def_tier_dataframe

    df = pd.DataFrame(
        [
            {
                "sport": "WNBA",
                "opp_team": "LV",
                "prop": "Assists",
                "def_tier": "Above Avg",
                "stat_def_tier": "",
            }
        ]
    )
    out = backfill_stat_def_tier_dataframe(df)
    assert out.at[0, "def_tier"] == "Above Avg"
    assert out.at[0, "stat_def_tier"] == "Elite"
    assert out.at[0, "stat_def_category"] == "ast"
