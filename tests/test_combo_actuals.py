"""Multi-player combo actuals: strip-combo prop keys + A+B arm sum."""
from __future__ import annotations

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.combo_actuals import (  # noqa: E402
    prop_lookup_keys,
    split_combo_players,
    split_combo_teams,
    strip_combo_prop_token,
    sum_multiplayer_combo_actual,
)


def test_strip_combo_prop_token_compact_and_display():
    assert strip_combo_prop_token("pitcherstrikeoutscombo") == "pitcherstrikeouts"
    assert strip_combo_prop_token("Pitcher Strikeouts (Combo)") == "Pitcher Strikeouts"
    assert strip_combo_prop_token("passes_attempted_combo") == "passes_attempted"
    assert strip_combo_prop_token("shots on goal combo") == "shots on goal"
    assert strip_combo_prop_token("pitcherstrikeouts") == "pitcherstrikeouts"


def test_prop_lookup_keys_includes_base_and_mlb_aliases():
    keys = prop_lookup_keys(
        "pitcherstrikeoutscombo",
        extra_aliases={
            "pitcherstrikeouts": ["pitcherstrikeouts", "strikeouts"],
        },
    )
    assert "pitcherstrikeoutscombo" in keys
    assert "pitcherstrikeouts" in keys
    assert "strikeouts" in keys


def test_split_combo_players_and_teams():
    assert split_combo_players("Tarik Skubal + Nick Lodolo") == [
        "Tarik Skubal",
        "Nick Lodolo",
    ]
    assert split_combo_teams("LAD/CIN", 2) == ["LAD", "CIN"]
    assert split_combo_teams("DET", 2) == ["DET", "DET"]


def test_sum_multiplayer_combo_actual_mlb_ks_shape():
    """Solo arm rows under pitcherstrikeouts resolve a pitcherstrikeoutscombo slate prop."""
    store = {
        ("Tarik Skubal", "LAD", "pitcherstrikeouts"): 7.0,
        ("Nick Lodolo", "CIN", "pitcherstrikeouts"): 2.0,
    }

    def lookup(pname: str, tm: str, pk: str):
        return store.get((pname, tm, pk))

    total = sum_multiplayer_combo_actual(
        "Tarik Skubal + Nick Lodolo",
        "LAD/CIN",
        "pitcherstrikeoutscombo",
        lookup,
        extra_aliases={"pitcherstrikeouts": ["pitcherstrikeouts", "strikeouts"]},
    )
    assert total == 9.0


def test_sum_multiplayer_combo_actual_missing_arm_returns_none():
    store = {("A", "T1", "shotsongoal"): 3.0}

    def lookup(pname: str, tm: str, pk: str):
        return store.get((pname, tm, pk))

    total = sum_multiplayer_combo_actual(
        "A + B",
        "T1/T2",
        "shotsongoalcombo",
        lookup,
    )
    assert total is None


def test_sum_multiplayer_combo_actual_soccer_passes():
    store = {
        ("Player One", "FRA", "passes"): 40.0,
        ("Player Two", "ESP", "passes"): 55.0,
    }

    def lookup(pname: str, tm: str, pk: str):
        return store.get((pname, tm, pk))

    total = sum_multiplayer_combo_actual(
        "Player One + Player Two",
        "FRA/ESP",
        "passes attempted (combo)",
        lookup,
        extra_aliases={"passes attempted": ["passes attempted", "passes"], "passes": ["passes"]},
    )
    assert total == 95.0
    assert not (isinstance(total, float) and math.isnan(total))


def test_combined_ticket_grader_prop_norm_strips_combo():
    sys.path.insert(0, str(ROOT / "scripts"))
    from combined_ticket_grader import prop_norm_from_label

    assert prop_norm_from_label("Pitcher Strikeouts (Combo)") == "pitcherstrikeouts"
    assert prop_norm_from_label("Passes Attempted (Combo)") == "passes"
    assert prop_norm_from_label("Shots On Goal (Combo)") == "shots_on_goal"
