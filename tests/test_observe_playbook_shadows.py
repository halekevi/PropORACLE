"""Observe-only playbook shadows stay out of the ticket rules."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from observe_playbook_shadows import CRITERIA, FLEX_BAR, nfl_week  # noqa: E402
from utils.observe_leg_snapshot import count_gate, leg_snapshot  # noqa: E402


def test_nfl_week_2026_opener_and_week_five():
    assert nfl_week(date(2026, 9, 10)) == 1
    assert nfl_week(date(2026, 9, 9)) is None
    assert nfl_week(date(2026, 10, 11)) == 5


def test_rule4_is_frozen_for_nfl_both_directions():
    from observe_playbook_shadows import nfl_promotion_disposition

    nfl = CRITERIA["paths"]["nfl_standard_under_3leg_flex"]
    assert nfl["rule4"]["decision"] == "apply_to_nfl_as_written"
    assert nfl["rule4"]["lines"] == [0.5, 2.5, 4.5]
    assert nfl_promotion_disposition(0.5, "SUPPORT") == "exclude"
    assert nfl_promotion_disposition(0.5, "PRIMARY") == "allow"
    assert nfl_promotion_disposition(0.5, "") == "unresolved"
    assert nfl_promotion_disposition(1.5, "SUPPORT") == "allow"
    assert nfl_promotion_disposition(4.5, "SUPPORT") == "exclude"


def test_sacks_under_picker_takes_three_distinct_players():
    from observe_playbook_shadows import pick_sacks_under_legs

    rows = [
        {"player": "Travis Jones", "team": "BAL", "opp": "LAR", "prop": "sacks", "direction": "UNDER", "line": 1.5},
        {"player": "Nate Wiggins", "team": "BAL", "opp": "LAR", "prop": "sacks", "direction": "UNDER", "line": 0.5},
        {"player": "Cameron Thomas", "team": "BAL", "opp": "LAR", "prop": "sacks", "direction": "UNDER", "line": 1.5},
        {"player": "Other DL", "team": "KC", "opp": "DET", "prop": "sacks", "direction": "UNDER", "line": 1.5},
        {"player": "Third", "team": "SF", "opp": "SEA", "prop": "sacks", "direction": "UNDER", "line": 1.5},
        {"player": "Fragile", "team": "PHI", "opp": "NYG", "prop": "sacks", "direction": "UNDER", "line": 2.5},
    ]
    picked = pick_sacks_under_legs(rows, 3)
    assert [r["player"] for r in picked] == ["Cameron Thomas", "Other DL", "Third"]


def test_sacks_quote_does_not_fill_from_the_same_game_or_a_quarter_line():
    from observe_playbook_shadows import pick_sacks_under_legs

    rows = [
        {"player": "Travis Jones", "team": "BAL", "opp": "ATL", "prop": "sacks", "direction": "UNDER", "line": 1.5},
        {"player": "Cameron Thomas", "team": "ATL", "opp": "BAL", "prop": "sacks", "direction": "UNDER", "line": 1.5},
        {"player": "Aidan Hutchinson", "team": "DET", "opp": "ARI", "prop": "sacks", "direction": "UNDER", "line": 0.25},
    ]
    assert [r["player"] for r in pick_sacks_under_legs(rows, 3)] == ["Cameron Thomas"]


def test_snap_maps_to_support_role():
    from utils.nfl_prop_defense import role_from_minutes_or_snap, role_from_snap

    assert role_from_snap(25) == "SUPPORT"
    assert role_from_snap(80) == "PRIMARY"
    assert role_from_snap(None) == ""
    assert role_from_minutes_or_snap("LOW") == ""
    assert role_from_minutes_or_snap("HIGH", None) == ""
    assert role_from_minutes_or_snap("LOW", 80) == "PRIMARY"


def test_promotion_bar_is_frozen_before_the_sample():
    nfl = CRITERIA["paths"]["nfl_standard_under_3leg_flex"]
    gob = CRITERIA["paths"]["goblin70_on_2x_plus"]
    assert CRITERIA["discovery_sample_counts"] is False
    assert CRITERIA["observe_after"] == "2026-10-11"
    assert "100 decided" in nfl["promotion_requires_all"][0]
    assert "real scraped Flex quote" in nfl["promotion_requires_all"][2]
    assert "own legs" in gob["promotion_requires_all"][3]
    assert abs(FLEX_BAR - (1 / 3) ** 0.5) < 1e-12


def test_goblin70_status_needs_a_complete_window():
    base = {
        "player": "Kelsey Mitchell",
        "prop": "Assists",
        "sport": "WNBA",
        "pick_type": "Goblin",
        "direction": "OVER",
        "line": 4.5,
        "standard_line": 6.5,
        "l5_over": 5,
        "l10_over": 8,
        "def_tier": "Below Avg",
    }
    snap = leg_snapshot(base)
    assert snap["goblin70"] is True
    assert snap["gap"] == 2.0
    missing = dict(base)
    missing["l5_over"] = ""
    assert count_gate(missing) is None
    assert leg_snapshot(missing)["goblin70"] is None
    fail = dict(base)
    fail["def_tier"] = "Elite"
    assert count_gate(fail) is False


def test_ladder_row_stores_the_quote_time_leg(monkeypatch):
    import collect_payout_data as cap

    rec = {
        "status": "ok",
        "power_min_x": 2.2,
        "ticket_id": "t1",
        "legs": [
            {
                "player": "Kelsey Mitchell",
                "prop_type": "Assists",
                "sport": "WNBA",
                "pick_type": "Goblin",
                "direction": "OVER",
                "line": 4.5,
                "standard_line": 6.5,
                "l5_over": 5,
                "l10_over": 8,
                "def_tier": "Below Avg",
            },
            {
                "player": "Other",
                "prop_type": "Points",
                "sport": "WNBA",
                "pick_type": "Goblin",
                "direction": "OVER",
                "line": 12.5,
                "standard_line": 14.5,
                "l5_over": 3,
                "l10_over": 6,
                "def_tier": "Weak",
            },
        ],
    }
    row = cap._capture_to_ladder_row(rec, "2026-10-12")
    assert row is not None
    assert len(row["legs"]) == 2
    assert row["legs"][0]["goblin70"] is True
    assert row["legs"][0]["standard_line"] == 6.5
    assert row["legs"][0]["gap"] == 2.0
    assert row["legs"][1]["goblin70"] is False
