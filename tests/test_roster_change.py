"""roster_changed flag: trade / team-churn vs L5 boxes."""
from __future__ import annotations

import pandas as pd

from utils.leg_flags import collect_leg_flags
from utils.roster_change import (
    attach_roster_change_columns,
    norm_nba_team,
    roster_change_from_row,
)


def test_norm_ny_to_nyk():
    assert norm_nba_team("NY") == "NYK"
    assert norm_nba_team("nyk") == "NYK"
    assert norm_nba_team("BOS") == "BOS"


def test_flag_reads_roster_changed_column():
    flags = collect_leg_flags(
        values=[13, 19, 16, 17, 19],
        line=21.5,
        side="UNDER",
        pick="Standard",
        sport="NBA",
        prop="FG Attempted",
        row={
            "roster_changed": 1,
            "roster_note": "BOS: out Jaylen Brown; in Paul George, Mitchell Robinson",
            "player": "Jayson Tatum",
            "team": "BOS",
        },
        wilson=0.55,
        slice_n=80,
    )
    assert "roster_changed" in flags


def test_attach_flags_boston_churn_and_trades():
    """Live DB: Brown/Robinson swapped; Tatum stays BOS but team churned."""
    df = pd.DataFrame(
        [
            {"player": "Jayson Tatum", "team": "BOS", "prop_type": "FG Attempted", "line": 21.5},
            {"player": "Mitchell Robinson", "team": "BOS", "prop_type": "Pts+Rebs", "line": 14.5},
            {"player": "Jaylen Brown", "team": "PHI", "prop_type": "Turnovers", "line": 2.5},
            {"player": "Paul George", "team": "BOS", "prop_type": "Points", "line": 15.5},
            {"player": "Payton Pritchard", "team": "BOS", "prop_type": "Turnovers", "line": 1.5},
            {"player": "Neemias Queta", "team": "BOS", "prop_type": "Rebounds", "line": 5.5},
        ]
    )
    out = attach_roster_change_columns(df, sport="NBA", slate_date="2026-10-20")
    by = {r["player"]: r for _, r in out.iterrows()}

    assert int(by["Jaylen Brown"]["roster_changed"]) == 1
    assert "L5 on BOS" in str(by["Jaylen Brown"]["roster_note"])
    assert "PHI" in str(by["Jaylen Brown"]["roster_note"])

    assert int(by["Mitchell Robinson"]["roster_changed"]) == 1
    note_r = str(by["Mitchell Robinson"]["roster_note"])
    assert "L5 on" in note_r and "BOS" in note_r

    assert int(by["Jayson Tatum"]["roster_changed"]) == 1
    assert "out" in str(by["Jayson Tatum"]["roster_note"]).lower()
    assert "Jaylen Brown" in str(by["Jayson Tatum"]["roster_note"])

    assert int(by["Payton Pritchard"]["roster_changed"]) == 1
    assert int(by["Neemias Queta"]["roster_changed"]) == 1

    assert roster_change_from_row(by["Paul George"]) is True
