"""Unique-game dedup for NFL prop×gate ledger."""

from __future__ import annotations

import pandas as pd

from utils.nfl_prop_gate_ledger import (
    FACE_MIN_ROWS,
    UNVERIFIED_PICK,
    build_prop_gate_ledger,
    dedupe_unique_player_games,
    prepare_decided,
)


def _box(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_dedupe_collapses_ladder_lines():
    box = _box(
        {
            "player": "QB A",
            "prop_type_norm": "longest_completion",
            "bet_direction": "UNDER",
            "pick_type": "Goblin",
            "line": 24.5,
            "actual": 39.0,
            "result": "MISS",
            "l5_under": 5,
            "l10_under": 10,
            "def_tier": "Elite",
            "slate": "2026-09-21",
        },
        {
            "player": "QB A",
            "prop_type_norm": "longest_completion",
            "bet_direction": "UNDER",
            "pick_type": "Goblin",
            "line": 49.5,
            "actual": 39.0,
            "result": "HIT",
            "l5_under": 5,
            "l10_under": 10,
            "def_tier": "Elite",
            "slate": "2026-09-21",
        },
        {
            "player": "QB A",
            "prop_type_norm": "longest_completion",
            "bet_direction": "UNDER",
            "pick_type": "Goblin",
            "line": 69.5,
            "actual": 39.0,
            "result": "HIT",
            "l5_under": 5,
            "l10_under": 10,
            "def_tier": "Elite",
            "slate": "2026-09-21",
        },
    )
    dec = prepare_decided([box])
    uniq = dedupe_unique_player_games(dec)
    assert len(dec) == 3
    assert len(uniq) == 1
    # median of 24.5/49.5/69.5 = 49.5 → HIT representative
    assert float(uniq.iloc[0]["line_f"]) == 49.5
    assert uniq.iloc[0]["result"] == "HIT"

    led = build_prop_gate_ledger([box])
    row = led[(led["prop"] == "longest_completion") & (led["pick_type"] == "Goblin")].iloc[0]
    assert int(row["n"]) == 1
    assert int(row["ladder_n"]) == 3
    assert float(row["hr"]) == 100.0
    assert float(row["ladder_hr"]) == 66.7


def test_standard_and_goblin_dedupe_separately():
    box = _box(
        {
            "player": "WR A",
            "prop_type_norm": "receiving_yards",
            "bet_direction": "UNDER",
            "pick_type": "Standard",
            "line": 60.5,
            "actual": 40.0,
            "result": "HIT",
            "l5_under": 5,
            "l10_under": 9,
            "def_tier": "Elite",
            "slate": "2026-09-21",
        },
        {
            "player": "WR A",
            "prop_type_norm": "receiving_yards",
            "bet_direction": "UNDER",
            "pick_type": "Goblin",
            "line": 39.5,
            "actual": 40.0,
            "result": "MISS",
            "l5_under": 5,
            "l10_under": 9,
            "def_tier": "Elite",
            "slate": "2026-09-21",
        },
    )
    led = build_prop_gate_ledger([box])
    assert len(led) == 2
    std = led[led["pick_type"] == "Standard"].iloc[0]
    gob = led[led["pick_type"] == "Goblin"].iloc[0]
    assert int(std["n"]) == 1 and float(std["hr"]) == 100.0
    assert int(gob["n"]) == 1 and float(gob["hr"]) == 0.0


def test_blank_or_defaulted_pick_type_never_becomes_standard():
    # No pick_type column at all -> every row is Unverified, not Standard.
    box = _box(
        {"player": "WR A", "prop_type_norm": "receptions", "bet_direction": "UNDER", "line": 4.5,
         "actual": 3.0, "result": "HIT", "l5_under": 5, "l10_under": 9, "def_tier": "Elite", "slate": "2026-09-14"},
    )
    dec = prepare_decided([box])
    assert dec["pick"].tolist() == [UNVERIFIED_PICK]
    assert not dec["face_verified"].any()

    # Column present but a whole date has >= FACE_MIN_ROWS rows and zero Goblin/Demon
    # -> defaulted upstream, the whole date is Unverified.
    rows = []
    for i in range(FACE_MIN_ROWS):
        rows.append(
            {"player": f"P{i}", "prop_type_norm": "receiving_yards", "bet_direction": "OVER",
             "pick_type": "Standard", "line": 4.5 + 27 * (i % 3), "actual": 40.0,
             "result": "HIT" if i % 2 else "MISS", "l5_over": 5, "l10_over": 8,
             "def_tier": "Weak", "slate": "2026-09-09"},
        )
    rows.append(
        {"player": "RB Z", "prop_type_norm": "rushing_yards", "bet_direction": "OVER",
         "pick_type": "Goblin", "line": 19.5, "actual": 40.0, "result": "HIT",
         "l5_over": 5, "l10_over": 8, "def_tier": "Weak", "slate": "2026-09-20"},
    )
    dec = prepare_decided([_box(*rows)])
    sep9 = dec[dec["slate"] == "2026-09-09"]
    assert (sep9["pick"] == UNVERIFIED_PICK).all()
    assert not sep9["face_verified"].any()
    sep20 = dec[dec["slate"] == "2026-09-20"]
    assert sep20["pick"].tolist() == ["Goblin"]
    assert sep20["face_verified"].all()
