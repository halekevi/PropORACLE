from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import fetch_tennis_actuals as fta  # noqa: E402


def test_fold_matches_diacritics():
    assert fta._fold("Taylor Fritz") == fta._fold("taylor  fritz")
    assert fta._fold("") == ""


def test_rows_from_match_emits_games_and_sets():
    rows = fta.rows_from_match(
        {
            "player": "Taylor Fritz",
            "games_won": 13,
            "match_total_games": 22,
            "aces": 8,
            "double_faults": 2,
            "sets_won": 2,
        }
    )
    props = {r["prop_type"]: r["actual"] for r in rows}
    assert props["games_won"] == 13
    assert props["match_total_games"] == 22
    assert props["sets won"] == 2.0


def test_ticketed_tennis_names_empty_without_tickets(tmp_path):
    assert fta.ticketed_tennis_names("2026-10-05", tmp_path) == {}
