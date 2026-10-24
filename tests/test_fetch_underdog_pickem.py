"""Underdog pick'em fetch: v5 alternate rungs map to Goblin/Demon."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from fetch_underdog_pickem import _pick_type, build_rows  # noqa: E402


def _line(val: str, lt: str, ouid: str, lid: str) -> dict:
    return {
        "id": lid,
        "stable_id": f"{ouid}|{lt}|{val}" if lt == "alternate" else f"{ouid}|{lt}",
        "line_type": lt,
        "stat_value": val,
        "status": "active",
        "over_under_id": ouid,
        "over_under": {
            "id": ouid,
            "category": "player_prop",
            "has_alternates": True,
            "appearance_stat": {
                "appearance_id": "a1",
                "display_stat": "Games Played",
                "stat": "games_played",
            },
        },
        "options": [],
    }


def _payload() -> dict:
    return {
        "games": [],
        "solo_games": [{"id": "sg1", "sport_id": "TENNIS", "scheduled_at": "2026-08-31T16:40:00Z"}],
        "players": [{"id": "p1", "first_name": "Carlos", "last_name": "Alcaraz", "team_id": ""}],
        "appearances": [
            {"id": "a1", "player_id": "p1", "match_type": "SoloGame", "match_id": "sg1", "team_id": ""}
        ],
        "over_under_lines": [
            _line("36.5", "balanced", "ou1", "l-bal"),
            _line("34.5", "alternate", "ou1", "l-gob"),
            _line("38.5", "alternate", "ou1", "l-dem"),
        ],
    }


def test_pick_type_alternate_vs_balanced():
    assert _pick_type({"stat_value": "36.5", "line_type": "balanced"}) == "Standard"
    assert _pick_type({"stat_value": "34.5", "line_type": "alternate"}, 36.5) == "Goblin"
    assert _pick_type({"stat_value": "38.5", "line_type": "alternate"}, 36.5) == "Demon"


def test_pick_type_alternate_uses_higher_payout_when_no_main():
    gob = {
        "stat_value": "34.5",
        "line_type": "alternate",
        "options": [{"choice": "higher", "payout_multiplier": "0.88"}],
    }
    dem = {
        "stat_value": "38.5",
        "line_type": "alternate",
        "options": [{"choice": "higher", "payout_multiplier": "1.11"}],
    }
    assert _pick_type(gob) == "Goblin"
    assert _pick_type(dem) == "Demon"


def test_build_rows_includes_alternate_rungs():
    rows = build_rows(_payload(), {"TENNIS"})
    by_line = {float(r["line"]): r for r in rows}
    assert set(by_line) == {34.5, 36.5, 38.5}
    assert by_line[36.5]["pick_type"] == "Standard"
    assert by_line[36.5]["ud_line_type"] == "balanced"
    assert by_line[34.5]["pick_type"] == "Goblin"
    assert by_line[34.5]["ud_line_type"] == "alternate"
    assert by_line[38.5]["pick_type"] == "Demon"
    assert by_line[38.5]["player"] == "Carlos Alcaraz"
    assert by_line[34.5]["prop_type"] == "Games Played"
