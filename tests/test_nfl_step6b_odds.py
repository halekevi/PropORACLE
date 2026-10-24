"""NFL step6b odds cache + ESPN line parse."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "Sports" / "NFL" / "scripts"))

from step6b_attach_game_context_nfl import (  # noqa: E402
    espn_lines_lookup,
    find_odds,
    odds_cache_usable,
)
from utils.nfl_espn_context import parse_espn_nfl_lines  # noqa: E402


def test_odds_cache_rejects_blank_fallback():
    blank = pd.DataFrame(
        {
            "team": ["KC"],
            "opp_team": ["IND"],
            "game_total": [None],
            "implied_team_total": [None],
            "odds_source": ["fallback"],
        }
    )
    assert not odds_cache_usable(blank)
    live = pd.DataFrame({"game_total": [47.5], "implied_team_total": [24.0], "odds_source": ["espn"]})
    assert odds_cache_usable(live)


def test_espn_lines_lookup_abbr():
    rows = [{"home": "KC", "away": "NYG", "game_total": 47.5, "home_spread": -6.5}]
    lookup = espn_lines_lookup(rows)
    rec = find_odds("KC", "NYG", lookup)
    assert rec is not None
    assert rec["game_total"] == 47.5
    assert rec["spread"] == -6.5
    rec_away = find_odds("NYG", "KC", lookup)
    assert rec_away is not None
    assert rec_away["spread"] == 6.5


def test_parse_espn_nfl_lines_from_scoreboard():
    payload = {
        "events": [
            {
                "id": "1",
                "competitions": [
                    {
                        "competitors": [
                            {"homeAway": "home", "team": {"abbreviation": "KC"}},
                            {"homeAway": "away", "team": {"abbreviation": "NYG"}},
                        ],
                        "odds": [{"spread": -6.5, "overUnder": 47.5, "details": "Kansas City -6.5"}],
                    }
                ],
            }
        ]
    }
    rows = parse_espn_nfl_lines(payload, fetch_missing_core=False)
    assert len(rows) == 1
    assert rows[0]["home"] == "KC"
    assert rows[0]["away"] == "NYG"
    assert rows[0]["game_total"] == 47.5
    assert rows[0]["home_spread"] == -6.5
