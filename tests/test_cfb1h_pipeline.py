"""CFB1H board stays separate; L5 uses first-half PBP only."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_CFB = _ROOT / "Sports" / "CFB"
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_CFB) not in sys.path:
    sys.path.insert(0, str(_CFB))

from prizepicks_league_ids import classify_pp_league_name, pick_league_id_from_rows
from scripts.espn_boxscore_cache import sport_key_from_espn_league


def _load_1h_pbp():
    path = _CFB / "scripts" / "pipeline" / "cfb_1h_pbp.py"
    spec = importlib.util.spec_from_file_location("cfb_1h_pbp", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_classify_cfb1h_league_names():
    assert classify_pp_league_name("CFB1H") == "cfb1h"
    assert classify_pp_league_name("NCAAF 1H") == "cfb1h"
    assert classify_pp_league_name("CFB") == "cfb"
    assert classify_pp_league_name("CFB2H") == "cfb2h"


def test_pick_cfb1h_league_id():
    rows = [
        {"id": "15", "attributes": {"name": "CFB", "projections_count": 4000}},
        {"id": "184", "attributes": {"name": "CFB1H", "projections_count": 139}},
    ]
    assert pick_league_id_from_rows("cfb1h", rows) == "184"
    assert pick_league_id_from_rows("cfb", rows) == "15"


def test_sport_key_cfb1h_not_full_game():
    assert sport_key_from_espn_league("college-football", "1H") == "cfb1h"
    assert sport_key_from_espn_league("college-football", "") == "cfb"


def test_1st_half_duration_not_tagged_2h():
    import pandas as pd

    s = pd.Series(["1st Half", "2nd Half", "2nd Quarter", "2H", "full"])
    h2 = s.str.contains(r"2nd\s*(?:Half|Quarter)|2H", case=False, na=False)
    assert list(h2) == [False, True, True, True, False]


def test_parse_1h_ignores_second_half_yards():
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"id": "61"},
                    "statistics": [
                        {
                            "athletes": [
                                {
                                    "athlete": {
                                        "id": "1",
                                        "displayName": "Gunner Stockton",
                                        "jersey": "14",
                                    }
                                }
                            ]
                        }
                    ],
                }
            ]
        },
        "drives": {
            "previous": [
                {
                    "plays": [
                        {
                            "period": {"number": 1},
                            "type": {"text": "Pass Reception"},
                            "text": "#14 G. Stockton pass complete to #1 A. Smith for 12 yards",
                            "statYardage": 12,
                            "teamParticipants": [{"id": "61", "type": "offense"}],
                        },
                        {
                            "period": {"number": 3},
                            "type": {"text": "Pass Reception"},
                            "text": "#14 G. Stockton pass complete to #1 A. Smith for 80 yards",
                            "statYardage": 80,
                            "teamParticipants": [{"id": "61", "type": "offense"}],
                        },
                    ]
                }
            ]
        },
    }
    rows = _load_1h_pbp().parse_1h_players(summary, game_date="20250901", event_id="401752786")
    assert rows
    qb = next(r for r in rows if r.get("espn_athlete_id") == "1")
    assert qb.get("segment") == "1H"
    assert float(qb.get("PASS_YDS") or 0) == 12.0
    assert float(qb.get("PASS_YDS") or 0) != 92.0
