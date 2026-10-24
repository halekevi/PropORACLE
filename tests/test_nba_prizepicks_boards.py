"""NBA PrizePicks boards: regular + NBAP preseason."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_NBA_ROOT = _REPO / "Sports" / "NBA"
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

_ids_spec = importlib.util.spec_from_file_location(
    "nba_prizepicks_league_ids",
    _NBA_ROOT / "prizepicks_league_ids.py",
)
_nba_ids = importlib.util.module_from_spec(_ids_spec)
assert _ids_spec.loader is not None
_ids_spec.loader.exec_module(_nba_ids)

DEFAULT_NBA_BOARDS = _nba_ids.DEFAULT_NBA_BOARDS
NBA = _nba_ids.NBA
NBAP = _nba_ids.NBAP
NBASZN = _nba_ids.NBASZN
GAME_FAMILY_IDS = _nba_ids.GAME_FAMILY_IDS
is_nbap = _nba_ids.is_nbap
resolve_nba_fetch_boards = _nba_ids.resolve_nba_fetch_boards


def test_default_boards_include_nbap_not_szn():
    assert DEFAULT_NBA_BOARDS[NBA] == "NBA"
    assert DEFAULT_NBA_BOARDS[NBAP] == "NBAP"
    assert NBASZN not in DEFAULT_NBA_BOARDS
    assert NBA in GAME_FAMILY_IDS
    assert NBAP in GAME_FAMILY_IDS
    assert NBAP == "237"


def test_nba_league_id_seven_still_fetches_nbap():
    boards = resolve_nba_fetch_boards(league_id="7")
    assert NBA in boards
    assert NBAP in boards


def test_nbap_league_id_still_fetches_nba():
    boards = resolve_nba_fetch_boards(league_id="237")
    assert NBA in boards
    assert NBAP in boards


def test_wnba_league_id_stays_single_board():
    boards = resolve_nba_fetch_boards(league_id="3")
    assert boards == {"3": "WNBA"}
    assert NBAP not in boards


def test_nba1h_league_id_stays_single_board():
    boards = resolve_nba_fetch_boards(league_id="84")
    assert boards == {"84": "NBA1H"}


def test_no_preseason_drops_nbap():
    boards = resolve_nba_fetch_boards(league_id="7", include_preseason=False)
    assert NBA in boards
    assert NBAP not in boards


def test_is_nbap():
    assert is_nbap("NBAP")
    assert is_nbap(league_id="237")
    assert is_nbap("NBA PRESEASON")
    assert not is_nbap("NBA", "7")


def test_board_size_ok_nbap_only():
    step1_path = _NBA_ROOT / "scripts" / "step1_fetch_prizepicks_api.py"
    spec = importlib.util.spec_from_file_location("nba_step1_fetch_prizepicks_api", step1_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    df = pd.DataFrame(
        {
            "team": ["BOS", "NYK"] * 5,
            "league": ["NBAP"] * 10,
        }
    )
    assert mod._board_size_ok(df, min_rows=50, min_teams=2)
    thin = pd.DataFrame({"team": ["BOS"], "league": ["NBA"]})
    assert not mod._board_size_ok(thin, min_rows=50, min_teams=2)


def test_nbap_blocked_from_goblin70_and_standard():
    from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible

    base = {
        "sport": "NBA",
        "league": "NBAP",
        "player": "Jayson Tatum",
        "prop_type": "Points",
        "line": 19.5,
        "l5_over": 5,
        "l10_over": 8,
        "def": "Weak",
        "def_tier": "Weak",
        "final_bet_direction": "OVER",
        "direction": "OVER",
        "side": "OVER",
    }
    gob = {**base, "pick_type": "Goblin"}
    std = {**base, "pick_type": "Standard"}
    assert not goblin_70_eligible(gob)
    assert not standard_ticket_eligible(std)
