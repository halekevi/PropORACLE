"""Core odds are the only spread/total source. Details text is not a line."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from fetch_game_lines import (  # noqa: E402
    init_db,
    line_from_core_item,
    purge_bad_soccer_spreads,
)
from utils.pp_vegas_gap import (  # noqa: E402
    apply_game_lines_to_prop,
    build_game_index,
    team_perspective_spread,
)


def test_core_item_ignores_details_moneyline():
    parsed = line_from_core_item(
        {
            "spread": 1.5,
            "overUnder": 2.5,
            "details": "MAN -270",
            "provider": {"id": "100", "name": "DraftKings"},
            "homeTeamOdds": {"moneyLine": {"american": "-150"}},
            "awayTeamOdds": {"moneyLine": 130},
        }
    )
    assert parsed["spread"] == 1.5
    assert parsed["total"] == 2.5
    assert parsed["provider"] == "DraftKings"
    assert parsed["home_win_prob"] is not None
    assert parsed["away_win_prob"] is not None
    assert parsed["draw_win_prob"] is None
    assert abs((parsed["home_win_prob"] + parsed["away_win_prob"]) - 1) < 1e-9


def test_soccer_three_way_wp_normalizes_home_draw_away():
    """Outright win probs; draw is in the denominator, not folded into either side."""
    parsed = line_from_core_item(
        {
            "spread": -1.5,
            "overUnder": 3.5,
            "details": "WHU -270",
            "provider": {"name": "DraftKings"},
            "homeTeamOdds": {"moneyLine": -270},
            "awayTeamOdds": {"moneyLine": 700},
            "drawOdds": {"moneyLine": 425.0},
        }
    )
    ph = 270.0 / (270.0 + 100.0)
    pa = 100.0 / (700.0 + 100.0)
    pd = 100.0 / (425.0 + 100.0)
    s = ph + pa + pd
    assert parsed["draw_win_prob"] is not None
    assert abs(parsed["home_win_prob"] - ph / s) < 1e-9
    assert abs(parsed["away_win_prob"] - pa / s) < 1e-9
    assert abs(parsed["draw_win_prob"] - pd / s) < 1e-9
    assert abs(
        parsed["home_win_prob"] + parsed["away_win_prob"] + parsed["draw_win_prob"] - 1.0
    ) < 1e-9
    # Two-way would have inflated home to ph/(ph+pa) ≈ 0.85; three-way is lower.
    assert parsed["home_win_prob"] < ph / (ph + pa) - 0.05


def test_missing_spread_stays_missing_when_details_looks_like_odds():
    parsed = line_from_core_item(
        {
            "spread": None,
            "overUnder": 2.5,
            "details": "MAN -270",
            "homeTeamOdds": {},
            "awayTeamOdds": {},
        }
    )
    assert parsed["spread"] is None
    assert parsed["total"] == 2.5


def test_purge_deletes_moneyline_spreads_and_keeps_goal_lines():
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    conn.execute(
        """
        INSERT INTO game_lines (
            sport, game_date, home_team, away_team, spread, total, source, created_at, updated_at
        ) VALUES
            ('Soccer', '2026-05-01', 'MIA', 'PHI', -260, 2.5, 'espn', '', ''),
            ('Soccer', '2026-05-02', 'LAFC', 'SEA', 1.5, 2.5, 'espn_core', '', ''),
            ('NBA', '2026-05-01', 'BOS', 'NY', -12.5, 220.5, 'espn', '', '')
        """
    )
    dropped = purge_bad_soccer_spreads(conn)
    assert dropped == 1
    left = conn.execute("SELECT sport, spread FROM game_lines ORDER BY sport").fetchall()
    assert left == [("NBA", -12.5), ("Soccer", 1.5)]


def test_is_closing_follows_event_not_store_time():
    from fetch_game_lines import _is_closing_from_feed

    final = {"status": {"type": {"state": "post"}}}
    pre = {"status": {"type": {"state": "pre"}}}
    assert _is_closing_from_feed(final, {"spread": -1.5}) == 1
    assert _is_closing_from_feed(pre, {"spread": -1.5}) == 0
    assert _is_closing_from_feed(pre, {"isClose": True}) == 1
    assert _is_closing_from_feed(final, {"status": "open"}) == 0


def test_away_spread_is_signed_for_the_players_team():
    assert team_perspective_spread(-7.5, player_is_home=True) == -7.5
    assert team_perspective_spread(-7.5, player_is_home=False) == 7.5
    index = build_game_index(
        [
            {
                "sport": "NBA",
                "home_team": "SA",
                "away_team": "LAL",
                "home_team_id": "24",
                "away_team_id": "13",
                "spread": -7.5,
                "total": 225.5,
                "home_win_prob": 0.72,
                "away_win_prob": 0.28,
                "provider": "DraftKings",
                "is_closing": 1,
                "captured_at": "2026-04-01T00:00:00Z",
            }
        ],
        abbrs_for_id={("NBA", "24"): ["SA", "SAS"], ("NBA", "13"): ["LAL"]},
    )
    prop = {"sport": "NBA", "team": "SAS", "player": "Victor Wembanyama"}
    assert apply_game_lines_to_prop(
        prop,
        index,
        abbr_to_ids={("NBA", "SAS"): {"24"}, ("NBA", "SA"): {"24"}},
    )
    assert prop["game_spread"] == "-7.5"
    assert prop["game_total"] == "225.5"
    assert prop["team_win_prob"] == "0.72"
    assert prop["line_provider"] == "DraftKings"
    assert prop["line_is_closing"] == "1"

    away = {"sport": "NBA", "team": "LAL"}
    assert apply_game_lines_to_prop(away, index, abbr_to_ids={("NBA", "LAL"): {"13"}})
    assert away["game_spread"] == "7.5"
    assert away["team_win_prob"] == "0.28"
