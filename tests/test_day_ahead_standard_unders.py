"""Day-ahead PrizePicks Standard unders: date window, as-of tags, watchlist."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO / "scripts"
for _p in (str(_REPO), str(_SCRIPTS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import combined_slate_tickets as cst  # noqa: E402
from scripts.day_ahead_standard_under_watchlist import collect_watchlist  # noqa: E402
from utils.step1_slate_date_filter import add_calendar_days, apply_game_date_filter  # noqa: E402


def _board(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_include_tomorrow_keeps_next_et_day():
    # Sunday fetch, Monday 00:30 ET tip (still Monday Eastern).
    df = _board(
        [
            {
                "player": "Sunday Player",
                "prop_type": "Points",
                "pick_type": "Standard",
                "line": 10.5,
                "start_time": "2026-08-16T23:00:00Z",  # 19:00 ET Sunday
            },
            {
                "player": "Odyssey Sims",
                "prop_type": "Pts+Rebs+Asts",
                "pick_type": "Standard",
                "line": 17.5,
                "start_time": "2026-08-17T23:00:00Z",  # 19:00 ET Monday
            },
            {
                "player": "Tuesday Player",
                "prop_type": "Points",
                "pick_type": "Standard",
                "line": 9.5,
                "start_time": "2026-08-18T23:00:00Z",
            },
        ]
    )
    kept, _ = apply_game_date_filter(
        df,
        target_date="2026-08-16",
        tz_name="America/New_York",
        allow_nearest_future=False,
        include_tomorrow=True,
        board_date="2026-08-16",
    )
    players = set(kept["player"].astype(str))
    assert "Sunday Player" in players
    assert "Odyssey Sims" in players
    assert "Tuesday Player" not in players
    sims = kept.loc[kept["player"].eq("Odyssey Sims")].iloc[0]
    assert str(sims["game_date"]) == "2026-08-17"
    assert str(sims["board_date"]) == "2026-08-16"
    assert str(sims["line_asof"]) == "2026-08-16"


def test_without_include_tomorrow_drops_next_day():
    df = _board(
        [
            {
                "player": "Odyssey Sims",
                "prop_type": "Pts+Rebs+Asts",
                "pick_type": "Standard",
                "line": 17.5,
                "start_time": "2026-08-17T23:00:00Z",
            }
        ]
    )
    kept, _ = apply_game_date_filter(
        df,
        target_date="2026-08-16",
        tz_name="America/New_York",
        allow_nearest_future=False,
        include_tomorrow=False,
        board_date="2026-08-16",
    )
    assert kept.empty


def test_add_calendar_days():
    assert add_calendar_days("2026-08-16", 1) == "2026-08-17"


def test_enforce_target_date_keeps_wnba_extra_day():
    df = pd.DataFrame(
        {
            "player": ["A", "Sims", "C"],
            "game_time": [
                "2026-08-16T19:00:00-04:00",
                "2026-08-17T19:00:00-04:00",
                "2026-08-18T19:00:00-04:00",
            ],
            "game_date": ["2026-08-16", "2026-08-17", "2026-08-18"],
        }
    )
    kept = cst.enforce_target_date(
        df, "WNBA", "2026-08-16", extra_dates=["2026-08-17"]
    )
    assert set(kept["player"]) == {"A", "Sims"}


def test_default_day_ahead_match_date():
    assert cst.default_day_ahead_match_date("2026-08-16") == "2026-08-17"


def test_watchlist_excludes_demons_and_prefers_l5(tmp_path: Path):
    board = "2026-08-16"
    out = tmp_path / "outputs" / board / "wnba"
    out.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "player": "Odyssey Sims",
                "prop_type": "Pts+Rebs+Asts",
                "pick_type": "Standard",
                "line": 17.5,
                "game_date": "2026-08-17",
                "board_date": board,
                "line_asof": board,
                "team": "DAL",
                "opp_team": "GSV",
            },
            {
                "player": "Natasha Cloud",
                "prop_type": "Pts+Rebs+Asts",
                "pick_type": "Demon",
                "line": 17.5,
                "game_date": "2026-08-17",
                "board_date": board,
                "line_asof": board,
                "team": "NYL",
                "opp_team": "DAL",
            },
            {
                "player": "Low L5",
                "prop_type": "Points",
                "pick_type": "Standard",
                "line": 12.5,
                "game_date": "2026-08-17",
                "board_date": board,
                "line_asof": board,
                "team": "DAL",
                "opp_team": "GSV",
            },
        ]
    ).to_csv(out / "step1_wnba_props.csv", index=False)
    pd.DataFrame(
        [
            {"player": "Odyssey Sims", "prop_type": "Pts+Rebs+Asts", "last5_under": 4},
            {"player": "Low L5", "prop_type": "Points", "last5_under": 2},
        ]
    ).to_csv(out / "step4_wnba_stats.csv", index=False)

    payload = collect_watchlist(repo=tmp_path, board_date=board, sports=("WNBA",))
    names = [r["player"] for r in payload["watchlist"]]
    assert names == ["Odyssey Sims"]
    rec = payload["watchlist"][0]
    assert rec["line"] == 17.5
    assert rec["pick_type"] == "Standard"
    assert rec["side"] == "UNDER"
    assert rec["l5_under"] == 4
    assert rec["game_date"] == "2026-08-17"
    also = [r["player"] for r in payload["also_ran"]]
    assert "Low L5" in also
    assert "Natasha Cloud" not in names
    assert "Natasha Cloud" not in also


def test_watchlist_same_day_game_date_for_day_ahead_initial(tmp_path: Path):
    board = "2026-08-17"
    out = tmp_path / "outputs" / board / "wnba"
    out.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "player": "Odyssey Sims",
                "prop_type": "Pts+Rebs+Asts",
                "pick_type": "Standard",
                "line": 17.5,
                "game_date": "2026-08-17",
                "board_date": board,
                "line_asof": board,
                "team": "DAL",
                "opp_team": "GSV",
            },
            {
                "player": "Next Day",
                "prop_type": "Points",
                "pick_type": "Standard",
                "line": 9.5,
                "game_date": "2026-08-18",
                "board_date": board,
                "line_asof": board,
                "team": "NYL",
                "opp_team": "DAL",
            },
        ]
    ).to_csv(out / "step1_wnba_props.csv", index=False)
    pd.DataFrame(
        [{"player": "Odyssey Sims", "prop_type": "Pts+Rebs+Asts", "last5_under": 5}]
    ).to_csv(out / "step4_wnba_stats.csv", index=False)

    payload = collect_watchlist(
        repo=tmp_path, board_date=board, sports=("WNBA",), game_date=board
    )
    assert payload["game_date"] == board
    names = [r["player"] for r in payload["watchlist"]]
    assert names == ["Odyssey Sims"]
    assert "Next Day" not in names
    assert "Next Day" not in [r["player"] for r in payload["also_ran"]]


def test_extra_match_dates_for_sport():
    args = argparse.Namespace(date="2026-08-16", wnba_date="2026-08-17", mlb_date="2026-08-16")
    assert cst.extra_match_dates_for_sport("WNBA", args) == ["2026-08-17"]
    assert cst.extra_match_dates_for_sport("MLB", args) == []
