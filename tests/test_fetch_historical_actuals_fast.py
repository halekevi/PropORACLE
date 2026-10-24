"""Fast-path helpers for fetch_historical_actuals (A1)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts import fetch_historical_actuals as fha


def test_path_within_active_slate_days_uses_folder_date():
    ref = datetime(2026, 8, 18, tzinfo=timezone.utc)
    recent = Path("outputs/2026-08-17/soccer/step8_soccer_direction_clean.xlsx")
    old = Path("outputs/2026-07-01/soccer/step8_soccer_direction_clean.xlsx")
    assert fha._path_within_active_slate_days(recent, 21, ref) is True
    assert fha._path_within_active_slate_days(old, 21, ref) is False


def test_player_needs_fetch_skips_when_current_season_through_yesterday():
    conn = sqlite3.connect(":memory:")
    fha.create_db(conn)
    seasons = (20232024, 20242025, 20252026)
    current = 20252026
    player, sport = "Test Player", "Soccer"
    today = datetime.now(timezone.utc).date()
    yesterday = (today - timedelta(days=1)).isoformat()
    for label, gdate in (
        ("2023-24", "2023-01-01"),
        ("2024-25", "2024-01-01"),
        ("2025-26", yesterday),
    ):
        conn.execute(
            """
            INSERT INTO player_game_logs
            (player_name, player_id, sport, game_date, season, source, created_at)
            VALUES (?, '1', ?, ?, ?, 'test', '2026-01-01')
            """,
            (player, sport, gdate, label),
        )
    conn.commit()
    assert (
        fha.player_needs_fetch(conn, player, sport, seasons, current, False, inactive_skip_days=14)
        is False
    )


def test_resolve_soccer_espn_id_cached_uses_csv(monkeypatch: pytest.MonkeyPatch):
    cache = {"erling haaland": ("123", "erling haaland")}
    eid, name, from_cache = fha.resolve_soccer_espn_id_cached("Erling Haaland", cache, {})
    assert eid == "123"
    assert from_cache is True

    def _fail_search(_name: str):
        raise AssertionError("search should not run when cache hits")

    monkeypatch.setattr(fha, "search_soccer_espn_id", _fail_search)
    eid2, _, from_cache2 = fha.resolve_soccer_espn_id_cached("Erling Haaland", cache, {})
    assert eid2 == "123"
    assert from_cache2 is True
