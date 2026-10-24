"""Game-lines freshness gate."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.game_lines_freshness import (
    assert_game_lines_fresh,
    et_today,
    expected_sports_for_date,
    game_lines_freshness_report,
)

ET = ZoneInfo("America/New_York")


def _seed_db(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE game_lines (
            sport TEXT, game_date TEXT, home_team TEXT, away_team TEXT,
            spread REAL, total REAL, home_win_prob REAL, away_win_prob REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO game_lines (sport, game_date, home_team, away_team, spread, total, "
        "home_win_prob, away_win_prob) VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_early_sep_cache_stale_on_late_sep_slate(tmp_path: Path):
    db = tmp_path / "game_lines.db"
    _seed_db(
        db,
        [
            ("MLB", "2026-09-05", "NYY", "BOS", -1.5, 8.5, 0.6, 0.4),
            ("Soccer", "2026-09-05", "ARS", "CHE", -0.5, 2.5, 0.55, 0.45),
        ],
    )
    # Touch mtime to "today" so file age is not the signal — max_game_date is.
    import os

    now = datetime.now(ET).timestamp()
    os.utime(db, (now, now))

    rep = game_lines_freshness_report(
        slate_date="2026-09-24",
        db_path=db,
        sports=["MLB", "Soccer"],
    )
    assert not rep["ok"]
    assert "MLB" in rep["stale"] or "MLB" in rep["missing"]
    assert "Soccer" in rep["stale"] or "Soccer" in rep["missing"]


def test_same_slate_rows_pass(tmp_path: Path):
    db = tmp_path / "game_lines.db"
    day = "2026-09-24"
    _seed_db(
        db,
        [
            ("MLB", day, "NYY", "BOS", -1.5, 8.5, 0.6, 0.4),
            ("Soccer", day, "ARS", "CHE", -0.5, 2.5, 0.55, 0.45),
            ("NHL", day, "NYR", "BOS", -1.5, 6.0, 0.58, 0.42),
        ],
    )
    rep = game_lines_freshness_report(
        slate_date=day,
        db_path=db,
        sports=["MLB", "Soccer", "NHL"],
    )
    assert rep["ok"]
    assert_game_lines_fresh(slate_date=day, db_path=db)


def test_expected_sports_sep_includes_mlb_soccer_not_nba():
    sports = expected_sports_for_date("2026-09-24")
    assert "MLB" in sports
    assert "Soccer" in sports
    assert "NBA" not in sports  # resumes Oct 1
    assert "NHL" not in sports  # regular resumes ~Oct 8
