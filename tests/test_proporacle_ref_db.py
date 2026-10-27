"""Canonical ref DB path + last-completed-slate freshness."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from utils.proporacle_ref_db import (
    REF_DB_ENV,
    assert_ref_db_fresh,
    last_completed_nba_slate,
    resolve_ref_db,
)


def test_last_completed_skips_off_nights():
    finals = {date(2026, 10, 24)}

    def has_final(d: date):
        return d in finals

    assert last_completed_nba_slate(
        date(2026, 10, 27), has_final=has_final
    ) == date(2026, 10, 24)


def test_last_completed_excludes_as_of_tonight():
    finals = {date(2026, 10, 26), date(2026, 10, 27)}

    def has_final(d: date):
        return d in finals

    assert last_completed_nba_slate(
        date(2026, 10, 27), has_final=has_final
    ) == date(2026, 10, 26)


def test_fresh_when_db_matches_last_completed_not_yesterday():
    finals = {date(2026, 10, 24)}

    def has_final(d: date):
        return d in finals

    mx = assert_ref_db_fresh(
        as_of=date(2026, 10, 27),
        db_max=date(2026, 10, 24),
        has_final=has_final,
        path=Path("unused.db"),
    )
    assert mx == date(2026, 10, 24)


def test_stale_when_missing_last_completed_slate():
    with pytest.raises(RuntimeError, match="last_completed_slate=2026-10-26"):
        assert_ref_db_fresh(
            as_of=date(2026, 10, 27),
            db_max=date(2026, 10, 24),
            last_completed=date(2026, 10, 26),
            path=Path("unused.db"),
        )


def test_resolve_fails_when_sibling_newer(tmp_path, monkeypatch):
    import sqlite3

    from utils import proporacle_ref_db as mod

    older = tmp_path / "old" / "proporacle_ref.db"
    newer = tmp_path / "new" / "proporacle_ref.db"
    older.parent.mkdir()
    newer.parent.mkdir()
    for path, day in ((older, "2026-10-24"), (newer, "2026-10-26")):
        con = sqlite3.connect(str(path))
        con.execute("CREATE TABLE nba (game_date TEXT)")
        con.execute("INSERT INTO nba VALUES (?)", (day,))
        con.commit()
        con.close()

    monkeypatch.setenv(REF_DB_ENV, str(older))
    monkeypatch.setattr(mod, "worktree_ref_dbs", lambda root=None: [older, newer])
    with pytest.raises(RuntimeError, match="stale vs sibling"):
        resolve_ref_db(root=tmp_path)
