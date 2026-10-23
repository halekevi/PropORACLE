"""Bet-window labels and append-only payout scrape log."""
from __future__ import annotations

from pathlib import Path

from utils.bet_windows import (
    append_payout_scrape_log,
    rebuild_bet_windows,
    summarize_fetch_window,
    window_label,
)


def test_window_label_maps_refresh_slots():
    assert window_label("2026-08-27T01:05:00-04:00") == "1AM"
    assert window_label("2026-08-27T08:02:00-04:00") == "8AM"
    assert window_label("2026-08-27T09:00:00-04:00") == "9AM"
    assert window_label("2026-08-27T09:47:00-04:00") == "9:45"
    assert window_label("2026-08-27T10:32:00-04:00") == "10:30"
    assert window_label("2026-08-27T13:04:00-04:00") == "1PM"
    assert window_label("2026-08-27T16:31:00-04:00") == "4:30"
    assert window_label("2026-08-27T21:04:00-04:00") == "9PM"
    assert window_label("2026-08-27T05:03:00-04:00") == "5AM"


def test_job_window_uses_1am_and_8am_labels(monkeypatch):
    from utils.bet_windows import job_window_label

    monkeypatch.setenv("PROPORACLE_BET_WINDOW", "1AM")
    assert job_window_label("2026-08-27T02:40:00-04:00") == "1AM"
    monkeypatch.setenv("PROPORACLE_BET_WINDOW", "8AM")
    assert job_window_label("2026-08-27T09:20:00-04:00") == "8AM"
    monkeypatch.setenv("PROPORACLE_BET_WINDOW", "9PM")
    assert job_window_label("2026-08-27T21:40:00-04:00") == "9PM"
    monkeypatch.setenv("PROPORACLE_BET_WINDOW", "DAY_AHEAD")
    assert job_window_label("2026-08-27T21:40:00-04:00") == "9PM"
    monkeypatch.delenv("PROPORACLE_BET_WINDOW", raising=False)
    assert job_window_label("2026-08-27T08:02:00-04:00", explicit="8AM") == "8AM"


def test_summarize_force_payout_only_when_moved(monkeypatch):

    pulls = [
        {"fetched_at": "2026-08-28T01:05:00-04:00", "window": "1AM", "n_props": 10, "n_moved": 0},
        {"fetched_at": "2026-08-28T08:02:00-04:00", "window": "8AM", "n_props": 10, "n_moved": 0},
        {"fetched_at": "2026-08-28T09:47:00-04:00", "window": "9:45", "n_props": 12, "n_moved": 4},
    ]
    monkeypatch.setattr("utils.bet_windows._line_pulls", lambda date: pulls)
    monkeypatch.setattr(
        "utils.bet_windows._payout_windows",
        lambda date: [{"window": "1AM", "n_scrapes": 1}],
    )
    first = summarize_fetch_window("2026-08-28", "1AM")
    assert first["is_initial_stamp"] is True
    assert first["force_payout"] is True
    eight = summarize_fetch_window("2026-08-28", "8AM")
    assert eight["is_initial_stamp"] is False
    assert eight["n_moved_this_window"] == 0
    assert eight["force_payout"] is False
    assert eight["rebuild_tickets"] is False
    later = summarize_fetch_window("2026-08-28", "945AM")
    assert later["n_moved_this_window"] == 4
    assert later["n_moved_from_initial"] == 4
    assert later["force_payout"] is True


def test_summarize_empty_payout_log_does_not_force_midday(monkeypatch):
    """Quiet mid-day boards must stay Auto even with no jsonl yet."""

    pulls = [
        {"fetched_at": "2026-08-28T01:05:00-04:00", "window": "1AM", "n_props": 10, "n_moved": 0},
        {"fetched_at": "2026-08-28T08:02:00-04:00", "window": "8AM", "n_props": 10, "n_moved": 0},
    ]
    monkeypatch.setattr("utils.bet_windows._line_pulls", lambda date: pulls)
    monkeypatch.setattr("utils.bet_windows._payout_windows", lambda date: [])
    eight = summarize_fetch_window("2026-08-28", "8AM")
    assert eight["window"] == "8AM"
    assert eight["force_payout"] is False
    assert eight["rebuild_tickets"] is False
    # First stamp of the day still Forces even with empty payout log.
    first = summarize_fetch_window("2026-08-28", "1AM")
    assert first["force_payout"] is True
    assert first["rebuild_tickets"] is True


def test_write_fetch_window_stamp(tmp_path: Path, monkeypatch):
    import utils.bet_windows as bw

    monkeypatch.setattr(bw, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(bw, "LINE_WINDOW_CACHE", tmp_path / "last_line_window.json")
    monkeypatch.setattr(bw, "STAMP_TEMPLATES", tmp_path / "templates" / "last_fetch_window.json")
    monkeypatch.setattr(bw, "STAMP_RUNTIME", tmp_path / "runtime" / "last_fetch_window.json")
    monkeypatch.setattr(bw, "_line_pulls", lambda date: [])
    out = bw.write_fetch_window_stamp(bw.summarize_fetch_window("2026-08-28", "1PM"))
    assert out["window"] == "1PM"
    assert out["force_payout"] is True
    assert (tmp_path / "templates" / "last_fetch_window.json").is_file()
    assert (tmp_path / "last_line_window.json").is_file()


def test_append_scrape_log_and_rebuild(tmp_path: Path, monkeypatch):
    import utils.bet_windows as bw

    monkeypatch.setattr(bw, "REPORTS", tmp_path)
    monkeypatch.setattr(bw, "LINE_DB", tmp_path / "missing.db")
    captured = [
        {
            "status": "ok",
            "ticket_id": "t1",
            "n_legs": 3,
            "power_min_x": 2.0,
            "captured_at": "2026-08-27T16:32:00-04:00",
            "legs": [
                {"player": "A", "prop_type": "Pts", "pick_type": "Goblin", "line": 16.5},
            ],
        }
    ]
    n = append_payout_scrape_log("2026-08-27", captured)
    assert n == 1
    payload = rebuild_bet_windows("2026-08-27")
    assert payload["date"] == "2026-08-27"
    assert payload["payout_scrapes"]
    row = payload["payout_scrapes"][0]
    assert row["window"] == "4:30"
    assert row["median_power_x"] == 2.0
    assert "16:32" in str(row["last_captured_at"])

# ---- per-sport rebuild on newly posted props -------------------------------------------------


def _make_line_db(tmp_path, rows):
    import sqlite3

    db = tmp_path / "line_history.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE line_history (projection_id TEXT, player TEXT, prop_type TEXT, line REAL, "
            "pick_type TEXT, game_date TEXT, fetched_at TEXT, sport TEXT)"
        )
        conn.executemany(
            "INSERT INTO line_history VALUES (?,?,?,?,?,?,?,?)",
            rows,
        )
    return db


def test_new_rows_by_sport_counts_only_props_absent_from_earlier_fetches(monkeypatch, tmp_path):
    from utils import bet_windows as bw

    d = "2026-10-23"
    rows = []
    # 1AM: 10 tennis props (aces + games_won), 5 NBA props
    for i in range(10):
        rows.append((f"t{i}", f"P{i}", "games_won" if i % 2 else "aces", 9.5, "standard", d, "2026-10-23T01:05:00-04:00", "TENNIS"))
    for i in range(5):
        rows.append((f"n{i}", f"N{i}", "points", 20.5, "standard", d, "2026-10-23T01:05:30-04:00", "NBA"))
    # 8AM: same 10 tennis + 30 new tennis (one new prop type), NBA unchanged + 3 new
    for i in range(10):
        rows.append((f"t{i}", f"P{i}", "games_won" if i % 2 else "aces", 9.5, "standard", d, "2026-10-23T08:01:00-04:00", "TENNIS"))
    for i in range(10, 40):
        rows.append((f"t{i}", f"P{i}", "total_games", 22.5, "standard", d, "2026-10-23T08:02:00-04:00", "TENNIS"))
    for i in range(5):
        rows.append((f"n{i}", f"N{i}", "points", 20.5, "standard", d, "2026-10-23T08:03:00-04:00", "NBA"))
    for i in range(5, 8):
        rows.append((f"n{i}", f"N{i}", "points", 20.5, "standard", d, "2026-10-23T08:03:10-04:00", "NBA"))
    monkeypatch.setattr(bw, "LINE_DB", _make_line_db(tmp_path, rows))

    got = bw._new_rows_by_sport(d, "2026-10-23T08:00:00-04:00", "2026-10-23T08:59:00-04:00")
    assert got["TENNIS"]["new_rows"] == 30
    assert got["TENNIS"]["new_prop_types"] == ["total_games"]
    assert got["NBA"]["new_rows"] == 3 and got["NBA"]["new_prop_types"] == []
    # Before the first fetch of the day nothing counts as new.
    assert bw._new_rows_by_sport(d, "2026-10-23T01:00:00-04:00", "2026-10-23T01:59:00-04:00") == {}


def test_sports_needing_rebuild_threshold_and_new_prop_type(monkeypatch):
    from utils import bet_windows as bw

    monkeypatch.delenv("PROPORACLE_LATE_SPORT_NEW_ROWS_MIN", raising=False)
    got = bw.sports_needing_rebuild(
        {
            "TENNIS": {"new_rows": 24, "new_prop_types": []},
            "NBA": {"new_rows": 25, "new_prop_types": []},
            "SOCCER": {"new_rows": 2, "new_prop_types": ["saves"]},
            "MLB": {"new_rows": 0, "new_prop_types": []},
        }
    )
    assert got == ["NBA", "SOCCER"]


def test_summarize_sport_rebuild_only_when_enabled_and_no_full_rebuild(monkeypatch):
    from utils import bet_windows as bw

    pulls = [
        {"fetched_at": "2026-10-23T01:05:00-04:00", "window": "1AM", "n_props": 10, "n_moved": 0},
        {"fetched_at": "2026-10-23T08:02:00-04:00", "window": "8AM", "n_props": 40, "n_moved": 0},
    ]
    monkeypatch.setattr(bw, "_line_pulls", lambda date: pulls)
    monkeypatch.setattr(bw, "_payout_windows", lambda date: [{"window": "1AM", "n_scrapes": 1}])
    monkeypatch.setattr(
        bw,
        "_new_rows_by_sport",
        lambda date, a, b: {
            "TENNIS": {"new_rows": 30, "new_prop_types": ["total_games"]},
            "SOCCER": {"new_rows": 90, "new_prop_types": []},
        },
    )
    monkeypatch.delenv("PROPORACLE_LATE_SPORT_REBUILD", raising=False)
    s = bw.summarize_fetch_window("2026-10-23", "8AM")
    assert s["rebuild_tickets"] is False
    assert s["sport_rebuild_triggered"] == ["SOCCER", "TENNIS"]
    assert s["rebuild_sports"] == ["TENNIS"]  # default flag = tennis only

    monkeypatch.setenv("PROPORACLE_LATE_SPORT_REBUILD", "off")
    assert bw.summarize_fetch_window("2026-10-23", "8AM")["rebuild_sports"] == []
    monkeypatch.setenv("PROPORACLE_LATE_SPORT_REBUILD", "tennis,soccer")
    assert bw.summarize_fetch_window("2026-10-23", "8AM")["rebuild_sports"] == ["SOCCER", "TENNIS"]

    # The day's first stamp already runs the full rebuild; no per-sport pass on top of it.
    first = bw.summarize_fetch_window("2026-10-23", "1AM")
    assert first["rebuild_tickets"] is True and first["rebuild_sports"] == []


def test_append_sport_rebuild_log_writes_one_line(tmp_path, monkeypatch):
    import json as _json

    from utils import bet_windows as bw

    monkeypatch.delenv("PROPORACLE_LATE_SPORT_REBUILD", raising=False)
    path = bw.append_sport_rebuild_log(
        date="2026-10-23",
        window="1PM",
        mode="sport",
        triggered=["TENNIS"],
        rebuilt=["TENNIS"],
        new_by_sport={"TENNIS": {"new_rows": 30, "new_prop_types": []}},
        outcome="ok",
        path=tmp_path / "log.jsonl",
    )
    rec = _json.loads(path.read_text(encoding="utf-8").strip())
    assert rec["mode"] == "sport" and rec["rebuilt_sports"] == ["TENNIS"] and rec["window"] == "1PM"
    assert rec["enabled_sports"] == ["TENNIS"] and rec["new_rows_min"] == 25
