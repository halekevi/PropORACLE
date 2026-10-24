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
    from utils.bet_windows import summarize_fetch_window

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
    from utils.bet_windows import summarize_fetch_window

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


def test_scrape_log_records_time_and_shared_game(tmp_path: Path, monkeypatch):
    import json

    import utils.bet_windows as bw

    monkeypatch.setattr(bw, "REPORTS", tmp_path)
    captured = [
        {
            "status": "ok",
            "ticket_id": "t-share",
            "n_legs": 2,
            "power_min_x": 1.5,
            "captured_at": "2026-08-27T16:32:00-04:00",
            "legs": [
                {
                    "player": "A",
                    "sport": "MLB",
                    "team": "MIL",
                    "opp": "SD",
                    "game_id": "g1",
                    "prop_type": "Pitcher Strikeouts",
                    "line": 1.5,
                    "pick_type": "Goblin",
                },
                {
                    "player": "B",
                    "sport": "MLB",
                    "team": "SD",
                    "opp": "MIL",
                    "game_id": "g1",
                    "prop_type": "Hits",
                    "line": 0.5,
                    "pick_type": "Goblin",
                },
            ],
        }
    ]
    assert bw.append_payout_scrape_log("2026-08-27", captured) == 1
    line = (tmp_path / "payout_scrape_log_2026-08-27.jsonl").read_text(encoding="utf-8")
    row = json.loads(line)
    assert row["captured_time_et"] == "16:32"
    assert row["shared_game"] is True
    assert row["shared_team"] is False


def test_scrape_log_keeps_sibling_gap_and_drops_offset_estimate(tmp_path: Path, monkeypatch):
    import json

    import utils.bet_windows as bw

    monkeypatch.setattr(bw, "REPORTS", tmp_path)
    captured = [
        {
            "status": "ok",
            "ticket_id": "t-gap",
            "n_legs": 2,
            "power_min_x": 1.8,
            "captured_at": "2026-10-08T16:00:00-04:00",
            "legs": [
                {
                    "player": "Novak Djokovic",
                    "sport": "TENNIS",
                    "prop_type": "Total Games",
                    "line": 19.5,
                    "pick_type": "Goblin",
                    "standard_line": 24.5,
                    "standard_line_source": "sibling",
                    "gap": 5.0,
                },
                {
                    "player": "Orphan",
                    "sport": "TENNIS",
                    "prop_type": "Aces",
                    "line": 4.5,
                    "pick_type": "Goblin",
                    "standard_line": 6.0,
                    "standard_line_source": "offset_estimate",
                },
                {
                    "player": "Unlabeled",
                    "sport": "TENNIS",
                    "prop_type": "Total Games",
                    "line": 20.5,
                    "pick_type": "Goblin",
                    "standard_line": 22.5,
                },
            ],
        }
    ]
    assert bw.append_payout_scrape_log("2026-10-08", captured) == 1
    row = json.loads((tmp_path / "payout_scrape_log_2026-10-08.jsonl").read_text(encoding="utf-8"))
    by_player = {leg["player"]: leg for leg in row["lines"]}
    assert by_player["Novak Djokovic"]["gap"] == 5.0
    assert by_player["Novak Djokovic"]["standard_line"] == 24.5
    assert by_player["Orphan"]["gap"] is None
    assert by_player["Orphan"]["standard_line"] is None
    assert by_player["Unlabeled"]["gap"] is None
    assert by_player["Unlabeled"]["standard_line"] is None
