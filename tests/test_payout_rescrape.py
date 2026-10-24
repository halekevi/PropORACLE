"""Incremental payout re-scrape: changed lines/types + missing live_cdp."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from utils.payout_rescrape import (
    missing_live_ticket_ids,
    plan_rescrape,
    ticket_has_live_cdp,
    ticket_ids_for_props,
)


def _payload() -> dict:
    return {
        "date": "2026-08-28",
        "groups": [
            {
                "group_name": "G",
                "tickets": [
                    {
                        "ticket_id": "t-changed",
                        "payout_source": "live_cdp",
                        "display_min_x": 3.5,
                        "payout": {
                            "payout_source": "live_cdp",
                            "power_min_x": 3.5,
                            "captured_at": "2026-08-28T08:53:31",
                        },
                        "legs": [
                            {
                                "player": "Jane Doe",
                                "prop_type": "Points",
                                "pick_type": "Goblin",
                                "line": 16.5,
                            },
                            {
                                "player": "Other",
                                "prop_type": "Assists",
                                "pick_type": "Goblin",
                                "line": 4.5,
                            },
                        ],
                    },
                    {
                        "ticket_id": "t-stable",
                        "payout": {
                            "payout_source": "live_cdp",
                            "power_min_x": 2.3,
                        },
                        "legs": [
                            {
                                "player": "Stable Player",
                                "prop_type": "Rebounds",
                                "pick_type": "Goblin",
                                "line": 7.5,
                            },
                            {
                                "player": "Other",
                                "prop_type": "Assists",
                                "pick_type": "Goblin",
                                "line": 4.5,
                            },
                        ],
                    },
                    {
                        "ticket_id": "t-new",
                        "payout": {"payout_source": "pending_live"},
                        "legs": [
                            {
                                "player": "New Player",
                                "prop_type": "Hits",
                                "pick_type": "Goblin",
                                "line": 0.5,
                            },
                            {
                                "player": "Bat Two",
                                "prop_type": "Total Bases",
                                "pick_type": "Goblin",
                                "line": 1.5,
                            },
                        ],
                    },
                ],
            }
        ],
    }


def test_live_cdp_helper():
    assert ticket_has_live_cdp(
        {"payout": {"payout_source": "live_cdp", "power_min_x": 3.5}}
    )
    assert not ticket_has_live_cdp({"payout": {"payout_source": "pending_live"}})
    assert not ticket_has_live_cdp(
        {"payout": {"payout_source": "live_cdp", "power_min_x": 0}}
    )


def test_ticket_ids_for_moved_and_cut_props():
    payload = _payload()
    moved = [{"player": "Jane Doe", "prop_type": "Points", "event": "moved"}]
    assert ticket_ids_for_props(payload, moved) == ["t-changed"]
    cut = [{"player": "jane  doe", "prop_type": "points", "event": "cut"}]
    assert ticket_ids_for_props(payload, cut) == ["t-changed"]
    assert ticket_ids_for_props(payload, []) == []


def test_missing_live_ids():
    ids = missing_live_ticket_ids(_payload())
    assert ids == ["t-new"]


def test_plan_unions_changed_and_missing(tmp_path: Path):
    db = tmp_path / "line_history.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE line_events (
          sport TEXT, game_date TEXT, player TEXT, prop_type TEXT,
          projection_id TEXT, event TEXT, prev_pick TEXT, new_pick TEXT,
          prev_line REAL, new_line REAL, prev_fetched_at TEXT, fetched_at TEXT,
          start_time TEXT, team TEXT, opp_team TEXT, still_on_slate INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO line_events (
          sport, game_date, player, prop_type, event, prev_pick, new_pick,
          prev_line, new_line, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "wnba",
            "2026-08-28",
            "Jane Doe",
            "Points",
            "moved",
            "Goblin",
            "Goblin",
            16.5,
            17.5,
            "2026-08-28T10:30:05-04:00",
        ),
    )
    conn.commit()
    conn.close()
    tickets = tmp_path / "tickets.json"
    tickets.write_text(json.dumps(_payload()), encoding="utf-8")
    plan = plan_rescrape(
        tickets,
        "2026-08-28",
        since="2026-08-28T10:29:00-04:00",
        db_path=db,
    )
    assert plan["n_changed_props"] == 1
    assert set(plan["ticket_ids"]) == {"t-changed", "t-new"}
    assert plan["skip_scrape"] is False
    stale = plan_rescrape(
        tickets,
        "2026-08-28",
        since="2026-08-28T11:00:00-04:00",
        db_path=db,
    )
    assert stale["ticket_ids"] == ["t-new"]
    assert stale["n_changed_tickets"] == 0
