from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from utils.slate_schedule_gate import (
    filter_payload_to_espn_schedule,
    leg_schedule_reason,
    load_team_games,
    parse_loose_game_time,
)
from utils.stale_board_tickets import ticket_is_stale_board


def _nba_event(home: str, away: str, start: str) -> dict:
    return {
        "date": start,
        "competitions": [
            {
                "date": start,
                "competitors": [
                    {"homeAway": "home", "team": {"abbreviation": home}},
                    {"homeAway": "away", "team": {"abbreviation": away}},
                ],
            }
        ],
    }


def test_okc_sas_blocked_when_espn_has_okc_den():
    def fetch(path: str, day: str):
        assert "nba" in path
        if day == "2026-10-08":
            return [_nba_event("DEN", "OKC", "2026-10-08T23:00:00Z")]
        return []

    games, err = load_team_games("NBA", "2026-10-08", fetch=fetch)
    assert not err
    assert games
    leg = {
        "sport": "NBA1Q",
        "player": "Jalen Williams",
        "team": "OKC",
        "opp": "SAS",
    }
    reason = leg_schedule_reason(
        leg,
        slate="2026-10-08",
        team_games={"NBA": (games, ""), "NBA1Q": (games, "")},
    )
    assert reason and "matchup_not_on_scoreboard" in reason


def test_real_matchup_passes():
    def fetch(path: str, day: str):
        return [_nba_event("NYK", "PHI", "2026-10-20T23:30:00Z")]

    games, _ = load_team_games("NBA", "2026-10-20", fetch=fetch)
    leg = {"sport": "NBA1Q", "player": "Brunson", "team": "NYK", "opp": "PHI"}
    assert (
        leg_schedule_reason(
            leg,
            slate="2026-10-20",
            team_games={"NBA": (games, ""), "NBA1Q": (games, "")},
        )
        is None
    )


def test_empty_scoreboard_fail_closed():
    def fetch(path: str, day: str):
        return []

    games, err = load_team_games("NBA", "2026-10-08", fetch=fetch)
    assert games == set()
    leg = {"sport": "NBA", "player": "X", "team": "OKC", "opp": "SAS"}
    reason = leg_schedule_reason(leg, slate="2026-10-08", team_games={"NBA": (games, err)})
    assert reason == "empty_scoreboard"


def test_filter_payload_drops_off_board_slip():
    def fetch(path: str, day: str):
        return [_nba_event("BOS", "DET", "2026-10-20T23:00:00Z")]

    payload = {
        "date": "2026-10-20",
        "groups": [
            {
                "group_name": "NBA1Q Power 3",
                "tickets": [
                    {
                        "ticket_id": "phantom",
                        "legs": [
                            {
                                "sport": "NBA1Q",
                                "player": "Jalen Williams",
                                "team": "OKC",
                                "opp": "SAS",
                            }
                        ],
                    },
                    {
                        "ticket_id": "live",
                        "legs": [
                            {
                                "sport": "NBA1Q",
                                "player": "Cade Cunningham",
                                "team": "DET",
                                "opp": "BOS",
                            }
                        ],
                    },
                ],
            }
        ],
    }
    out, blocked = filter_payload_to_espn_schedule(
        payload,
        fetch=fetch,
        now=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
    )
    ids = [t["ticket_id"] for t in out["groups"][0]["tickets"]]
    assert ids == ["live"]
    assert blocked[0]["ticket_id"] == "phantom"
    assert blocked[0]["reason"] in {
        "team_not_on_scoreboard:OKC",
        "matchup_not_on_scoreboard:OKC@SAS",
    } or "scoreboard" in blocked[0]["reason"]


def test_time_only_stamp_is_not_a_game_date():
    s = pd.Series(["8:00 PM", "2026-05-30", None])
    ok = s.astype(str).str.strip().str[:10].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    assert list(ok) == [False, True, False]


def test_fetch_error_fail_closed():
    def fetch(path: str, day: str):
        raise TimeoutError("espn down")

    games, err = load_team_games("NBA", "2026-10-23", fetch=fetch)
    assert games == set()
    assert err.startswith("fetch_failed")
    leg = {"sport": "NBA", "team": "NYK", "opp": "PHI"}
    reason = leg_schedule_reason(leg, slate="2026-10-23", team_games={"NBA": (games, err)})
    assert reason.startswith("fetch_failed")


def test_sa_and_sas_are_the_same_game():
    def fetch(path: str, day: str):
        return [_nba_event("SA", "OKC", "2026-10-23T23:00:00Z")]

    games, _ = load_team_games("NBA", "2026-10-23", fetch=fetch)
    leg = {"sport": "NBA", "player": "Wemby", "team": "SAS", "opp": "OKC"}
    assert (
        leg_schedule_reason(leg, slate="2026-10-23", team_games={"NBA": (games, "")})
        is None
    )


def test_combo_det_bos_passes():
    def fetch(path: str, day: str):
        return [_nba_event("BOS", "DET", "2026-10-20T23:00:00Z")]

    games, _ = load_team_games("NBA", "2026-10-20", fetch=fetch)
    leg = {"sport": "NBA", "player": "Cade / Tatum", "team": "DET/BOS"}
    assert (
        leg_schedule_reason(leg, slate="2026-10-20", team_games={"NBA": (games, "")})
        is None
    )


def test_tennis_doubles_on_scoreboard():
    board = {"marcelo arevalo", "mate pavic"}
    leg = {"sport": "TENNIS", "player": "Arevalo / Pavic"}
    assert (
        leg_schedule_reason(
            leg,
            slate="2026-10-23",
            name_boards={"TENNIS": (board, "")},
        )
        is None
    )


def test_started_game_is_blocked_at_publish():
    def fetch(path: str, day: str):
        return [_nba_event("NYK", "PHI", "2026-10-23T23:30:00Z")]

    payload = {
        "date": "2026-10-23",
        "groups": [
            {
                "group_name": "NBA Goblin-70 Power 3",
                "tickets": [
                    {
                        "ticket_id": "after-tip",
                        "legs": [
                            {
                                "sport": "NBA",
                                "player": "Jalen Brunson",
                                "team": "NYK",
                                "opp": "PHI",
                            }
                        ],
                    }
                ],
            }
        ],
    }
    before, blocked_before = filter_payload_to_espn_schedule(
        {
            "date": "2026-10-23",
            "groups": [
                {
                    "group_name": "NBA Goblin-70 Power 3",
                    "tickets": [dict(payload["groups"][0]["tickets"][0])],
                }
            ],
        },
        fetch=fetch,
        now=datetime(2026, 10, 23, 20, 0, tzinfo=timezone.utc),
    )
    assert [t["ticket_id"] for t in before["groups"][0]["tickets"]] == ["after-tip"]
    assert blocked_before == []

    after, blocked_after = filter_payload_to_espn_schedule(
        payload, fetch=fetch, now=datetime(2026, 10, 24, 1, 0, tzinfo=timezone.utc)
    )
    assert after["groups"][0]["tickets"] == []
    assert blocked_after[0]["reason"].startswith("game_started:")


def test_parse_loose_game_time_clock_only():
    dt = parse_loose_game_time("7:00 PM", "2026-10-23")
    assert dt is not None
    assert dt.hour == 19
    assert dt.tzinfo is not None


def test_nba1q_hold_still_on():
    from utils.ticket_sport_holds import nba1q_tickets_held

    assert nba1q_tickets_held(env={}) is True
    assert ticket_is_stale_board(
        {
            "legs": [
                {
                    "sport": "NBA1Q",
                    "team": "OKC",
                    "opp": "SAS",
                    "player": "Jalen Williams",
                }
            ]
        },
        slate_date="2026-10-08",
    )
