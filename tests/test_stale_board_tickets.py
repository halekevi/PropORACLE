from __future__ import annotations

from utils.stale_board_tickets import (
    NBA1Q_REAL_BOARD_FROM,
    annotate_ticket_payload,
    board_row_is_stale_board,
    cross_date_fallback_allowed,
    ticket_is_stale_board,
    ticket_leg_row_is_stale_board,
    ticket_stale_board_reason,
)
from utils.ticket_70_pool import ticket_excluded_from_winrate


def _okc_sas_ticket(*, date: str, game_date: str = "2026-05-30") -> dict:
    return {
        "ticket_id": "N1Q-phantom",
        "date": date,
        "legs": [
            {
                "sport": "NBA1Q",
                "player": "Jalen Williams",
                "team": "OKC",
                "opp": "SAS",
                "prop_type": "Points",
                "line": 5.5,
                "direction": "OVER",
                "pick_type": "Goblin",
                "game_date": game_date,
            }
        ],
    }


def test_cross_date_fallback_caps_at_three_days():
    assert cross_date_fallback_allowed("2026-10-24", "2026-10-23")
    assert cross_date_fallback_allowed("2026-10-26", "2026-10-23")
    assert not cross_date_fallback_allowed("2026-05-30", "2026-10-08")
    assert not cross_date_fallback_allowed("2026-03-15", "2026-10-23")


def test_early_oct_nba1q_is_stale_board_not_void():
    t = _okc_sas_ticket(date="2026-10-08")
    assert ticket_stale_board_reason(t, slate_date="2026-10-08") == "stale_board"
    assert ticket_excluded_from_winrate(t, slate_date="2026-10-08")
    payload = {"date": "2026-10-08", "groups": [{"group_name": "NBA1Q Power 3", "tickets": [t]}]}
    assert annotate_ticket_payload(payload) == 1
    stamped = payload["groups"][0]["tickets"][0]
    assert stamped["invalid"] is True
    assert stamped["invalid_reason"] == "stale_board"
    assert stamped["exclude_from_winrate"] is True
    assert stamped["exclude_from_book"] is True
    assert stamped["exclude_from_forward"] is True
    assert stamped.get("result") != "VOID"


def test_oct20_real_board_is_not_stale():
    t = {
        "legs": [
            {
                "sport": "NBA1Q",
                "player": "Jalen Brunson",
                "team": "NYK",
                "opp": "PHI",
                "game_date": "2026-10-20",
                "prop_type": "Points",
                "line": 6.5,
                "direction": "OVER",
            }
        ]
    }
    assert NBA1Q_REAL_BOARD_FROM == "2026-10-20"
    assert not ticket_is_stale_board(t, slate_date="2026-10-20")
    assert not ticket_excluded_from_winrate(t, slate_date="2026-10-20")


def test_oct21_okc_sas_is_stale():
    t = _okc_sas_ticket(date="2026-10-21", game_date="2026-10-21")
    assert ticket_is_stale_board(t, slate_date="2026-10-21")


def test_ledger_row_filter():
    assert ticket_leg_row_is_stale_board({"sport": "NBA1Q", "slate_date": "2026-10-08"})
    assert not ticket_leg_row_is_stale_board({"sport": "NBA1Q", "slate_date": "2026-10-20"})
    assert not ticket_leg_row_is_stale_board({"sport": "NBA", "slate_date": "2026-10-08"})
    assert ticket_leg_row_is_stale_board(
        {"sport": "NBA1Q", "slate_date": "2026-10-21", "team": "OKC", "opp": "SAS"}
    )


def test_stamp_grade_dataframe_oct21():
    import pandas as pd

    from utils.stale_board_tickets import stamp_grade_dataframe

    df = pd.DataFrame(
        [
            {
                "player": "Jalen Williams",
                "team": "OKC",
                "opp_team": "SAS",
                "sport": "NBA1Q",
                "result": "",
            }
        ]
    )
    n = stamp_grade_dataframe(df, slate_date="2026-10-21", sport="NBA1Q")
    assert n == 1
    assert df.loc[0, "invalid_reason"] == "stale_board"
    assert bool(df.loc[0, "exclude_from_forward"]) is True
    assert str(df.loc[0, "result"]).upper() != "VOID"


def test_oct21_single_side_grade_row_is_stale():
    row = {
        "sport": "NBA1Q",
        "player": "Jalen Williams",
        "team": "OKC",
        "opp_team": None,
        "ticket_id": "2026-05-18|X-Sport Goblin 6-Leg #83|2",
    }
    assert board_row_is_stale_board(row, slate_date="2026-10-21")
    assert board_row_is_stale_board(
        {"sport": "NBA1Q", "team": "SAS", "opp_team": None},
        slate_date="2026-10-21",
    )


def test_board_row_may_finals():
    row = {"sport": "NBA1Q", "game_date": "2026-05-30", "team": "OKC", "opp": "SAS"}
    assert board_row_is_stale_board(row, slate_date="2026-10-08")
