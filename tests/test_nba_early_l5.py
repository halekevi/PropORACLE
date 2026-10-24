"""Card-window L5: RS+playoffs gate; preseason flags only."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pandas as pd

from utils.nba_early_l5 import (
    PRE_RETIRED,
    card_l5_ok,
    current_season_rs_n,
    early_season_l5_ok,
    exclude_preseason_from_l5,
    format_side_by_side,
    is_early_season,
    l5_card_window,
    l5_preseason,
    l5_rs_only,
    preseason_bridge_active,
    preseason_flag,
    window_mix,
)


def _boxes() -> pd.DataFrame:
    rows = [
        # Demin: preseason soft overs vs RS
        ("2026-10-14", "preseason", "Egor Demin", 23, 3, 17),
        ("2026-10-12", "preseason", "Egor Demin", 22, 0, 4),
        ("2026-10-08", "preseason", "Egor Demin", 22, 1, 13),
        ("2026-10-06", "preseason", "Egor Demin", 18, 1, 6),
        ("2026-02-27", "regular", "Egor Demin", 22, 0, 5),
        ("2026-02-26", "regular", "Egor Demin", 25, 5, 6),
        ("2026-02-24", "regular", "Egor Demin", 28, 5, 3),
        ("2026-02-22", "regular", "Egor Demin", 28, 1, 13),
        ("2026-02-20", "regular", "Egor Demin", 28, 5, 14),
        # Garland: pre + RS
        ("2026-10-14", "preseason", "Darius Garland", 23, 2, 14),
        ("2026-10-10", "preseason", "Darius Garland", 19, 1, 11),
        ("2026-10-04", "preseason", "Darius Garland", 15, 2, 15),
        ("2026-04-12", "regular", "Darius Garland", 27, 3, 15),
        ("2026-04-10", "regular", "Darius Garland", 41, 4, 16),
        ("2026-04-07", "regular", "Darius Garland", 28, 2, 22),
        ("2026-04-05", "regular", "Darius Garland", 24, 1, 17),
        ("2026-04-02", "regular", "Darius Garland", 29, 2, 11),
        # Allen: playoff boxes on the PP card (RS-only would miss these)
        ("2026-06-01", "playoffs", "Jarrett Allen", 35, 12, 18),
        ("2026-05-28", "playoffs", "Jarrett Allen", 34, 11, 16),
        ("2026-05-25", "playoffs", "Jarrett Allen", 36, 10, 14),
        ("2026-05-22", "playoffs", "Jarrett Allen", 33, 13, 15),
        ("2026-05-20", "playoffs", "Jarrett Allen", 32, 11, 17),
        ("2026-04-10", "regular", "Jarrett Allen", 30, 9, 12),
        ("2026-10-12", "preseason", "Jarrett Allen", 20, 4, 8),
        ("2026-10-08", "preseason", "Jarrett Allen", 18, 5, 6),
    ]
    df = pd.DataFrame(
        rows, columns=["game_date", "window", "player", "minutes", "reb", "pts"]
    )
    df["game_date"] = pd.to_datetime(df["game_date"])
    for c in ("ast", "stl", "blk", "tov", "fg3m", "pra", "pr", "pa", "ra"):
        df[c] = 0.0
    df["_pn"] = df["player"].str.lower()
    return df


OPENER = date(2026, 10, 20)


@patch("utils.nba_early_l5.first_regular_season_date", return_value=OPENER)
def test_early_season_helper_still_uses_opener(_mock):
    assert is_early_season(date(2026, 10, 19))
    assert is_early_season(date(2026, 10, 20))
    assert is_early_season(date(2026, 11, 2))
    assert not is_early_season(date(2026, 11, 3))


def test_exclude_preseason_always():
    assert exclude_preseason_from_l5("2026-10-21")
    assert exclude_preseason_from_l5(date(2026, 11, 20))
    assert exclude_preseason_from_l5(None)


def test_card_window_includes_playoffs_not_preseason():
    boxes = _boxes()
    card = l5_card_window(boxes, "Jarrett Allen", "Rebounds")
    assert card is not None
    assert window_mix(card) == "OOOOO"
    assert [r["val"] for r in card] == [12, 11, 10, 13, 11]
    pre = l5_preseason(boxes, "Jarrett Allen", "Rebounds")
    assert [r["val"] for r in pre] == [4, 5]
    # RS-only legacy would skip the card's playoff window
    rs = l5_rs_only(boxes, "Jarrett Allen", "Rebounds")
    assert rs is None or [r["val"] for r in rs] != [12, 11, 10, 13, 11]


def test_allen_reb_over_card_clears_pre_conflicts():
    boxes = _boxes()
    out = card_l5_ok(boxes, "Jarrett Allen", "Rebounds", 9.5, "OVER", slate="2026-10-22")
    assert out is not None
    assert out["mode"] == "card_rs_playoffs"
    assert out["window_mix"] == "OOOOO"
    assert out["pre_flag"] == "pre_conflicts"
    assert "card[OOOOO]" in format_side_by_side(out)


def test_garland_points_under_card_with_pre_confirms():
    boxes = _boxes()
    out = early_season_l5_ok(
        boxes, "Darius Garland", "Points", 23.5, "UNDER", slate="2026-10-21"
    )
    assert out is not None
    assert out["ticket_vals"] == [15, 16, 22, 17, 11]
    assert out["window_mix"] == "RRRRR"
    assert out["pre_flag"] == "pre_confirms"
    assert preseason_flag(boxes, "Darius Garland", "Points", 23.5, "UNDER") == "pre_confirms"


def test_demin_reb_under_fails_card_rs():
    boxes = _boxes()
    # Card window is RS [0,5,5,1,5] vs 3.5 UNDER — does not clear 5/5
    assert (
        card_l5_ok(boxes, "Egor Demin", "Rebounds", 3.5, "UNDER", slate="2026-10-21")
        is None
    )
    # Preseason [3,0,1,1] all clear 3.5 UNDER — confirms, but card RS fails the gate
    assert preseason_flag(boxes, "Egor Demin", "Rebounds", 3.5, "UNDER") == "pre_confirms"


def test_pre_thin_when_fewer_than_two():
    boxes = _boxes()
    # Strip Allen preseason to one game
    boxes = boxes[~((boxes["_pn"] == "jarrett allen") & (boxes["window"] == "preseason"))]
    boxes = pd.concat(
        [
            boxes,
            pd.DataFrame(
                [
                    {
                        "game_date": pd.Timestamp("2026-10-12"),
                        "window": "preseason",
                        "player": "Jarrett Allen",
                        "minutes": 20,
                        "reb": 4,
                        "pts": 8,
                        "ast": 0,
                        "stl": 0,
                        "blk": 0,
                        "tov": 0,
                        "fg3m": 0,
                        "pra": 0,
                        "pr": 0,
                        "pa": 0,
                        "ra": 0,
                        "_pn": "jarrett allen",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    assert preseason_flag(boxes, "Jarrett Allen", "Rebounds", 9.5, "OVER") == "pre_thin"


@patch("utils.nba_early_l5.first_regular_season_date", return_value=OPENER)
def test_pre_retired_after_five_current_season_rs(_mock):
    """Once a player has 5 current-season RS games, hide the pre_ bridge."""
    boxes = _boxes()
    # Five RS games on/after opener for a new player with conflicting preseason.
    rows = []
    for i, (gd, reb) in enumerate(
        [
            ("2026-10-22", 10),
            ("2026-10-24", 11),
            ("2026-10-26", 9),
            ("2026-10-28", 12),
            ("2026-10-30", 10),
        ]
    ):
        rows.append(
            {
                "game_date": pd.Timestamp(gd),
                "window": "regular",
                "player": "Bridge Tester",
                "minutes": 28,
                "reb": reb,
                "pts": 15,
                "ast": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "fg3m": 0,
                "pra": 0,
                "pr": 0,
                "pa": 0,
                "ra": 0,
                "_pn": "bridge tester",
            }
        )
    # Preseason would conflict vs 8.5 OVER (all unders) — must not surface once retired.
    for gd, reb in [("2026-10-10", 2), ("2026-10-12", 3), ("2026-10-14", 1)]:
        rows.append(
            {
                "game_date": pd.Timestamp(gd),
                "window": "preseason",
                "player": "Bridge Tester",
                "minutes": 20,
                "reb": reb,
                "pts": 8,
                "ast": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "fg3m": 0,
                "pra": 0,
                "pr": 0,
                "pa": 0,
                "ra": 0,
                "_pn": "bridge tester",
            }
        )
    boxes = pd.concat([boxes, pd.DataFrame(rows)], ignore_index=True)
    slate = "2026-10-31"
    assert current_season_rs_n(boxes, "Bridge Tester", as_of=slate) == 5
    assert not preseason_bridge_active(boxes, "Bridge Tester", as_of=slate)
    assert (
        preseason_flag(boxes, "Bridge Tester", "Rebounds", 8.5, "OVER", as_of=slate)
        == PRE_RETIRED
    )
    out = card_l5_ok(
        boxes, "Bridge Tester", "Rebounds", 8.5, "OVER", slate=slate
    )
    assert out is not None
    assert out["pre_flag"] == PRE_RETIRED
    assert out["pre_bridge_active"] is False
    assert out["preseason"] == []
    assert "pre[" not in format_side_by_side(out)


@patch("utils.nba_early_l5.first_regular_season_date", return_value=OPENER)
def test_pre_bridge_still_on_with_few_rs(_mock):
    boxes = _boxes()
    # Allen has 0 RS on/after Oct 20 opener in the fixture → bridge on.
    assert current_season_rs_n(boxes, "Jarrett Allen", as_of="2026-10-23") == 0
    assert preseason_bridge_active(boxes, "Jarrett Allen", as_of="2026-10-23")
