"""Tennis opponent rank: no fake 75, fill from the other player on the slate."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "Sports" / "Tennis" / "scripts"))

from tennis_shared import (  # noqa: E402
    atp_wta_def_tier,
    ensure_opponent_atp_wta_rank,
    fill_opponent_rank_from_slate_players,
    resolve_opp_rank,
)


def test_unknown_opp_rank_is_none():
    assert resolve_opp_rank("UNKNOWN_OPP", []) is None
    assert resolve_opp_rank("UNK", [{"player_key": "foo", "rank": 12}]) is None
    assert resolve_opp_rank("", []) is None


def test_named_opp_does_not_substring_ann_li():
    ranks = [
        {"player_key": "ann li", "rank": 40, "espn_athlete_id": "3380"},
        {"player_key": "rajeev ram", "rank": 80, "espn_athlete_id": "111"},
    ]
    for name in ("Jordyn Hazelitt", "Elizabeth Mandlik", "Alice Tubello", "Julia Avdeeva"):
        assert resolve_opp_rank(name, ranks) is None, name
    assert resolve_opp_rank("Ram R", ranks) == 80.0


def test_fill_from_slate_players_tiafoe_tien():
    df = pd.DataFrame(
        [
            {
                "player": "Frances Tiafoe",
                "opp_team": "LEARNER TIEN",
                "player_atp_rank": 23,
                "opponent_rank": 75,
            },
            {
                "player": "Learner Tien",
                "opp_team": "FRANCES TIAFOE",
                "player_atp_rank": 12,
                "opponent_rank": 75,
            },
            {
                "player": "Unknown Player",
                "opp_team": "UNKNOWN_OPP",
                "player_atp_rank": 40,
                "opponent_rank": 75,
            },
        ]
    )
    out = fill_opponent_rank_from_slate_players(df)
    assert int(out.loc[0, "opponent_rank"]) == 12
    assert int(out.loc[1, "opponent_rank"]) == 23
    assert pd.isna(out.loc[2, "opponent_rank"]) or out.loc[2, "opponent_rank"] is None


def test_ensure_rank_backfills_blank_opp_from_game():
    df = pd.DataFrame(
        [
            {
                "player": "Frances Tiafoe",
                "opp_team": "",
                "pp_game_id": "g1",
                "player_atp_rank": 23,
                "opponent_rank": None,
            },
            {
                "player": "Learner Tien",
                "opp_team": "",
                "pp_game_id": "g1",
                "player_atp_rank": 12,
                "opponent_rank": None,
            },
        ]
    )
    out = ensure_opponent_atp_wta_rank(df)
    assert str(out.loc[0, "opp_team"]).upper() == "LEARNER TIEN"
    assert str(out.loc[1, "opp_team"]).upper() == "FRANCES TIAFOE"
    assert int(out.loc[0, "opponent_rank"]) == 12
    assert int(out.loc[1, "opponent_rank"]) == 23


def test_atp_wta_def_tier_bands():
    assert atp_wta_def_tier(None) == ""
    assert atp_wta_def_tier(8) == "Elite"
    assert atp_wta_def_tier(20) == "Above Avg"
    assert atp_wta_def_tier(40) == "Avg"
    assert atp_wta_def_tier(80) == "Below Avg"
    assert atp_wta_def_tier(250) == "Weak"
