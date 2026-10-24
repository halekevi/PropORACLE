"""Within-ticket correlation: hard blocks vs same_game_pair flag."""
from __future__ import annotations

from utils.slip_correlation import (
    SAME_GAME_PAIR_GATE_N,
    all_same_team,
    annotate_slip,
    hard_block_reason,
    has_duplicate_player,
    has_same_game_pair,
    pick_legs_without_hard_blocks,
    same_game_pair_may_gate,
    summarize_same_game_pair_grades,
)


def _leg(player: str, team: str, game: str, **kw) -> dict:
    row = {
        "player": player,
        "team": team,
        "game_id": game,
        "opp_team": "OPP",
    }
    row.update(kw)
    return row


def test_hard_blocks_same_player_and_all_same_team():
    assert has_duplicate_player(
        [_leg("Garland", "LAC", "g1"), _leg("Garland", "LAC", "g1", prop="Assists")]
    )
    assert hard_block_reason(
        [_leg("Garland", "LAC", "g1"), _leg("Garland", "LAC", "g1", prop="Assists")]
    ) == "same_player_twice"

    same_team = [
        _leg("Garland", "LAC", "g1"),
        _leg("Jackson", "LAC", "g1"),
        _leg("Harden", "LAC", "g1"),
    ]
    assert all_same_team(same_team)
    assert hard_block_reason(same_team) == "all_same_team"


def test_same_game_pair_is_flag_not_block():
    legs = [
        _leg("Garland", "LAC", "lac-orl"),
        _leg("Wagner", "ORL", "lac-orl"),
        _leg("Murray", "NOP", "nop-xxx"),
    ]
    assert has_same_game_pair(legs)
    assert hard_block_reason(legs) is None
    slip = annotate_slip({"legs": legs})
    assert slip["same_game_pair"] is True
    assert slip["correlation_ok"] is True


def test_garland_wagner_jackson_slip_passes():
    """ORL + two LAC is fine: not all one team; same-game pair flagged."""
    legs = [
        _leg("Garland", "LAC", "lac-orl"),
        _leg("Wagner", "ORL", "lac-orl"),
        _leg("Jackson", "LAC", "lac-orl"),
    ]
    assert hard_block_reason(legs) is None
    assert has_same_game_pair(legs)
    # All LAC would fail
    assert (
        hard_block_reason(
            [
                _leg("Garland", "LAC", "lac-orl"),
                _leg("Jackson", "LAC", "lac-orl"),
                _leg("Harden", "LAC", "lac-orl"),
            ]
        )
        == "all_same_team"
    )


def test_pick_legs_skips_all_same_team_finale():
    pool = [
        _leg("A", "LAC", "g1"),
        _leg("B", "LAC", "g1"),
        _leg("C", "ORL", "g1"),
    ]
    picked = pick_legs_without_hard_blocks(pool, 3)
    assert picked is not None
    assert {x["player"] for x in picked} == {"A", "B", "C"}
    only_lac = pick_legs_without_hard_blocks(pool[:2], 2)
    assert only_lac is None


def test_same_game_pair_gate_waits_for_100_each_side():
    assert SAME_GAME_PAIR_GATE_N == 100
    assert same_game_pair_may_gate(n_with=99, n_without=100) is False
    assert same_game_pair_may_gate(n_with=100, n_without=100) is True
    summary = summarize_same_game_pair_grades(
        [
            {"same_game_pair": True, "paid": True},
            {"same_game_pair": True, "paid": False},
            {"same_game_pair": False, "paid": True},
        ]
    )
    assert summary["same_game_pair"]["n"] == 2
    assert summary["no_same_game_pair"]["n"] == 1
    assert summary["may_gate"] is False
