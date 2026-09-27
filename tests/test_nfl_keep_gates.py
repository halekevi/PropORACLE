"""NFL Standard UNDER-heavy ticket gate + Week 1–2 priority order + Goblin flip."""

from __future__ import annotations

import utils.nfl_keep_gates as nfl_kg
from utils.nfl_keep_gates import (
    NFL_GOBLIN_OVER_KEEP_FLIP,
    NFL_GOBLIN_OVER_KEEP_PASS_LATER,
    NFL_GOBLIN_TICKETS_ENABLED,
    NFL_GOBLIN_WAVE1_FLIP_MIN_HR,
    NFL_GOBLIN_WAVE1_FLIP_MIN_N,
    NFL_STD_OVER_KEEP,
    NFL_STD_UNDER_KEEP,
    NFL_TICKET_PRIORITY,
    nfl_goblin_is_extreme_low_line,
    nfl_goblin_ticket_eligible,
    nfl_goblin_wave1_flip_go,
    nfl_soft_unit_ticket_ok,
    nfl_standard_ticket_eligible,
    nfl_std_under_skips_l5_eq5,
    nfl_ticket_priority,
)
from utils.ticket_70_pool import goblin_70_eligible, standard_sort_key, ticket_gate_passes


def _row(**kwargs):
    base = {
        "sport": "NFL",
        "pick_type": "Standard",
        "league": "NFL",
        "l5_over": 5,
        "l5_under": 5,
        "l10_over": 9,
        "l10_under": 9,
        "def_tier": "Elite",
        "cover": -10,
        "dist_l5": -10,
    }
    base.update(kwargs)
    return base


def _soft_ctx(prop: str, side: str) -> dict:
    """Minimal soft context so soft-gated keeps clear."""
    if side == "UNDER" and prop == "passing_yards":
        return {"opp_pass_d_tier": "Elite", "own_pass_off_tier": "Avg"}
    if side == "UNDER" and prop in {
        "pass_attempts",
        "pass_completions",
        "passing_tds",
    }:
        return {"opp_pass_d_tier": "Elite"}
    if side == "UNDER" and prop == "completion_percentage":
        return {"opp_pass_d_tier": "Elite", "opp_dl_pass_tier": "Elite"}
    if side == "OVER" and prop == "punts":
        return {"opp_rush_d_tier": "Elite", "def_tier": "Weak", "cover": 2, "dist_l5": 2}
    if side == "OVER" and prop == "rush_yards_per_carry":
        return {
            "opp_rush_d_tier": "Weak",
            "own_rush_off_tier": "Avg",
            "def_tier": "Weak",
            "cover": 1,
            "dist_l5": 1,
        }
    if side == "OVER" and prop == "longest_completion":
        return {
            "qb_mobility_tier": "Mobile",
            "def_tier": "Weak",
            "cover": 5,
            "dist_l5": 5,
        }
    return {}


def test_each_ticketable_under_chip():
    assert set(p for p, s in NFL_TICKET_PRIORITY if s == "UNDER") == NFL_STD_UNDER_KEEP
    for prop in sorted(NFL_STD_UNDER_KEEP):
        r = _row(side="UNDER", prop=prop, **_soft_ctx(prop, "UNDER"))
        assert nfl_standard_ticket_eligible(r) is True, prop


def test_each_ticketable_over_chip():
    assert set(p for p, s in NFL_TICKET_PRIORITY if s == "OVER") == NFL_STD_OVER_KEEP
    for prop in sorted(NFL_STD_OVER_KEEP):
        r = _row(side="OVER", prop=prop, **_soft_ctx(prop, "OVER"))
        assert nfl_standard_ticket_eligible(r) is True, prop


def test_pass_yards_under_requires_soft_stack():
    assert (
        nfl_standard_ticket_eligible(_row(side="UNDER", prop="passing_yards", def_tier="Elite"))
        is False
    )
    ok = _row(
        side="UNDER",
        prop="passing_yards",
        def_tier="Elite",
        opp_pass_d_tier="Elite",
        own_pass_off_tier="Avg",
    )
    assert nfl_soft_unit_ticket_ok(ok) is True
    assert nfl_standard_ticket_eligible(ok) is True
    bad = _row(
        side="UNDER",
        prop="passing_yards",
        def_tier="Elite",
        opp_pass_d_tier="Elite",
        own_pass_off_tier="Elite",
    )
    assert nfl_standard_ticket_eligible(bad) is False


def test_pass_attempts_std_over_still_faded():
    assert (
        nfl_standard_ticket_eligible(
            _row(side="OVER", prop="pass_attempts", def_tier="Weak", cover=20, dist_l5=20)
        )
        is False
    )


def test_punts_over_soft_gate():
    assert nfl_standard_ticket_eligible(_row(side="OVER", prop="punts", def_tier="Weak")) is False
    ok = _row(
        side="OVER",
        prop="punts",
        def_tier="Weak",
        cover=2,
        dist_l5=2,
        opp_rush_d_tier="Elite",
    )
    assert nfl_standard_ticket_eligible(ok) is True


def test_priority_order_rec_before_sacks_before_soft_over():
    rec_catch = _row(side="UNDER", prop="receptions")
    long = _row(side="UNDER", prop="longest_reception")
    sack = _row(side="UNDER", prop="sacks")
    tackles = _row(side="UNDER", prop="tackles_ast")
    rec_yds = _row(side="UNDER", prop="receiving_yards")
    punts = _row(side="OVER", prop="punts", def_tier="Weak", cover=2, dist_l5=2, opp_rush_d_tier="Elite")
    assert (
        nfl_ticket_priority(rec_catch)
        < nfl_ticket_priority(long)
        < nfl_ticket_priority(sack)
        < nfl_ticket_priority(tackles)
        < nfl_ticket_priority(rec_yds)
        < nfl_ticket_priority(punts)
    )
    assert standard_sort_key(rec_catch) < standard_sort_key(sack) < standard_sort_key(punts)


def test_receptions_under_option_c_soft_rank_bump():
    from utils.nfl_keep_gates import nfl_receptions_under_soft_rank_bump

    preferred = _row(
        side="UNDER",
        prop="receptions",
        wopr_low=True,
        opp_pass_d_tier="Elite",
        player="A Preferred",
    )
    other = _row(
        side="UNDER",
        prop="receptions",
        wopr_low=False,
        opp_pass_d_tier="Weak",
        player="B Other",
    )
    assert nfl_receptions_under_soft_rank_bump(preferred) == 0
    assert nfl_receptions_under_soft_rank_bump(other) == 1
    assert standard_sort_key(preferred) < standard_sort_key(other)
    assert nfl_standard_ticket_eligible(preferred) is True
    assert nfl_standard_ticket_eligible(other) is True
    from_rank = _row(
        side="UNDER",
        prop="receptions",
        wopr_L3=0.40,
        opp_pass_def_rank=5,
        player="C FromRank",
    )
    assert nfl_receptions_under_soft_rank_bump(from_rank) == 0


def test_rush_yards_under_and_over_watch():
    assert nfl_standard_ticket_eligible(_row(side="UNDER", prop="rushing_yards")) is False
    assert nfl_standard_ticket_eligible(
        _row(
            side="OVER",
            prop="rushing_yards",
            def_tier="Weak",
            cover=20,
            dist_l5=20,
            opp_rush_d_tier="Weak",
            own_ol_tier="Elite",
        )
    ) is False


def test_pass_std_over_watch_until_l5_volume():
    assert nfl_standard_ticket_eligible(
        _row(
            side="OVER",
            prop="passing_yards",
            def_tier="Weak",
            cover=20,
            dist_l5=20,
            opp_pass_d_tier="Weak",
            own_pass_off_tier="Avg",
        )
    ) is False
    assert nfl_standard_ticket_eligible(
        _row(
            side="OVER",
            prop="passing_tds",
            def_tier="Weak",
            cover=1,
            dist_l5=1,
            opp_dl_pass_tier="Weak",
        )
    ) is False


def test_soft_under_fail_closed_when_pass_d_missing():
    for prop in (
        "passing_yards",
        "pass_attempts",
        "pass_completions",
        "completion_percentage",
        "passing_tds",
    ):
        bare = _row(side="UNDER", prop=prop, def_tier="Elite")
        assert nfl_soft_unit_ticket_ok(bare) is False, prop
        assert nfl_standard_ticket_eligible(bare) is False, prop
    from_rank = _row(
        side="UNDER",
        prop="pass_attempts",
        def_tier="Elite",
        opp_pass_def_rank=3,
    )
    assert nfl_soft_unit_ticket_ok(from_rank) is True
    assert nfl_standard_ticket_eligible(from_rank) is True


def test_fantasy_off_all_books():
    assert nfl_standard_ticket_eligible(_row(side="UNDER", prop="fantasy_score")) is False
    assert nfl_standard_ticket_eligible(
        _row(side="OVER", prop="fantasy_score", def_tier="Weak", cover=10, dist_l5=10)
    ) is False


def test_rec_and_receptions_under_skip_l5_eq5():
    thin_yds = _row(
        side="UNDER",
        prop="receiving_yards",
        l5_under=2,
        l10_under=9,
        def_tier="Elite",
        cover=-10,
        dist_l5=-10,
        l5_sample_n=5,
    )
    assert nfl_std_under_skips_l5_eq5(thin_yds) is True
    from utils.competition_history import row_competition_ok

    if row_competition_ok(thin_yds):
        assert ticket_gate_passes(thin_yds) is True

    thin_rec = _row(
        side="UNDER",
        prop="receptions",
        l5_under=2,
        l10_under=9,
        def_tier="Elite",
        cover=-3,
        dist_l5=-3,
        l5_sample_n=5,
    )
    assert nfl_std_under_skips_l5_eq5(thin_rec) is True
    if row_competition_ok(thin_rec):
        assert ticket_gate_passes(thin_rec) is True

    assert ticket_gate_passes(_row(side="UNDER", prop="longest_reception", l5_under=2)) is False


def test_rec_under_keep_over_fade():
    assert nfl_standard_ticket_eligible(_row(side="UNDER", prop="receiving_yards")) is True
    assert nfl_standard_ticket_eligible(
        _row(side="OVER", prop="receiving_yards", def_tier="Weak", cover=12, dist_l5=12)
    ) is False


def test_nfl_goblin_wave1_live_when_flag_on():
    assert NFL_GOBLIN_TICKETS_ENABLED is True
    gob = _row(
        pick_type="Goblin",
        side="OVER",
        prop="receiving_yards",
        def_tier="Weak",
        cover=12,
        dist_l5=12,
        l5_sample_n=5,
        l5_window_n=5,
    )
    assert nfl_goblin_ticket_eligible(gob) is True
    # competition_history may be thin in unit tests — gate via keep + L5 stack only
    from utils.competition_history import row_competition_ok
    if row_competition_ok(gob):
        assert goblin_70_eligible(gob) is True
    else:
        assert ticket_gate_passes(gob) is False  # competition gate alone
        assert nfl_goblin_ticket_eligible(gob) is True


def test_nfl_goblin_wave1_skips_d():
    from utils.nfl_keep_gates import nfl_goblin_wave1_skip_d

    avg = _row(
        pick_type="Goblin",
        side="OVER",
        prop="fg_made",
        def_tier="Avg",
        cover=1.1,
        dist_l5=1.1,
        line=0.5,
        l5_sample_n=5,
        l5_window_n=5,
    )
    avg["def"] = "Avg"
    assert nfl_goblin_wave1_skip_d(avg) is True
    from utils.competition_history import row_competition_ok

    if row_competition_ok(avg):
        assert ticket_gate_passes(avg) is True
        assert goblin_70_eligible(avg) is True
    pass_later = _row(
        pick_type="Goblin",
        side="OVER",
        prop="passing_yards",
        def_tier="Avg",
        cover=20,
        dist_l5=20,
        l5_sample_n=5,
        l5_window_n=5,
    )
    pass_later["def"] = "Avg"
    assert nfl_goblin_wave1_skip_d(pass_later) is False


def test_extreme_low_goblin_line_helper():
    assert nfl_goblin_is_extreme_low_line(
        _row(pick_type="Goblin", side="OVER", prop="sacks", line=0.5)
    )
    assert not nfl_goblin_is_extreme_low_line(
        _row(pick_type="Goblin", side="OVER", prop="receiving_yards", line=12.5)
    )


def test_ypc_over_soft_gate():
    ok = _row(
        side="OVER",
        prop="rush_yards_per_carry",
        def_tier="Weak",
        cover=1,
        dist_l5=1,
        opp_rush_d_tier="Weak",
        own_rush_off_tier="Avg",
    )
    assert nfl_standard_ticket_eligible(ok) is True


def test_goblin_wave1_flip_go_binary():
    assert NFL_GOBLIN_WAVE1_FLIP_MIN_HR == 0.75
    assert NFL_GOBLIN_WAVE1_FLIP_MIN_N == 5
    # All four clear ≥75% n≥5 → GO
    go_rates = {p: (4, 5) for p in NFL_GOBLIN_OVER_KEEP_FLIP}  # 80%
    go, detail = nfl_goblin_wave1_flip_go(go_rates)
    assert go is True and detail["go"] is True
    # One chip 3/5=60% → NO-GO
    bad = dict(go_rates)
    bad["fg_made"] = (3, 5)
    go2, _ = nfl_goblin_wave1_flip_go(bad)
    assert go2 is False
    # n=4 even at 100% → NO-GO (insufficient)
    thin = {p: (4, 4) for p in NFL_GOBLIN_OVER_KEEP_FLIP}
    go3, _ = nfl_goblin_wave1_flip_go(thin)
    assert go3 is False
    # Missing chip → NO-GO
    missing = {p: (5, 5) for p in NFL_GOBLIN_OVER_KEEP_FLIP if p != "rushing_yards"}
    go4, _ = nfl_goblin_wave1_flip_go(missing)
    assert go4 is False


def test_goblin_wave1_eligible_pass_later_blocked_when_flag_on(monkeypatch):
    """When master flag is on: each wave-1 chip tickets; pass-side stays blocked."""
    monkeypatch.setattr(nfl_kg, "NFL_GOBLIN_TICKETS_ENABLED", True)
    assert nfl_kg.NFL_GOBLIN_TICKETS_ENABLED is True

    for prop in sorted(NFL_GOBLIN_OVER_KEEP_FLIP):
        r = _row(
            pick_type="Goblin",
            side="OVER",
            prop=prop,
            def_tier="Weak",
            cover=12,
            dist_l5=12,
        )
        assert nfl_kg.nfl_goblin_ticket_eligible(r) is True, prop

    for prop in sorted(NFL_GOBLIN_OVER_KEEP_PASS_LATER):
        soft = {}
        if prop in nfl_kg.NFL_GOBLIN_SOFT_UNIT_TICKET_GATES:
            soft = {
                "opp_pass_d_tier": "Weak",
                "own_pass_off_tier": "Avg",
                "opp_dl_pass_tier": "Weak",
                "own_ol_tier": "Elite",
            }
        r = _row(
            pick_type="Goblin",
            side="OVER",
            prop=prop,
            def_tier="Weak",
            cover=12,
            dist_l5=12,
            **soft,
        )
        assert nfl_kg.nfl_goblin_ticket_eligible(r) is False, prop
