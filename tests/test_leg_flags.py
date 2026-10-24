"""Flags stay on; 1.4x floor + EV at cell−2pts."""
from __future__ import annotations

from utils.leg_flags import (
    MIN_PAYOUT_X,
    collect_leg_flags,
    ev_at_scraped,
    placeable_at_quote,
)


def test_stale_gap_is_flag_not_block():
    flags = collect_leg_flags(
        values=[0, 1, 0, 0, 1],
        line=1.5,
        side="UNDER",
        pick="Standard",
        sport="NBA",
        prop="turnovers",
        wilson=0.55,
        gap_scaled=2.0,
        slice_n=83,
    )
    assert "stale_gap" in flags
    assert "thin_slice" not in flags


def test_thin_slice_wilson_under_bar():
    flags = collect_leg_flags(
        values=[13, 13, 13, 13, 13],
        line=12.5,
        side="OVER",
        pick="Standard",
        sport="TENNIS",
        prop="games_won",
        wilson=0.40,
        gap_scaled=None,
        slice_n=103,
    )
    assert "thin_slice" in flags
    assert "zero_spread" in flags


def test_dnp_risk_soccer_shots_under():
    flags = collect_leg_flags(
        values=[2, 1, 0, 1, 2],
        line=1.5,
        side="UNDER",
        pick="Standard",
        sport="SOCCER",
        prop="shots",
        wilson=0.55,
        slice_n=50,
    )
    assert "dnp_risk" in flags


def test_payout_floor_is_1_4():
    assert MIN_PAYOUT_X == 1.4
    ok, scored = placeable_at_quote([0.80, 0.80], 1.4)
    # (0.78*0.78)*1.4 - 1 = 0.851 - 1 < 0 → EV blocks
    assert scored["quote"] == 1.4
    assert ok is False
    assert scored["ev"] < 0


def test_standard_under_6x_placeable_at_cell_minus_2():
    # Pritchard / Keldon / Towns cell hrs ≈ 0.663 / 0.580 / 0.565
    ok, scored = placeable_at_quote([55 / 83, 175 / 302, 166 / 294], 6.0)
    assert ok is True
    assert scored["ev"] > 0


def test_goblin_3_at_1_65_still_ev_blocked():
    # ~0.72 / 0.70 / 0.69 Wilson-ish cells still fail EV at 1.65 after −2pts
    ok, scored = placeable_at_quote([0.72, 0.70, 0.69], 1.65)
    assert scored["quote"] == 1.65
    assert ok is False
    assert scored["ev"] < 0


def test_not_primary_playmaker_on_assist_under_big():
    # Towns-style: center, no top-2 assist rank on row → flag track only
    flags = collect_leg_flags(
        values=[1, 2, 1, 4, 4],
        line=5.0,
        side="UNDER",
        pick="Standard",
        sport="NBA",
        prop="assists",
        wilson=0.51,
        slice_n=294,
        row={"player": "Karl-Anthony Towns", "team": "NY", "pos": "C"},
    )
    assert "not_primary_playmaker" in flags
    assert "big_man" in flags


def test_castle_preseason_assist_role_flags_not_primary():
    # Last-season #1 APG, but preseason 3/5/3 behind Fox/Harper → flag
    flags = collect_leg_flags(
        values=[4, 5, 5, 4, 3],
        line=6.5,
        side="UNDER",
        pick="Standard",
        sport="NBA",
        prop="assists",
        wilson=0.51,
        slice_n=294,
        row={"player": "Stephon Castle", "team": "SA", "pos": "G"},
    )
    assert "not_primary_playmaker" in flags


def test_ev_haircut():
    scored = ev_at_scraped([0.70, 0.70], 2.0)
    assert abs(scored["p_win"] - 0.68 * 0.68) < 1e-9


def test_roster_changed_flag_from_row():
    flags = collect_leg_flags(
        values=[16, 17, 19, 13, 19],
        line=21.5,
        side="UNDER",
        pick="Standard",
        sport="NBA",
        prop="FG Attempted",
        row={"roster_changed": 1, "roster_note": "BOS: out Jaylen Brown"},
        wilson=0.55,
        slice_n=50,
    )
    assert "roster_changed" in flags
