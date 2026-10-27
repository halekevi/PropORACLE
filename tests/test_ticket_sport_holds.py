from utils.ticket_keep_registry import rule_for
from utils.ticket_sport_holds import (
    NBA1Q_HOLD_REASON,
    NBA_G70_HOLD_REASON,
    NBA_MIXER_HOLD_REASON,
    nba1q_tickets_held,
    nba_g70_tickets_held,
    nba_mixer_tickets_held,
)


def test_nba1q_held_by_default():
    assert nba1q_tickets_held(env={}) is True
    assert "full-game" in NBA1Q_HOLD_REASON


def test_nba1q_hold_override():
    assert nba1q_tickets_held(env={"PROPORACLE_NBA1Q_TICKETS": "1"}) is False
    assert nba1q_tickets_held(env={"PROPORACLE_NBA1Q_TICKETS": "0"}) is True


def test_nba_g70_held_and_unregistered():
    assert nba_g70_tickets_held(env={}) is True
    assert "0.668" in NBA_G70_HOLD_REASON
    assert rule_for("NBA") is None
    assert rule_for("NBA1H") is None
    assert nba_g70_tickets_held(env={"PROPORACLE_NBA_G70_TICKETS": "1"}) is False


def test_nba_mixer_held_by_default():
    assert nba_mixer_tickets_held(env={}) is True
    assert "7b" in NBA_MIXER_HOLD_REASON
    assert "mid-November" in NBA_MIXER_HOLD_REASON
    assert "70%" in NBA_MIXER_HOLD_REASON
    assert nba_mixer_tickets_held(env={"PROPORACLE_NBA_MIXER_TICKETS": "1"}) is False


def test_nba_mixer_sport_hold_unlock_is_advisory():
    from datetime import date

    from utils.ticket_sport_holds import nba_mixer_sport_hold_unlock_ok

    passing = {"goblin_over_2026_l5eq5": {"n": 80, "hit_rate": 0.74}}
    ok, why = nba_mixer_sport_hold_unlock_ok(
        passing, as_of=date(2026, 10, 27)
    )
    assert ok is False
    assert "before" in why
    # Unlock checklist can pass and the sport hold still stays on.
    ok, why = nba_mixer_sport_hold_unlock_ok(
        passing, as_of=date(2026, 11, 15)
    )
    assert ok is True
    assert "may be turned off" in why
    assert nba_mixer_tickets_held(env={}) is True
    low = {"goblin_over_2026_l5eq5": {"n": 80, "hit_rate": 0.65}}
    ok, why = nba_mixer_sport_hold_unlock_ok(low, as_of=date(2026, 11, 20))
    assert ok is False
    assert "keep the sport hold on" in why
    thin = {"goblin_over_2026_l5eq5": {"n": 8, "hit_rate": 1.0}}
    ok, _why = nba_mixer_sport_hold_unlock_ok(thin, as_of=date(2026, 11, 20))
    assert ok is False


def test_nba_mixer_thin_rs_row_tightens_only():
    from utils.ticket_sport_holds import nba_mixer_thin_rs_row

    assert nba_mixer_thin_rs_row({"sport": "WNBA", "current_season_rs_n": 1}) is False
    assert nba_mixer_thin_rs_row({"sport": "NBA"}) is True
    assert nba_mixer_thin_rs_row({"sport": "NBA", "current_season_rs_n": 3}) is True
    assert nba_mixer_thin_rs_row({"sport": "NBA", "current_season_rs_n": 5}) is False
    assert nba_mixer_thin_rs_row({"sport": "NBA1Q", "Current Season RS N": 4}) is True
    assert nba_mixer_thin_rs_row({"sport": "NBA1H", "current_season_rs_n": 8}) is False


def test_nhl_mixer_held_by_default():
    from utils.ticket_sport_holds import NHL_MIXER_HOLD_REASON, nhl_mixer_tickets_held

    assert nhl_mixer_tickets_held(env={}) is True
    assert "no keep gate" in NHL_MIXER_HOLD_REASON
    assert nhl_mixer_tickets_held(env={"PROPORACLE_NHL_MIXER_TICKETS": "1"}) is False


def test_cbb_held_until_opening_board_clears():
    from utils.ticket_sport_holds import CBB_HOLD_REASON, cbb_tickets_held

    assert cbb_tickets_held(env={}) is True
    assert "opening night" in CBB_HOLD_REASON
    assert cbb_tickets_held(env={"PROPORACLE_CBB_TICKETS": "1"}) is False
    assert cbb_tickets_held(env={"PROPORACLE_CBB_TICKETS": "0"}) is True
