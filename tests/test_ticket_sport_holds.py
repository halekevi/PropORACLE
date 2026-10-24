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
    assert "step7b" in NBA_MIXER_HOLD_REASON
    assert nba_mixer_tickets_held(env={"PROPORACLE_NBA_MIXER_TICKETS": "1"}) is False


def test_nhl_mixer_held_by_default():
    from utils.ticket_sport_holds import NHL_MIXER_HOLD_REASON, nhl_mixer_tickets_held

    assert nhl_mixer_tickets_held(env={}) is True
    assert "no keep gate" in NHL_MIXER_HOLD_REASON
    assert nhl_mixer_tickets_held(env={"PROPORACLE_NHL_MIXER_TICKETS": "1"}) is False
