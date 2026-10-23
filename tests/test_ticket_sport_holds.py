from utils.ticket_sport_holds import NBA1Q_HOLD_REASON, nba1q_tickets_held


def test_nba1q_held_by_default():
    assert nba1q_tickets_held(env={}) is True
    assert "full-game" in NBA1Q_HOLD_REASON


def test_nba1q_hold_override():
    assert nba1q_tickets_held(env={"PROPORACLE_NBA1Q_TICKETS": "1"}) is False
    assert nba1q_tickets_held(env={"PROPORACLE_NBA1Q_TICKETS": "0"}) is True
