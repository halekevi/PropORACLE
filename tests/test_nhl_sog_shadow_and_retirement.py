"""Shadow NHL shots line 0.5 stays off tickets. Retirement grades need both readings."""
from __future__ import annotations

from utils.nhl_sog_shadow import is_nhl_goblin_sog_line05
from utils.tennis_retirement import grade_retirement, grade_walkover, readings
from utils.ticket_70_pool import goblin_70_eligible


def test_cina_set_in_progress_credit():
    played_gw, played_total, credited_gw, credited_total = readings(
        [6, 3], [4, 0], player_won=True
    )
    assert (played_gw, played_total) == (9, 13)
    assert (credited_gw, credited_total) == (12, 16)


def test_seven_six_then_unstarted_set_credits_nineteen():
    _gw, played_total, _cgw, credited_total = readings([7], [6], player_won=True)
    assert played_total == 13
    assert credited_total == 19


def test_disagreement_is_unresolved_not_a_grade():
    # Over 10.5: played 9 misses, credited 12 hits.
    out = grade_retirement(10.5, "OVER", [6, 3], [4, 0], player_won=True, prop="games_won")
    assert out["result"] == "UNRESOLVED"
    assert out["played_grade"] == "MISS"
    assert out["credited_grade"] == "HIT"


def test_agreement_records_the_grade():
    out = grade_retirement(8.5, "OVER", [6, 3], [4, 0], player_won=True, prop="games_won")
    assert out["result"] == "HIT"


def test_walkover_has_no_games_grade():
    assert grade_walkover("games_won")["result"] == "UNRESOLVED"
    assert grade_walkover("aces") is None


def test_nhl_line05_is_shadow_and_not_a_ticket():
    row = {
        "sport": "NHL",
        "pick_type": "Goblin",
        "direction": "OVER",
        "prop": "Shots On Goal",
        "line": 0.5,
        "l5_over": 5,
        "l10_over": 8,
        "season_type": "regular",
    }
    assert is_nhl_goblin_sog_line05(row) is True
    assert goblin_70_eligible(row) is False
