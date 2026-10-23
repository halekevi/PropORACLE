"""Faceoff win percentage must not grade as faceoffs won, and the prop stays off tickets."""
from scripts.fetch_actuals import _nhl_faceoffs_won
from scripts.grading.leg_grade_utils import slate_grade_row
from utils.dnp_zero_void import FACEOFF_PCT_MISMATCH
from utils.ticket_70_pool import _nhl_faceoffs_excluded, goblin_70_eligible


def test_fw_wins_not_fo_percentage():
    assert _nhl_faceoffs_won({}, {"FW": "11", "FO": "52"}) == 11.0
    assert _nhl_faceoffs_won({"faceoffsWon": "9"}, {"FO": "48"}) == 9.0
    assert _nhl_faceoffs_won({}, {"FO": "50"}) is None


def test_faceoff_pct_actual_voids_at_grade_time():
    row = {
        "sport": "NHL",
        "prop": "Faceoffs Won",
        "line": 5.5,
        "bet_direction": "OVER",
    }
    result, reason, _ = slate_grade_row(44.4, row)
    assert result == "VOID"
    assert reason == FACEOFF_PCT_MISMATCH


def test_faceoff_count_actual_still_grades():
    row = {
        "sport": "NHL",
        "prop": "Faceoffs Won",
        "line": 5.5,
        "bet_direction": "OVER",
    }
    result, reason, _ = slate_grade_row(7.0, row)
    assert result == "HIT"
    assert reason is None


def test_faceoffs_never_ticket():
    row = {
        "sport": "NHL",
        "prop": "Faceoffs Won",
        "pick_type": "Goblin",
        "side": "OVER",
        "season_type": "2",
        "l5_over": 5,
        "l10_over": 8,
        "def": "Weak",
    }
    assert _nhl_faceoffs_excluded(row)
    assert goblin_70_eligible(row) is False
