"""Visible Total Games tight-match note (#11–25 opponents)."""
from utils.ticket_tier_defense_gates import tennis_tight_match_note


def test_over_note_tags_nakashima_band():
    note = tennis_tight_match_note(
        "Total Games", 22, direction="OVER", opp_name="Brandon Nakashima"
    )
    assert note is not None
    assert "Nakashima" in note
    assert "#22" in note
    assert "OVER fades" in note
    assert "11–25" in note


def test_under_note_keeps_over_fade_and_warns_mid_line():
    note = tennis_tight_match_note(
        "Total Games", 22, direction="UNDER", opp_name="Brandon Nakashima"
    )
    assert note is not None
    assert "OVER fades" in note
    assert "UNDER also fades" in note


def test_games_won_and_out_of_band_have_no_note():
    assert tennis_tight_match_note("Total Games Won", 22, direction="OVER") is None
    assert tennis_tight_match_note("Total Games", 9, direction="OVER") is None
    assert tennis_tight_match_note("Total Games", 70, direction="OVER") is None
    assert tennis_tight_match_note("match_total_games", 22, direction="OVER") is not None
