"""NBA board prop map: card face and tab spellings share one key."""
from __future__ import annotations

from utils.pp_board_prop_map import lookup_category
from utils.prop_norm import canon_prop, prop_key


def test_card_face_2pt_att_maps_to_two_pointers_attempted():
    cat = lookup_category("NBA", "2-PT Att")
    assert cat is not None
    assert cat.key == "fg2a"
    assert cat.card_label == "Two Pointers Attempted"
    assert prop_key("2-PT Att", "NBA") == "fg2a"
    assert prop_key("Two Pointers Attempted", "NBA") == "fg2a"
    assert canon_prop("NBA", "2-PT Att") == "fg2a"


def test_quarters_plus_points_chips_map():
    q3 = lookup_category("NBA", "Quarters with 3+ Points")
    q5 = lookup_category("NBA", "Quarters with 5+ Points")
    assert q3 is not None and q3.key == "quarters_3plus"
    assert q5 is not None and q5.key == "quarters_5plus"
