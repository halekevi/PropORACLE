from utils.income_tracks import is_card_track, row_in_scope


def test_card_track_keeps_main_and_unlabeled():
    assert is_card_track("graded_main")
    assert is_card_track("")
    assert is_card_track("goblin70")
    assert is_card_track("goblin_only_3leg")


def test_card_track_drops_shadows_yolo_long():
    assert not is_card_track("long_parlay")
    assert not is_card_track("strong_recombo_shadow")
    assert not is_card_track("winrate_goblin_opt3_shadow")
    assert not is_card_track("goblin70_yolo")
    assert not is_card_track("YOLO Goblin-70 Power 6")


def test_g70_scope_cuts_pre_august_26():
    old = {"date": "2026-07-28", "track": "graded_main"}
    new = {"date": "2026-08-26", "track": "graded_main"}
    shadow = {"date": "2026-08-27", "track": "long_parlay"}
    assert row_in_scope(old, scope="card")
    assert not row_in_scope(old, scope="g70")
    assert row_in_scope(new, scope="g70")
    assert not row_in_scope(shadow, scope="g70")
    assert row_in_scope(shadow, scope="all")
