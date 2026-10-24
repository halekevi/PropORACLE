"""CFB prop-aware unit axis (pass / rush / TD / kick / INT / sack / tackle)."""

from utils.cfb_prop_defense import prop_def_axis, prop_def_keys


def test_prop_def_axis_core():
    assert prop_def_axis("pass_yds") == "pass"
    assert prop_def_axis("Pass Yards") == "pass"
    assert prop_def_axis("pass_cmp") == "pass"
    assert prop_def_axis("longest completion") == "pass"
    assert prop_def_axis("rush_yds") == "rush"
    assert prop_def_axis("receptions") == "pass"
    assert prop_def_axis("rec_yds") == "pass"


def test_prop_def_axis_tds_and_kick():
    assert prop_def_axis("pass_td") == "pass_td"
    assert prop_def_axis("rush_td") == "rush_td"
    assert prop_def_axis("rec_td") == "rec_td"
    assert prop_def_axis("player_td") == "td"
    assert prop_def_axis("Player Touchdowns") == "td"
    assert prop_def_axis("fg_made") == "fg"
    assert prop_def_axis("pat_made") == "pat"
    assert prop_def_axis("kicking_points") == "kick"
    assert prop_def_axis("kick_pts") == "kick"


def test_prop_def_axis_int_sack_tackle():
    assert prop_def_axis("int") == "int"
    assert prop_def_axis("Interceptions") == "int"
    assert prop_def_axis("sacks") == "sack"
    assert prop_def_axis("tackles") == "tackle"
    assert prop_def_axis("tfl") == "tfl"


def test_prop_def_axis_combos_longest_targets():
    assert prop_def_axis("pass_rush_yds") == "points"
    assert prop_def_axis("Pass+Rush Yds") == "points"
    assert prop_def_axis("rush_rec_yds") == "points"
    assert prop_def_axis("Rush+Rec Yds") == "points"
    assert prop_def_axis("pass_rush_rec_td") == "td"
    assert prop_def_axis("Pass+Rush+Rec TDs") == "td"
    assert prop_def_axis("pass_long") == "pass"
    assert prop_def_axis("longest completion") == "pass"
    assert prop_def_axis("rush_long") == "rush"
    assert prop_def_axis("longest rush") == "rush"
    assert prop_def_axis("rec_long") == "pass"
    assert prop_def_axis("longest reception") == "pass"
    assert prop_def_axis("rec_tgt") == "pass"
    assert prop_def_axis("Rec Targets") == "pass"


def test_prop_def_keys_match_axis():
    rk, tr, avg = prop_def_keys("fg_made")
    assert rk == "fg_def_rank_nat"
    assert tr == "fg_def_tier_nat"
    rk, _, _ = prop_def_keys("sacks")
    assert rk == "sacks_taken_rank_nat"
    rk, _, _ = prop_def_keys("tackles")
    assert rk == "plays_off_rank_nat"
    rk, _, _ = prop_def_keys("int")
    assert rk == "int_def_rank_nat"
    rk, _, _ = prop_def_keys("player_td")
    assert rk == "td_def_rank_nat"
    rk, _, _ = prop_def_keys("pass_yds")
    assert rk == "pass_def_rank_nat"
    rk, _, _ = prop_def_keys("pass_rush_yds")
    assert rk == "overall_def_rank_nat"
    rk, _, _ = prop_def_keys("pass_rush_rec_td")
    assert rk == "td_def_rank_nat"
    rk, _, _ = prop_def_keys("rec_tgt")
    assert rk == "pass_def_rank_nat"
