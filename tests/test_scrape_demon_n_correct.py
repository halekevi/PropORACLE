"""Demon N-correct: never 1st-place, never Balance, no fixed one-step table."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parents[1] / "scripts")]

from scrape_demon_n_correct import (
    STD_POWER_FIRST,
    ladder_is_implausible,
    parse_demon_power_quote,
    power_n_correct_x,
)


TWO_LEG = """
Power Play
You must hit 1st place, or 2 out of 2 in the lineup.
1st place pays
3X
Minimum Guarantee
2 correct pays
8.76X
Entry Fee
$20
To Win
$175.20
Balance: $652.41
Reversion lineup payouts are different than standard.
"""

THREE_LEG = """
Power Play
1st place pays
6X
Minimum Guarantee
3 correct pays
20X
Entry Fee
$20
To Win
$400.00
Balance: $652.41
Reversion lineup payouts are different than standard.
"""

FOUR_LEG_REB = """
Flex Play
1st place pays 6X
4 correct pays 67X
3 correct pays 6.5X
Power Play
1st place pays
10X
Minimum Guarantee
4 correct pays
126X
Entry Fee
$20
To Win
$2,520.00
Balance: $652.41
Reversion lineup payouts are different than standard.
"""

SCRAPER_3_NO_TOWIN = """
Power Play
1st place pays
6X
Minimum Guarantee
3 correct pays
40X
Entry Fee
$
To Win
Balance: $652.41
"""


TWO_STD = """
Power Play
You must hit 1st place, or 2 out of 2 in the lineup.
1st place pays
3X
Minimum Guarantee
2 correct pays
3X
Entry Fee
$20
To Win
$60.00
Balance: $652.41
Reversion lineup payouts are different than standard.
"""


def test_two_standards_3x_is_trusted_n_correct():
    rec = parse_demon_power_quote(TWO_STD, n_legs=2)
    assert rec["quote_trusted"] is True
    assert rec["power_x"] == 3.0
    assert rec["to_win"] == 60.0
    assert rec["first_place_x"] == 3.0
    # 1st-place is also 3x on a 2-leg Standard; To Win is what makes it trusted.


def test_screenshot_2_and_3_match_to_win_not_first_place():
    two = parse_demon_power_quote(TWO_LEG, n_legs=2)
    assert two["quote_trusted"] is True
    assert two["power_x"] == 8.76
    assert two["first_place_x"] == 3.0
    assert two["first_place_x"] == STD_POWER_FIRST[2]
    assert two["reversion_disclaimer"] is True
    three = parse_demon_power_quote(THREE_LEG, n_legs=3)
    assert three["power_x"] == 20.0
    assert three["first_place_x"] == 6.0


def test_four_leg_126_from_to_win():
    rec = parse_demon_power_quote(FOUR_LEG_REB, n_legs=4)
    assert rec["quote_trusted"] is True
    assert rec["power_x"] == 126.0
    assert rec["to_win"] == 2520.0
    assert rec["first_place_x"] == 10.0


def test_balance_is_not_to_win_and_40x_without_entry_is_untrusted():
    rec = parse_demon_power_quote(SCRAPER_3_NO_TOWIN, n_legs=3)
    assert rec["to_win"] is None
    assert rec["quote_trusted"] is False
    assert rec["power_x"] is None
    assert rec["n_correct_raw"] == 40.0
    assert rec["first_place_x"] == 6.0


def test_cdp_table_3_5_40_540_is_implausible():
    assert ladder_is_implausible({2: 3.5, 3: 40.0, 6: 540.0})
    assert not ladder_is_implausible({2: 8.76, 3: 20.0, 4: 126.0})


def test_to_win_wins_when_n_correct_label_is_10x():
    text = """
Power Play
1st place pays 6X
3 correct pays 40X
Entry Fee
$20
To Win
$80.00
"""
    rec = parse_demon_power_quote(text, n_legs=3)
    assert rec["implied_x"] == 4.0
    assert rec["n_correct_raw"] == 40.0
    assert rec["quote_trusted"] is True
    assert rec["power_x"] == 4.0


def test_power_n_correct_still_reads_label():
    assert power_n_correct_x({"raw_slip_section": FOUR_LEG_REB}, 4) == 126.0
