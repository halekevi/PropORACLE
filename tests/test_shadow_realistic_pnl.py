"""Month shadow uses N-correct multipliers and drops 1st-place rates."""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from shadow_realistic_pnl import (  # noqa: E402
    _is_first_place,
    breakeven,
    composition_audit,
    expected_multiplier,
    expected_profit,
    load_grade_bundle,
    scraped_rates,
    slip_outcomes,
    wilson_lower,
)


def test_first_place_above_standard_power_is_dropped():
    assert _is_first_place(7.0, 2)
    assert not _is_first_place(1.5, 2)
    assert not _is_first_place(3.0, 2)


def test_scrape_keeps_min_guarantee_and_drops_first_place():
    rows = [
        {
            "n_legs": "2",
            "leg_composition": "0S+2G+0D",
            "power_payout_x": "1.5",
            "flex_n_correct": {},
        },
        {
            "n_legs": "2",
            "leg_composition": "0S+2G+0D",
            "power_payout_x": "7.0",
            "flex_n_correct": {},
        },
        {
            "n_legs": "2",
            "leg_composition": "0S+2G+0D",
            "power_payout_x": "1.2",
            "flex_payout_x": "1.2",
        },
    ]
    cells = scraped_rates(rows)
    power = next(c for c in cells if c["product"] == "Power")
    assert power["n_scrapes"] == 2
    assert power["n_correct"][2] == 1.35
    assert power["max_x"] == 1.5


def test_two_leg_month_expectation_at_55_percent():
    outcomes = slip_outcomes(2, {2: 3.0}, 0.55, 100.0)
    # 20 slips: win +200 with p=0.3025, else -100.
    ev = expected_profit(outcomes, 20)
    assert abs(ev - (20 * (0.3025 * 200 - 0.6975 * 100))) < 0.01


def test_two_leg_audit_separates_all_goblin_from_mixed():
    rows = [
        {"n_legs": "2", "leg_composition": "0S+2G+0D", "power_payout_x": "1.4"},
        {"n_legs": "2", "leg_composition": "0S+2G+0D", "power_payout_x": "7.0"},
        {"n_legs": "2", "leg_composition": "1S+1G+0D", "power_payout_x": "2.5"},
    ]
    audit = composition_audit(rows)
    assert audit["all_goblin"] == 2
    assert audit["mixed_1s1g"] == 1
    assert audit["all_goblin_first_place_dropped"] == 1
    assert audit["all_goblin_n_correct_kept"] == 1


def test_flex_3leg_breakeven_is_one_over_sqrt_3():
    ladder = {3: 3.0, 2: 1.0}
    be = breakeven(3, 3.0, ladder)
    assert be["formula"] == "3p^2"
    assert abs(be["per_leg_rate"] - (1.0 / math.sqrt(3.0))) < 1e-12
    p = be["per_leg_rate"]
    assert abs(expected_multiplier(3, ladder, p) - 1.0) < 1e-9
    assert abs(expected_multiplier(3, ladder, 0.429) - 3 * 0.429 * 0.429) < 1e-9


def test_push_and_void_stay_out_of_the_hit_rate(tmp_path: Path):
    folder = tmp_path / "templates"
    folder.mkdir()
    payload = {
        "props": [
            {"sport": "WNBA", "pick_type": "standard", "direction": "over", "result": "HIT", "player": "A", "prop": "points"},
            {"sport": "WNBA", "pick_type": "standard", "direction": "over", "result": "MISS", "player": "B", "prop": "points"},
            {"sport": "WNBA", "pick_type": "standard", "direction": "over", "result": "PUSH", "player": "C", "prop": "points"},
            {"sport": "WNBA", "pick_type": "standard", "direction": "over", "result": "VOID", "player": "D", "prop": "points"},
        ]
    }
    (folder / "graded_props_2026-01-01.json").write_text(json.dumps(payload), encoding="utf-8")
    bundle = load_grade_bundle(folder)
    cell = bundle["cells"][("WNBA", "Standard", "OVER")]
    assert cell["n"] == 2
    assert cell["hits"] == 1
    audit = bundle["audit"][("Standard", "OVER")]
    assert audit["PUSH"] == 1
    assert audit["VOID"] == 1


def test_nfl_wilson_lower_bound_is_about_75_percent():
    lo = wilson_lower(51, 59)
    assert lo is not None
    assert 0.74 < lo < 0.77
    assert lo < 0.845


def test_goblin_1_4x_fails_rule_9_and_needs_85_percent_legs():
    be = breakeven(2, 1.4)
    assert be["rule9_pass"] is False
    assert abs(be["slip_win_rate"] - (1 / 1.4)) < 1e-9
    assert abs(be["per_leg_rate"] - (1 / 1.4) ** 0.5) < 1e-9
    assert be["per_leg_rate"] > 0.84


def test_flex_one_miss_pays_partial():
    outcomes = slip_outcomes(4, {4: 6.0, 3: 1.5}, 1.0, 100.0)
    sweep = next(profit for prob, profit in outcomes if abs(prob - 1.0) < 1e-9)
    assert sweep == 500.0
