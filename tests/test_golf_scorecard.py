"""Golf scorecard counts + integrity (Pars attach bug regression)."""

from __future__ import annotations

from utils.golf_scorecard import (
    counting_prop_ok,
    counts_from_hole_scorecard,
    counts_from_summary_stats,
    integrity_flags,
    parse_round_stats,
)


def _hole(strokes: float, to_par_display: str, period: int = 1) -> dict:
    return {
        "value": strokes,
        "displayValue": str(int(strokes)),
        "period": period,
        "scoreType": {"displayValue": to_par_display},
    }


def test_scorecard_counts_birdie_par_bogey():
    # 18 holes: 5 birdies, 10 pars, 2 bogeys, 1 double
    holes = (
        [_hole(3, "-1", i) for i in range(1, 6)]
        + [_hole(4, "E", i) for i in range(6, 16)]
        + [_hole(5, "+1", i) for i in range(16, 18)]
        + [_hole(6, "+2", 18)]
    )
    got = counts_from_hole_scorecard(holes)
    assert got["holes_played"] == 18
    assert got["birdies"] == 5
    assert got["pars"] == 10
    assert got["bogeys"] == 2
    assert got["doubles"] == 1
    assert got["birdies_or_better"] == 5
    assert got["source"] == "scorecard"
    assert not integrity_flags(got)
    assert counting_prop_ok(got)


def test_summary_index5_is_pars_not_holes():
    """Regression: old step4 treated stats[5] as holes and residual-derived pars."""
    # birdies=7, bogeys=3, eagles=0, doubles=1, other=0, pars=7 → 18
    stats = [{"value": v} for v in (7, 3, 0, 1, 0, 7)]
    got = counts_from_summary_stats(stats, strokes=69.0, round_to_par=-2.0)
    assert got["pars"] == 7
    assert got["holes_played"] == 18
    assert got["birdies_or_better"] == 7
    assert got["course_par"] == 71.0
    assert "pars_out_of_range" not in integrity_flags(got)


def test_old_residual_pars_would_fail_integrity():
    # Simulate the bug: treat pars-slot as holes (7), residual pars = 7-7-4 = -4 → clamped garbage
    bogus = {
        "strokes": 69.0,
        "birdies": 7.0,
        "pars": 2.0,  # typical corrupted residual
        "bogeys": 3.0,
        "birdies_or_better": 7.0,
        "bogeys_or_worse": 4.0,
        "holes_played": 7.0,
    }
    flags = integrity_flags(bogus)
    assert (
        "missing_holes" in flags
        or "holes_not_9_or_18" in flags
        or "pars_out_of_range" in flags
    )
    assert not counting_prop_ok(bogus)


def test_parse_round_stats_prefers_scorecard():
    rnd = {
        "value": 69.0,
        "displayValue": "-2",
        "linescores": (
            [_hole(3, "-1", i) for i in range(1, 6)]
            + [_hole(4, "E", i) for i in range(6, 16)]
            + [_hole(5, "+1", i) for i in range(16, 18)]
            + [_hole(6, "+2", 18)]
        ),
        "statistics": {
            "categories": [
                {
                    "stats": [{"value": v} for v in (99, 99, 99, 99, 99, 99)],  # decoy
                }
            ]
        },
    }
    got = parse_round_stats(rnd)
    assert got["source"] == "scorecard"
    assert got["birdies"] == 5
    assert got["pars"] == 10
