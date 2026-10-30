"""Golf scorecard counts + integrity (Pars attach bug regression)."""

from __future__ import annotations

from utils.golf_scorecard import (
    counting_prop_ok,
    counts_from_hole_scorecard,
    counts_from_summary_stats,
    integrity_flags,
    is_match_play_event,
    parse_round_stats,
)
from utils.player_name_utils import normalize_player_name


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
    assert not integrity_flags(got)


def test_real_extreme_rounds_are_not_suspect():
    # Kevin Yu: 2 birdies + 16 pars for a 68/69
    yu = {
        "strokes": 69.0,
        "birdies": 2.0,
        "pars": 16.0,
        "birdies_or_better": 2.0,
        "bogeys_or_worse": 0.0,
        "holes_played": 18.0,
        "course_par": 71.0,
        "to_par": -2.0,
    }
    assert not integrity_flags(yu)
    # Russell Henley: 18 straight pars for a 70
    henley = {
        "strokes": 70.0,
        "birdies": 0.0,
        "pars": 18.0,
        "birdies_or_better": 0.0,
        "bogeys_or_worse": 0.0,
        "holes_played": 18.0,
        "course_par": 70.0,
        "to_par": 0.0,
    }
    assert not integrity_flags(henley)
    # Justin Lower: 9 birdies, 5 pars, 4 bogeys+
    lower = {
        "strokes": 66.0,
        "birdies": 9.0,
        "pars": 5.0,
        "birdies_or_better": 9.0,
        "bogeys_or_worse": 4.0,
        "holes_played": 18.0,
        "course_par": 72.0,
        "to_par": -6.0,
    }
    assert not integrity_flags(lower)
    # Scheffler 61 with 11 birdies
    scheffler = {
        "strokes": 61.0,
        "birdies": 11.0,
        "pars": 7.0,
        "birdies_or_better": 11.0,
        "bogeys_or_worse": 0.0,
        "holes_played": 18.0,
        "course_par": 72.0,
        "to_par": -11.0,
    }
    assert not integrity_flags(scheffler)


def test_impossible_and_inconsistent_still_flag():
    assert "strokes_impossible" in integrity_flags(
        {"strokes": 0.0, "holes_played": 13.0, "pars": 0.0}
    )
    assert "holes_not_9_or_18" in integrity_flags(
        {
            "strokes": 71.0,
            "holes_played": 17.0,
            "pars": 10.0,
            "birdies_or_better": 4.0,
            "bogeys_or_worse": 3.0,
            "course_par": 71.0,
            "to_par": 0.0,
        }
    )
    assert "bob_bow_pars_ne_holes" in integrity_flags(
        {
            "strokes": 70.0,
            "holes_played": 18.0,
            "pars": 10.0,
            "birdies_or_better": 5.0,
            "bogeys_or_worse": 1.0,  # 16 != 18
            "course_par": 72.0,
            "to_par": -2.0,
        }
    )
    bogus = {
        "strokes": 69.0,
        "birdies": 7.0,
        "pars": 2.0,
        "bogeys": 3.0,
        "birdies_or_better": 7.0,
        "bogeys_or_worse": 4.0,
        "holes_played": 7.0,
    }
    flags = integrity_flags(bogus)
    assert "missing_holes" in flags or "holes_not_9_or_18" in flags
    assert not counting_prop_ok(bogus)


def test_match_play_events_excluded():
    assert is_match_play_event("Presidents Cup")
    assert is_match_play_event("2026 Presidents Cup")
    assert is_match_play_event("Ryder Cup")
    assert not is_match_play_event("Baycurrent Classic")
    assert not is_match_play_event("TOUR Championship")


def test_name_transliteration_matches_pp():
    assert normalize_player_name("Rasmus Højgaard").lower() == "rasmus hojgaard"
    assert normalize_player_name("Thorbjørn Olesen").lower() == "thorbjorn olesen"
    assert normalize_player_name("Ludvig Åberg").lower() == "ludvig aberg"
    assert "hjgaard" not in normalize_player_name("Rasmus Højgaard").lower()


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
