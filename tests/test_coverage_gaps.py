"""Future-sport coverage: grades JSON, tickets, S-D, cover floors."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "grading"))
sys.path.insert(0, str(ROOT))

from prop_hit_tiers import ACTIVE, assign_tier, cover_need, norm_sport  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    TICKET_SPORTS,
    goblin_70_eligible,
    skip_era_half,
    skip_earned_runs,
    skip_hitter_counting,
    standard_flex_kind,
    standard_ticket_eligible,
    ticket_gate_passes,
)
from build_grades_html import GRADED_JSON_SPORTS  # noqa: E402
from build_ticket_eval import (  # noqa: E402
    ALLOWED_TICKET_SPORTS,
    TICKET_EVAL_SPORT_ORDER,
    _leg_match_buckets,
)


def test_nfl_cfb_wcbb_in_graded_json_export():
    keys = {k for k, _ in GRADED_JSON_SPORTS}
    labels = {lab for _, lab in GRADED_JSON_SPORTS}
    assert {"nfl", "cfb", "wcbb", "golf"} <= keys
    assert {"NFL", "CFB", "WCBB", "Golf"} <= labels


def test_run_daily_step_a_requires_cfb_graded_when_step8_present():
    """Saturday CFB used to skip STEP A because CFB was not in gradedExpected."""
    text = (ROOT / "scripts" / "run_daily.ps1").read_text(encoding="utf-8")
    assert "step8GradePairs" in text
    for needle in (
        "graded_cfb_$Yesterday.xlsx",
        "graded_nfl_$Yesterday.xlsx",
        "graded_golf_$Yesterday.xlsx",
        "graded_wcbb_$Yesterday.xlsx",
        "graded_tennis_$Yesterday.xlsx",
        "graded_wnba1h_$Yesterday.xlsx",
        "graded_wnba1q_$Yesterday.xlsx",
        "step8_cfb_direction_clean",
    ):
        assert needle in text, needle
    assert "graded_cfb1h_$Yesterday" not in text
    grader = (ROOT / "scripts" / "run_grader.ps1").read_text(encoding="utf-8")
    assert "CfbWorkbookMissing" in grader
    assert "graded_cfb was not written" in grader


def test_period_and_football_on_ticket_sports():
    for sport in ("WNBA1Q", "WNBA1H", "NBA1Q", "NBA1H", "NFL", "CFB", "Golf", "WCBB"):
        assert sport in TICKET_SPORTS, sport
        assert norm_sport(sport) in ACTIVE or norm_sport(sport) in TICKET_SPORTS


def test_sd_pins_follow_l5eq5_catalog():
    fga = assign_tier(sport="WNBA", pick_type="Goblin", side="OVER", prop="FG Attempted")
    reb_ast = assign_tier(sport="WNBA", pick_type="Goblin", side="OVER", prop="Rebs+Asts")
    pra = assign_tier(sport="WNBA", pick_type="Goblin", side="OVER", prop="Pts+Rebs+Asts")
    assert fga["prop_tier"] == "A"
    assert reb_ast["prop_tier"] == "A"
    assert pra["prop_tier"] == "B"
    nba_pts = assign_tier(sport="NBA", pick_type="Goblin", side="OVER", prop="Points")
    nba1q_reb = assign_tier(sport="NBA1Q", pick_type="Goblin", side="OVER", prop="Rebounds")
    assert nba_pts["prop_tier"] == "A"
    assert nba1q_reb["prop_tier"] == "S"
    cfb_pass = assign_tier(sport="CFB", pick_type="Goblin", side="OVER", prop="Pass Yards")
    assert cfb_pass["prop_tier"] == "C"


def test_cover_floors_basketball_and_football():
    assert cover_need("NBA", "Pts+Rebs+Asts") == 3.7
    assert cover_need("CBB", "FG Attempted") == 1.1
    assert cover_need("NBA1Q", "Points") == 0.87
    assert cover_need("NFL", "Passing Yards") == 15.0
    assert cover_need("CFB", "Pass Yards") == 15.0
    assert cover_need("NFL", "Receptions") == 0.8


def test_wnba1q_and_cfb_clear_ticket_gate():
    q = {
        "sport": "WNBA1Q",
        "player": "A'ja Wilson",
        "prop": "Points",
        "side": "OVER",
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "cover": 1.2,
        "def": "Weak",
    }
    assert ticket_gate_passes(q)
    assert goblin_70_eligible(q)
    thin = dict(q, cover=0.2)
    assert not goblin_70_eligible(thin)


def test_ticket_eval_allows_period_golf_and_football():
    for sport in ("WNBA1Q", "WNBA1H", "GOLF", "NFL", "CFB"):
        assert sport in ALLOWED_TICKET_SPORTS, sport
        assert sport in TICKET_EVAL_SPORT_ORDER, sport
    assert _leg_match_buckets("WNBA1Q")[0] == "WNBA1Q"
    assert _leg_match_buckets("GOLF") == ["GOLF"]
    assert _leg_match_buckets("PGA") == ["GOLF"]


def test_combo_canon_keeps_sd_and_flex_kind():
    assert assign_tier(
        sport="WNBA", pick_type="Standard", side="OVER", prop="points_combo"
    )["prop_tier"] == "B"
    row = {
        "sport": "WNBA",
        "player": "A'ja Wilson",
        "prop": "points_combo",
        "side": "OVER",
        "pick_type": "Standard",
        "l5_over": 5,
        "l10_over": 8,
        "cover": 3.0,
        "def": "Weak",
    }
    assert standard_flex_kind(row) == "wnba_combo_over"
    assert standard_flex_kind(dict(row, l5_over=4)) is None
    steals = {
        "sport": "WNBA",
        "player": "A'ja Wilson",
        "prop": "steals",
        "side": "UNDER",
        "pick_type": "Standard",
        "l5_under": 5,
        "l10_under": 8,
        "cover": -1.0,
        "def": "Elite",
    }
    assert standard_flex_kind(steals) == "wnba_steals_under"
    assert standard_flex_kind(dict(steals, l5_under=4)) is None
    assists = dict(steals, prop="assists")
    assert standard_flex_kind(assists) == "wnba_assists_under"
    assert standard_flex_kind(dict(assists, l5_under=4)) is None


def test_earned_runs_1p5_never_tickets_but_goblin_half_can():
    """Hughes 1.5 is the miss market. Goblin 0.5 is 83% and may ticket."""
    for line in (1.5, 2.5):
        for prop in ("Earned Runs Allowed", "earned_runs", "earned runs"):
            row = {
                "sport": "MLB",
                "player": "Gabriel Hughes",
                "prop": prop,
                "side": "OVER",
                "pick_type": "Goblin",
                "line": line,
                "l5_over": 5,
                "l10_over": 10,
                "cover": 2.0,
                "def": "Weak",
            }
            assert skip_earned_runs(row), (prop, line)
            assert skip_era_half(row), (prop, line)
            assert not ticket_gate_passes(row), (prop, line)
            assert not goblin_70_eligible(row), (prop, line)
            std = dict(row, pick_type="Standard")
            assert not standard_ticket_eligible(std), (prop, line)

    half = {
        "sport": "MLB",
        "player": "Payton Tolle",
        "prop": "Earned Runs Allowed",
        "side": "OVER",
        "pick_type": "Goblin",
        "line": 0.5,
        "l5_over": 5,
        "l10_over": 8,
        "cover": 0.2,
        "def": "Avg",
    }
    assert not skip_earned_runs(half)
    assert ticket_gate_passes(half)
    assert goblin_70_eligible(half)
    assert not goblin_70_eligible(dict(half, l5_over=4))
    assert not goblin_70_eligible(dict(half, l10_over=6))
    assert not standard_ticket_eligible(dict(half, pick_type="Standard"))

    ks = {
        "sport": "MLB",
        "player": "Jesús Luzardo",
        "prop": "Pitcher Strikeouts",
        "side": "OVER",
        "pick_type": "Goblin",
        "line": 2.5,
        "l5_over": 5,
        "l10_over": 8,
        "cover": 1.2,
        "def": "Weak",
        "own_def_tier": "Elite",
    }
    assert not skip_earned_runs(ks)
    assert not skip_hitter_counting(ks)
    assert ticket_gate_passes(ks)
    assert goblin_70_eligible(ks)
    assert not goblin_70_eligible(dict(ks, own_def_tier="Weak"))


def test_total_bases_and_hits_use_ba_l5_d_gate():
    """Hits/TB ticket on BA>=.275 + L5=5 + leaky opp pitch, not L10+cover."""
    for prop in ("Total Bases", "total_bases", "Hits", "hits"):
        row = {
            "sport": "MLB",
            "player": "Michael Harris II",
            "prop": prop,
            "side": "OVER",
            "pick_type": "Goblin",
            "line": 0.5,
            "l5_over": 5,
            "batting_avg": 0.280,
            "cover": 0.4,
            "def": "Weak",
        }
        assert skip_hitter_counting(row), prop
        assert ticket_gate_passes(row), prop
        assert goblin_70_eligible(row), prop
        assert not goblin_70_eligible(dict(row, l5_over=4)), prop
        assert not goblin_70_eligible(dict(row, batting_avg=0.250)), prop
        assert not goblin_70_eligible(dict(row, **{"def": "Avg"})), prop
        std = dict(row, pick_type="Standard")
        assert not standard_ticket_eligible(std), prop


def test_mixer_gates_era_tb_hits_instead_of_hard_ban():
    from combined_slate_tickets import (
        MAIN_MLB_GOBLIN_OVER_ALLOW_NORMS,
        STRONG_BUILDER_MLB_PROPS_NORM,
        _leg_mlb_construction_banned,
    )

    for key in ("earnedrunsallowed", "hits", "totalbases"):
        assert key in MAIN_MLB_GOBLIN_OVER_ALLOW_NORMS, key
        assert key in STRONG_BUILDER_MLB_PROPS_NORM, key
    era_15 = {
        "sport": "MLB",
        "pick_type": "Goblin",
        "direction": "OVER",
        "prop_type": "Earned Runs Allowed",
        "line": 1.5,
        "l5_over": 5,
        "l10_over": 10,
        "cover": 2.0,
        "def": "Weak",
    }
    assert _leg_mlb_construction_banned(era_15)
    era_half = {
        **era_15,
        "line": 0.5,
        "l10_over": 8,
        "cover": 0.2,
        "def": "Avg",
    }
    assert not _leg_mlb_construction_banned(era_half)
    assert _leg_mlb_construction_banned({**era_half, "l10_over": 6})
    tb = {
        "sport": "MLB",
        "pick_type": "Goblin",
        "direction": "OVER",
        "prop_type": "Total Bases",
        "line": 0.5,
        "l5_over": 5,
        "batting_avg": 0.280,
        "cover": 0.4,
        "def": "Weak",
    }
    assert not _leg_mlb_construction_banned(tb)
    assert _leg_mlb_construction_banned({**tb, "batting_avg": 0.250})
    pitcher = {
        "sport": "MLB",
        "pick_type": "Goblin",
        "direction": "OVER",
        "prop_type": "Pitching Outs",
        "l5_over": 5,
        "own_def_tier": "Elite",
    }
    assert not _leg_mlb_construction_banned(pitcher)
    assert _leg_mlb_construction_banned({**pitcher, "own_def_tier": "Weak"})


def test_sport_pure_ticket_ids_do_not_collide():
    from build_goblin70_tickets import SPORT_PURE, SPORT_TID_PREFIX

    prefixes = []
    for sport, *_ in SPORT_PURE:
        prefixes.append(
            SPORT_TID_PREFIX.get(str(sport).upper(), str(sport)[:3].upper())
        )
    assert len(prefixes) == len(set(prefixes)), prefixes
