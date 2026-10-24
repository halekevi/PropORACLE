#!/usr/bin/env python3
"""CFB L5/L8 fill: this player's last year, all season, tagged, not Diamond."""
from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402


def _load():
    p = (
        Path(__file__).resolve().parents[1]
        / "Sports"
        / "CFB"
        / "scripts"
        / "pipeline"
        / "step5b_attach_boxscore_stats.py"
    )
    spec = importlib.util.spec_from_file_location("step5b_cfb_season", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_week5_is_inside_fill_window():
    mod = _load()
    assert mod._cfb_week0_saturday(dt.date(2026, 8, 1)) == dt.date(2026, 8, 29)
    # Week 5 Sunday 2026-10-04; Week 6 Saturday is outside.
    assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 8, 15))
    assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 9, 20))
    assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 9, 21))
    assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 9, 26))
    assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 10, 4))
    # October still uses last year. The stop is 8 current games, not Week 6.
    assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 10, 5))
    assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 10, 9))


def test_season_start_and_current_only_filter():
    mod = _load()
    assert mod._cfb_season_start(dt.date(2026, 9, 19)) == dt.date(2026, 8, 1)

    games = [
        {"game_date": "20260912", "RUSH_YDS": 104},
        {"game_date": "20260904", "RUSH_YDS": 24},
        {"game_date": "20251227", "RUSH_YDS": 80},
        {"game_date": "20251122", "RUSH_YDS": 70},
        {"game_date": "20251108", "RUSH_YDS": 90},
    ]
    cur, flag = mod._filter_cfb_current_season(games, slate=dt.date(2026, 9, 19))
    assert flag == ""
    assert [g["RUSH_YDS"] for g in cur] == [104, 24]

    empty, flag2 = mod._filter_cfb_current_season(
        games[2:], slate=dt.date(2026, 9, 19), allow_prior_fallback=False
    )
    assert empty == [] and flag2 == "NO_CURRENT_SEASON"

    # Weeks 0–5: prior-only attach is allowed (this player's 2025 games).
    prior, flag3 = mod._filter_cfb_current_season(
        games[2:], slate=dt.date(2026, 9, 26), allow_prior_fallback=True
    )
    assert len(prior) == 3 and flag3 == "PRIOR_SEASON_L5"

    # October still keeps this player's prior boxes when asked.
    late, flag4 = mod._filter_cfb_current_season(
        games[2:], slate=dt.date(2026, 10, 5), allow_prior_fallback=True
    )
    assert len(late) == 3 and flag4 == "PRIOR_SEASON_L5"


def test_face_vals_fill_week3_and_tag():
    mod = _load()
    cur = [104.0, 24.0]
    pri = [80.0, 70.0, 90.0]
    face, flag = mod._cfb_face_vals(cur, pri, dt.date(2026, 9, 19))
    assert flag == "PRIOR_SEASON_FILL"
    assert face[:5] == [104.0, 24.0, 80.0, 70.0, 90.0]
    over5, under5, *_ = mod.hit_rates(face, 50.5, 5)
    assert over5 == 4 and under5 == 1
    # Honest sample n is still the 2 current games.
    assert len(cur) == 2
    assert mod._cfb_thin_sample_flag(n=2, overs=1, unders=1) == "THIN_MIXED"


def test_face_vals_fill_still_open_week4_saturday():
    mod = _load()
    face, flag = mod._cfb_face_vals(
        [104.0, 24.0], [80.0, 70.0, 90.0], dt.date(2026, 9, 26)
    )
    assert flag == "PRIOR_SEASON_FILL"
    assert face[:5] == [104.0, 24.0, 80.0, 70.0, 90.0]


def test_face_vals_october_still_fills_to_eight():
    mod = _load()
    cur = [104.0, 80.0, 90.0, 70.0, 60.0]
    pri = [55.0, 40.0, 30.0, 20.0]
    face, flag = mod._cfb_face_vals(cur, pri, dt.date(2026, 10, 9))
    assert flag == "PRIOR_SEASON_FILL"
    assert face[:5] == cur
    assert face[:8] == cur + pri[:3]
    over5, *_ = mod.hit_rates(face, 50.5, 5)
    over8, *_ = mod.hit_rates(face, 50.5, 8)
    assert over5 == 5
    assert over8 == 6


def test_week6_l5_can_be_current_while_l10_still_includes_last_year():
    """Six current games: L5 is all current. L10 still takes last year.

    PRIOR_SEASON_FILL stays on because the series is padded to 8. The L5
    checklist is l5 current-n < 5, not that string. Freeman's four-game
    window is the case the L5 flag is for.
    """
    mod = _load()
    cur = [40.0, 55.0, 70.0, 80.0, 90.0, 100.0]
    pri = [143.0, 20.0, 30.0, 15.0]
    face, flag = mod._cfb_face_vals(cur, pri, dt.date(2026, 10, 10))
    assert flag == "PRIOR_SEASON_FILL"
    assert mod._face_window_current_n(cur, 5) == 5
    assert face[:5] == cur[:5]
    assert mod._face_window_current_n(cur, 10) == 6
    assert face[6] == 143.0
    over5, *_ = mod.hit_rates(face, 50.5, 5)
    over10, *_ = mod.hit_rates(face, 50.5, 10)
    assert over5 == 4
    assert over10 == 6

    freeman_cur = [40.0, 55.0, 70.0, 80.0]
    freeman, freeman_flag = mod._cfb_face_vals(
        freeman_cur, [143.0, 20.0, 30.0, 15.0, 10.0, 8.0], dt.date(2026, 10, 10)
    )
    assert freeman_flag == "PRIOR_SEASON_FILL"
    assert mod._face_window_current_n(freeman_cur, 5) == 4
    assert freeman[4] == 143.0
    assert mod._face_window_current_n(freeman_cur, 10) == 4


def test_face_vals_stops_at_eight_current_games():
    mod = _load()
    cur = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]
    face, flag = mod._cfb_face_vals(cur, [1.0, 2.0, 3.0], dt.date(2026, 11, 15))
    assert flag == ""
    assert face == cur


def test_face_vals_prior_only_all_season():
    mod = _load()
    face, flag = mod._cfb_face_vals([], [80.0, 70.0, 90.0], dt.date(2026, 9, 26))
    assert flag == "PRIOR_SEASON_L5"
    assert face == [80.0, 70.0, 90.0]
    face2, flag2 = mod._cfb_face_vals([], [80.0], dt.date(2026, 10, 5))
    assert flag2 == "PRIOR_SEASON_L5" and face2 == [80.0]


def test_face_games_player_series_only():
    """Fill concatenates this athlete's current+prior boxes — not a team mean."""
    mod = _load()
    current = [{"espn_athlete_id": "1", "RUSH_YDS": 104, "game_date": "20260912"}]
    prior = [{"espn_athlete_id": "1", "RUSH_YDS": 80, "game_date": "20251108"}]
    other = [{"espn_athlete_id": "2", "RUSH_YDS": 999, "game_date": "20251108"}]
    face = mod._cfb_face_games(current, prior, allow_fill=True)
    assert [g["espn_athlete_id"] for g in face] == ["1", "1"]
    assert other[0] not in face


def test_thin_sample_split():
    mod = _load()
    assert mod._cfb_thin_sample_flag(n=2, overs=2, unders=0) == "THIN_CLEAR_OVER"
    assert mod._cfb_thin_sample_flag(n=2, overs=1, unders=1) == "THIN_MIXED"
    assert mod._cfb_thin_sample_flag(n=5, overs=5, unders=0) == ""


def test_goblin70_accepts_prior_fill_face_with_thin_sample_n():
    """Tickets use face L5 (filled to 5), not l5_sample_n (stays current=2).

    L10>=8 is not required. A thin current L5 still fails.
    """
    row = {
        "sport": "CFB",
        "player": "Caden Knighten",
        "prop": "Rush Yards",
        "pick_type": "Goblin",
        "side": "OVER",
        "line": 19.5,
        "l5_over": 5,
        "l5_under": 0,
        "l10_over": 8,
        "l10_under": 2,
        "def_tier": "Weak",
        "season_type": 2,
        "cover": 30.0,
        "dist_l5": 29.1,
        "season_l5_flag": "PRIOR_SEASON_FILL",
        "l5_sample_n": 2,
        "games_used": 2,
    }
    assert goblin_70_eligible(row)
    short_l10 = dict(row)
    short_l10["l10_over"] = 3
    short_l10.pop("l10_under")
    assert goblin_70_eligible(short_l10)
    thin = dict(row)
    thin["l5_over"] = 2
    thin["season_l5_flag"] = ""
    assert not goblin_70_eligible(thin)


def test_missing_stat_keeps_its_game_and_does_not_backfill():
    """Fifita: two 2026 games have yards and no attempts. Last year must not fill those slots."""
    mod = _load()
    games = [
        {"game_date": "20261004", "PASS_YDS": 284, "PASS_ATT": 37},
        {"game_date": "20260926", "PASS_YDS": 297, "PASS_ATT": 36},
        {"game_date": "20260920", "PASS_YDS": 325, "PASS_ATT": 34},
        {"game_date": "20260912", "PASS_YDS": 230, "PASS_ATT": None},
        {"game_date": "20260906", "PASS_YDS": 279, "PASS_ATT": None},
        {"game_date": "20251226", "PASS_YDS": 310, "PASS_ATT": 43},
        {"game_date": "20251122", "PASS_YDS": 280, "PASS_ATT": 45},
    ]
    played, vals = mod._cfb_played_vals(games, "pass_attempts")
    assert len(played) == 7
    assert vals[:5] == [37.0, 36.0, 34.0, None, None]
    face, _flag = mod._cfb_face_vals(vals[:5], vals[5:], dt.date(2026, 10, 10))
    over5, under5, _push, hr_over, *_ = mod.hit_rates(face, 33.5, 5)
    assert over5 == 3
    assert under5 == 0
    assert hr_over == 0.6


def test_zero_completions_beside_pass_yards_are_a_hole():
    """Browne: CMP stored as 0 on a 329-yard game is a blank, not an under."""
    mod = _load()
    games = [
        {"game_date": "20261003", "PASS_YDS": 255, "PASS_CMP": 18},
        {"game_date": "20260926", "PASS_YDS": 111, "PASS_CMP": 18},
        {"game_date": "20260920", "PASS_YDS": 378, "PASS_CMP": 19},
        {"game_date": "20260912", "PASS_YDS": 329, "PASS_CMP": 0.0},
        {"game_date": "20260904", "PASS_YDS": 317, "PASS_CMP": 0.0},
        {"game_date": "20251115", "PASS_YDS": 240, "PASS_CMP": 12},
    ]
    _played, vals = mod._cfb_played_vals(games, "pass_completions")
    assert vals[:5] == [18.0, 18.0, 19.0, None, None]
    over5, under5, *_ = mod.hit_rates(vals, 20.5, 5)
    assert under5 == 3
    assert over5 == 0
    # A real 0-completion game (no yards) still counts.
    real_zero = [{"game_date": "20261003", "PASS_YDS": 0, "PASS_CMP": 0, "RUSH_YDS": 40}]
    _played, zero_vals = mod._cfb_played_vals(real_zero, "pass_completions")
    assert zero_vals == [0.0]


def test_mixed_date_formats_keep_2026_ahead_of_last_year():
    mod = _load()
    games = [
        {"game_date": "2025-12-26", "KICK_PTS": 9},
        {"game_date": "20261003", "KICK_PTS": 8},
        {"game_date": "2026-09-27", "KICK_PTS": 9},
        {"game_date": "20260919", "KICK_PTS": 11},
        {"game_date": "20260912", "KICK_PTS": 7},
        {"game_date": "2025-11-28", "KICK_PTS": 5},
    ]
    ordered = sorted(
        games,
        key=lambda g: mod._parse_box_date(g["game_date"]) or dt.date.min,
        reverse=True,
    )
    _played, vals = mod._cfb_played_vals(ordered, "kicking_points")
    assert vals[:5] == [8.0, 9.0, 11.0, 7.0, 9.0]
    over5, under5, *_ = mod.hit_rates(vals, 6.5, 5)
    assert over5 == 5 and under5 == 0
