#!/usr/bin/env python3
"""NFL L5 Sep 1 boundary + weeks 1–3 prior-season face fill."""
from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path


def _load():
    p = (
        Path(__file__).resolve().parents[1]
        / "Sports"
        / "CFB"
        / "scripts"
        / "pipeline"
        / "step5b_attach_boxscore_stats.py"
    )
    spec = importlib.util.spec_from_file_location("step5b_nfl_season", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_nfl_season_start_and_week3_prior_fill():
    mod = _load()
    assert mod._nfl_season_start(dt.date(2026, 9, 20)) == dt.date(2026, 9, 1)
    assert mod._nfl_season_start(dt.date(2026, 2, 8)) == dt.date(2025, 9, 1)
    assert mod._nfl_season_start(dt.date(2026, 8, 15)) == dt.date(2025, 9, 1)

    # Weeks 1–3 (through Week 3 Sunday) allow prior fill; Oct+ does not.
    assert mod._nfl_allow_prior_season_fallback(dt.date(2026, 9, 5))
    assert mod._nfl_allow_prior_season_fallback(dt.date(2026, 9, 20))
    assert not mod._nfl_allow_prior_season_fallback(dt.date(2026, 10, 5))
    assert not mod._nfl_allow_prior_season_fallback(dt.date(2026, 8, 15))

    games = [
        {"game_date": "20260907", "PASS_YDS": 410},
        {"game_date": "20260112", "PASS_YDS": 180},
        {"game_date": "20251228", "PASS_YDS": 220},
        {"game_date": "20251221", "PASS_YDS": 195},
        {"game_date": "20251214", "PASS_YDS": 210},
    ]
    cur, flag = mod._filter_nfl_current_season(games, slate=dt.date(2026, 9, 20))
    assert flag == ""
    assert len(cur) == 1
    assert cur[0]["PASS_YDS"] == 410

    # No current + allow → prior-only L5 in weeks 1–3
    prior_only, flag2 = mod._filter_nfl_current_season(
        games[1:], slate=dt.date(2026, 9, 20), allow_prior_fallback=True
    )
    assert flag2 == "PRIOR_SEASON_L5" and len(prior_only) == 4

    # Closed window (Oct): still NO_CURRENT_SEASON even with allow
    empty, flag_oct = mod._filter_nfl_current_season(
        games[1:], slate=dt.date(2026, 10, 5), allow_prior_fallback=True
    )
    assert empty == [] and flag_oct == "NO_CURRENT_SEASON"

    # Face vals: 1 current + prior → PRIOR_SEASON_FILL
    cur_v = [410.0]
    pri_v = [180.0, 220.0, 195.0, 210.0]
    face, fflag = mod._nfl_face_vals(cur_v, pri_v, dt.date(2026, 9, 20))
    assert fflag == "PRIOR_SEASON_FILL"
    assert face[:5] == [410.0, 180.0, 220.0, 195.0, 210.0]

    face2, fflag2 = mod._nfl_face_vals(cur_v, pri_v, dt.date(2026, 10, 5))
    assert fflag2 == "" and face2 == [410.0]

    # NFLP (Aug): season start is prior Sep 1 → 2025 regular season is "current"
    nflp, flag3 = mod._filter_nfl_current_season(games[1:], slate=dt.date(2026, 8, 15))
    assert flag3 == ""
    assert len(nflp) == 4

    assert mod._cfb_thin_sample_flag(n=1, overs=1, unders=0) == "THIN_CLEAR_OVER"
    assert mod._cfb_thin_sample_flag(n=1, overs=0, unders=1) == "THIN_CLEAR_UNDER"
    assert mod._cfb_thin_sample_flag(n=1, overs=0, unders=0, pushes=1) == "THIN_SEASON"


def test_nfl_current_season_l5_ok_rejects_prior_fill():
    from utils.nfl_keep_gates import nfl_current_season_l5_ok

    assert nfl_current_season_l5_ok({"l5_sample_n": 5, "season_l5_flag": ""}) is True
    assert (
        nfl_current_season_l5_ok(
            {"l5_sample_n": 1, "season_l5_flag": "PRIOR_SEASON_FILL", "l5_over": 5}
        )
        is False
    )
    assert (
        nfl_current_season_l5_ok({"l5_sample_n": 1, "season_l5_flag": "THIN_CLEAR_OVER"})
        is False
    )
