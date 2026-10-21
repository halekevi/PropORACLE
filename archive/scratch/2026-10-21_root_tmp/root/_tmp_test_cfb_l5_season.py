#!/usr/bin/env python3
"""Unit tests for CFB L5 current-season boundary."""
from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

p = Path(r"H:/PropORACLE/Sports/CFB/scripts/pipeline/step5b_attach_boxscore_stats.py")
spec = importlib.util.spec_from_file_location("step5b", p)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(mod)

# Season start
assert mod._cfb_season_start(dt.date(2026, 9, 19)) == dt.date(2026, 8, 1)
assert mod._cfb_season_start(dt.date(2026, 1, 10)) == dt.date(2025, 8, 1)
assert mod._cfb_season_start(dt.date(2026, 8, 1)) == dt.date(2026, 8, 1)

games = [
    {"game_date": "20260912", "RUSH_YDS": 104},
    {"game_date": "20260904", "RUSH_YDS": 24},
    {"game_date": "20251227", "RUSH_YDS": 0},
    {"game_date": "20251122", "RUSH_YDS": 0},
    {"game_date": "20251108", "RUSH_YDS": 0},
]
cur, flag = mod._filter_cfb_current_season(games, slate=dt.date(2026, 9, 19))
assert flag == ""
assert len(cur) == 2
assert [g["RUSH_YDS"] for g in cur] == [104, 24]

# No current season — refuse prior unless fallback
empty, flag2 = mod._filter_cfb_current_season(
    games[2:], slate=dt.date(2026, 9, 19), allow_prior_fallback=False
)
assert empty == [] and flag2 == "NO_CURRENT_SEASON"

# Weeks 0–3: prior-only fallback allowed (this player's 2025 games)
prior, flag3 = mod._filter_cfb_current_season(
    games[2:], slate=dt.date(2026, 9, 19), allow_prior_fallback=True
)
assert len(prior) == 3 and flag3 == "PRIOR_SEASON_L5"

# After Week 3: refuse prior pad even with allow_prior_fallback
late, flag_late = mod._filter_cfb_current_season(
    games[2:], slate=dt.date(2026, 9, 26), allow_prior_fallback=True
)
assert late == [] and flag_late == "NO_CURRENT_SEASON"

# Week 0/1 only: prior fallback still allowed
early, flag4 = mod._filter_cfb_current_season(
    games[2:], slate=dt.date(2026, 8, 15), allow_prior_fallback=True
)
assert len(early) == 3 and flag4 == "PRIOR_SEASON_L5"
assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 8, 15))
assert mod._cfb_allow_prior_season_fallback(dt.date(2026, 9, 19))
assert not mod._cfb_allow_prior_season_fallback(dt.date(2026, 9, 26))

# Thin-sample risk split: consistent CLEAR vs Jones-type MIXED
assert mod._cfb_thin_sample_flag(n=2, overs=2, unders=0) == "THIN_CLEAR_OVER"
assert mod._cfb_thin_sample_flag(n=2, overs=0, unders=2) == "THIN_CLEAR_UNDER"
assert mod._cfb_thin_sample_flag(n=2, overs=1, unders=1) == "THIN_MIXED"  # Jones U37.5
assert mod._cfb_thin_sample_flag(n=3, overs=3, unders=0) == "THIN_CLEAR_OVER"
assert mod._cfb_thin_sample_flag(n=5, overs=5, unders=0) == ""
assert mod._cfb_thin_sample_flag(n=2, overs=0, unders=0, pushes=2) == "THIN_SEASON"

print("OK — CFB season-boundary filter")
