"""NBA projection layers: walk-forward base rates + shrunk P(over line)."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from utils.expanding_marginals import (  # noqa: E402
    cell_key,
    compute_expanding_marginals_lookup,
    lookup_base_rate,
)
from utils.nba_player_projection import (  # noqa: E402
    NbaBoxIndex,
    nba_season_start,
    pick_type_breakeven,
    pos_group,
    project_player_stat,
)
from utils.nba_proj_k import K_MEAN_DEFAULT, k_mean_for  # noqa: E402
from utils.player_prob_shrinkage import shrink_prob_toward_baseline, shrink_toward_baseline  # noqa: E402
from utils.stat_distributions import choose_family, line_probs, p_over_line, p_over_poisson  # noqa: E402


def test_poisson_p_over_half_line():
    p = p_over_poisson(1.0, 0.5)
    assert abs(p - (1.0 - math.exp(-1.0))) < 1e-6
    p2, fam = p_over_line(2.0, 1.5, var=2.0, n=20, count_like=True, stat_col="stl")
    assert fam == "poisson"
    assert 0.5 < p2 < 0.8


def test_points_and_combos_never_poisson():
    assert choose_family(10.0, 14.0, n=40, count_like=True, stat_col="pts") == "normal"
    assert choose_family(8.0, 20.0, n=40, count_like=True, stat_col="pra") == "normal"
    _, fam = p_over_line(10.0, 9.5, var=25.0, n=40, count_like=True, stat_col="pts")
    assert fam == "normal"


def test_integer_line_push_mass():
    p_over, p_under, p_push, _fam = line_probs(5.0, 5.0, var=8.0, n=30, count_like=True, stat_col="stl")
    assert p_push > 0.05
    assert abs(p_over + p_under + p_push - 1.0) < 1e-5
    half_o, half_u, half_p, _ = line_probs(5.0, 4.5, var=8.0, n=30, count_like=True, stat_col="stl")
    assert half_p == 0.0
    assert abs(half_o + half_u - 1.0) < 1e-5


def test_same_mu_prices_three_lines_consistently():
    mu = 24.0
    p_gob, _ = p_over_line(mu, 19.5, var=40.0, n=50, count_like=False, stat_col="pts")
    p_std, _ = p_over_line(mu, 24.5, var=40.0, n=50, count_like=False, stat_col="pts")
    p_dem, _ = p_over_line(mu, 29.5, var=40.0, n=50, count_like=False, stat_col="pts")
    assert p_gob > p_std > p_dem


def test_mean_shrinkage_not_clipped_like_prob():
    shrunk = shrink_toward_baseline(20.0, 150.0, 10.0, 150.0)
    assert abs(shrunk - 15.0) < 1e-9
    none = shrink_toward_baseline(20.0, 0.0, 10.0, 150.0)
    assert abs(none - 10.0) < 1e-9
    deep = shrink_toward_baseline(20.0, 10_000.0, 10.0, 150.0)
    assert abs(deep - 20.0) < 0.3
    assert shrink_prob_toward_baseline(20.0, 150.0, 0.5, 150.0) == 1.0


def test_thin_player_hold_and_per_stat_k():
    rec = project_player_stat(
        np.array([30.0, 28.0, 32.0]),
        24.5,
        baseline=18.0,
        count_like=False,
        shrink_k=150.0,
        base_rate_n=5.0,
        stat_col="pts",
    )
    assert rec.n == 3
    assert rec.hold is True
    assert rec.mu_shrunk < rec.mu_raw
    assert abs(rec.mu_shrunk - (3 / 153 * rec.mu_raw + 150 / 153 * 18.0)) < 1e-6
    assert k_mean_for("pts") == K_MEAN_DEFAULT["pts"]
    assert k_mean_for("stl") > k_mean_for("reb")
    assert k_mean_for("minutes") < k_mean_for("pts")


def test_minutes_drive_mu():
    rec = project_player_stat(
        np.array([10.0] * 8),
        14.5,
        baseline=12.0,
        minutes=np.array([20.0] * 8),
        baseline_rate=0.4,
        baseline_rate_var=0.01,
        count_like=False,
        shrink_k=22.0,
        k_var=55.0,
        base_rate_n=50.0,
        stat_col="pts",
    )
    assert rec.hold is False
    assert abs(rec.proj_minutes - 20.0) < 1e-9
    # w = 8/30; rate_hat = 0.5*(8/30)+0.4*(22/30)
    w = 8.0 / 30.0
    rate_hat = w * 0.5 + (1.0 - w) * 0.4
    assert abs(rec.mu_rate - rate_hat) < 1e-6
    assert abs(rec.mu_shrunk - rate_hat * 20.0) < 1e-6
    assert rec.family == "normal"


def test_last_season_seed_avoids_opening_hold():
    prior = np.full(20, 0.5)
    rec = project_player_stat(
        np.array([12.0, 11.0, 13.0]),
        14.5,
        baseline=12.0,
        minutes=np.array([24.0, 24.0, 24.0]),
        baseline_rate=0.45,
        prior_rates=prior,
        prior_minutes=np.full(20, 24.0),
        shrink_k=22.0,
        base_rate_n=40.0,
        stat_col="pts",
    )
    assert rec.n_current == 3
    assert rec.n_prior == 10  # 20 * 0.5
    assert rec.n == 13
    assert rec.hold is False


def test_as_of_position_not_end_of_season():
    df = pd.DataFrame(
        {
            "game_date": ["2026-01-10", "2026-03-01"],
            "player": ["Test Guy", "Test Guy"],
            "position": ["PG", "C"],
            "espn_athlete_id": ["1", "1"],
            "minutes": [30.0, 30.0],
            "pts": [20.0, 20.0],
            "reb": [4.0, 10.0],
            "ast": [8.0, 2.0],
            "stl": [1.0, 1.0],
            "blk": [0.0, 2.0],
            "tov": [2.0, 2.0],
            "fg3m": [2.0, 1.0],
            "pra": [32.0, 32.0],
            "pr": [24.0, 30.0],
            "pa": [28.0, 22.0],
            "ra": [12.0, 12.0],
        }
    )
    idx = NbaBoxIndex.from_frame(df)
    assert pos_group(idx.position_as_of(espn_id="1", before_date="2026-02-01")) == "G"
    assert pos_group(idx.position_as_of(espn_id="1", before_date="2026-03-15")) == "C"
    assert nba_season_start("2026-02-19") == "2025-10-15"


def test_breakeven_is_n_correct_not_first_place():
    std = pick_type_breakeven("Standard")
    gob = pick_type_breakeven("Goblin")
    assert abs(std - 6.0 ** (-1.0 / 3.0)) < 1e-9
    assert abs(gob - 2.5 ** (-1.0 / 3.0)) < 1e-9
    assert gob > std


def test_expanding_marginals_no_future_leak():
    df = pd.DataFrame(
        {
            "sport": ["NBA", "NBA", "NBA"],
            "prop": ["Points", "Points", "Points"],
            "direction": ["OVER", "OVER", "OVER"],
            "line": [24.5, 24.5, 24.5],
            "file_date": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "result_binary": [1, 0, 1],
        }
    )
    as_of = compute_expanding_marginals_lookup(df, as_of="2026-01-03")
    key = cell_key("NBA", "Points", "OVER", 24.5)
    assert as_of[key]["n"] == 2.0
    assert as_of[key]["hits"] == 1.0
    assert abs(as_of[key]["rate"] - 0.5) < 1e-9
    rate, n = lookup_base_rate(as_of, sport="NBA", stat="Points", direction="OVER", line=24.5)
    assert n == 2.0
    assert abs(rate - 0.5) < 1e-9
    empty = compute_expanding_marginals_lookup(df, as_of="2026-01-01")
    assert key not in empty


def test_hierarchical_cell_hold_only_when_coarse_thin():
    from utils.expanding_marginals import lookup_base_rate_hierarchical

    rows = []
    for i in range(25):
        rows.append(
            {
                "sport": "NBA",
                "prop": "Points",
                "direction": "OVER",
                "line": 24.5,
                "file_date": "2026-01-01",
                "result_binary": 1 if i < 15 else 0,
            }
        )
    for i in range(3):
        rows.append(
            {
                "sport": "NBA",
                "prop": "Points",
                "direction": "OVER",
                "line": 3.5,
                "file_date": "2026-01-01",
                "result_binary": 1,
            }
        )
    df = pd.DataFrame(rows)
    lookup = compute_expanding_marginals_lookup(df, as_of="2026-01-02")
    fat = lookup_base_rate_hierarchical(lookup, sport="NBA", stat="Points", direction="OVER", line=24.5)
    thin = lookup_base_rate_hierarchical(lookup, sport="NBA", stat="Points", direction="OVER", line=3.5)
    assert fat["hold_cell"] is False
    assert fat["hold_legacy"] is False
    assert thin["hold_legacy"] is True
    assert thin["hold_cell"] is False
    assert thin["level"] in ("mid", "coarse")
    assert thin["rate"] < 1.0  # shrunk off the 3/3 sample toward the 24.5 cell mix


def test_isotonic_walk_forward_no_future_leak():
    from utils.proj_isotonic import calibrate_walk_forward

    rows = [{"file_date": "2026-01-01", "prop": "Points", "direction": "OVER", "proj_p_play": 0.9, "_y": 0.0} for _ in range(12)]
    rows += [{"file_date": "2026-01-02", "prop": "Points", "direction": "OVER", "proj_p_play": 0.9, "_y": 0.0} for _ in range(12)]
    df = pd.DataFrame(rows)
    cal = calibrate_walk_forward(df, min_n=8)
    assert abs(float(cal.iloc[0]) - 0.9) < 1e-9
    assert float(cal.iloc[12]) < 0.5


def test_integer_line_is_push_not_miss():
    sys.path.insert(0, str(ROOT / "scripts"))
    from grading.leg_grade_utils import compare_leg_to_line

    r, margin, _ = compare_leg_to_line(5.0, 5.0, "OVER")
    assert r == "PUSH"
    assert margin == 0.0
    r2, _, _ = compare_leg_to_line(6.0, 5.0, "OVER")
    assert r2 == "HIT"
    r3, _, _ = compare_leg_to_line(5.0, 5.0, "UNDER")
    assert r3 == "PUSH"

