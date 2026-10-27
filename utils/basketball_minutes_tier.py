"""Basketball minutes_tier from real MPG (NBA bands).

Shared by NBA / WNBA / CBB / WCBB pipeline steps and backtests.

Thresholds match Sports/NBA/scripts/step6_team_role_context.tier_minutes:
  <= 20 → LOW
  <= 30 → MEDIUM
  >  30 → HIGH

Prefer real minutes columns only — never prop-stat averages
(stat_last5_avg / pts / etc.).
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

# Column preference order for real minutes-per-game.
MPG_COLUMNS: tuple[str, ...] = (
    "min_last5_avg",
    "min_per_game",
    "min_season_avg",
    "avg_minutes",
)

# Do not treat these as minutes (common false positives in graded / step6 history).
_NON_MPG_COLUMNS: frozenset[str] = frozenset(
    {
        "stat_last5_avg",
        "stat_last10_avg",
        "stat_season_avg",
        "min_player_avg",  # historically conflated with prop avg on WNBA
        "pts_player_avg",
        "fga_player_avg",
        "line",
    }
)


def tier_minutes_from_mpg(x: object) -> str:
    """Map a single MPG value to LOW / MEDIUM / HIGH / UNKNOWN (NBA bands)."""
    v = pd.to_numeric(x, errors="coerce")
    if pd.isna(v):
        return "UNKNOWN"
    fv = float(v)
    if fv <= 20:
        return "LOW"
    if fv <= 30:
        return "MEDIUM"
    return "HIGH"


def resolve_mpg_series(
    df: pd.DataFrame,
    *,
    columns: Iterable[str] | None = None,
) -> pd.Series:
    """First finite value across preferred real-minutes columns (row-wise)."""
    cols = tuple(columns) if columns is not None else MPG_COLUMNS
    out = pd.Series(np.nan, index=df.index, dtype=float)
    for name in cols:
        if name in _NON_MPG_COLUMNS:
            continue
        if name not in df.columns:
            continue
        s = pd.to_numeric(df[name], errors="coerce")
        out = out.where(out.notna(), s)
    return out


def assign_minutes_tier(
    df: pd.DataFrame,
    *,
    columns: Iterable[str] | None = None,
    out_col: str = "minutes_tier",
    mpg_col: str | None = "min_player_avg",
) -> pd.DataFrame:
    """Set minutes_tier (and optionally min_player_avg) from real MPG."""
    out = df.copy()
    mpg = resolve_mpg_series(out, columns=columns)
    out[out_col] = mpg.apply(tier_minutes_from_mpg)
    if mpg_col:
        out[mpg_col] = mpg
    return out
