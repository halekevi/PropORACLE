"""Shared push-aware L5 counts for every sport.

A push (stat == line) is not a hit. ``l5_over`` / ``l5_under`` count only
decided wins; ``l5_push`` / ``l5_decided`` / ``thin_decided`` make the split
explicit. A clean 5/5 needs five decided hits in the play direction (no pushes
in the L5 window).

Used by ``fill_l5_from_stat_games`` (all step8 paths) and Golf windows.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Literal, Sequence

import numpy as np
import pandas as pd

THIN_DECIDED_MIN = 3


@dataclass(frozen=True)
class LineCountResult:
    over: int
    under: int
    push: int
    decided: int
    played: int
    hit_rate_decided_over: float | None
    hit_rate_decided_under: float | None
    thin_decided: bool

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def line_outcome(value: float | None, line: float) -> Literal["over", "under", "push"] | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if line is None or (isinstance(line, float) and np.isnan(line)):
        return None
    v = float(value)
    ln = float(line)
    if v > ln:
        return "over"
    if v < ln:
        return "under"
    return "push"


def count_vs_line(
    values: Sequence[object] | Iterable[object],
    line: float,
) -> LineCountResult:
    over = under = push = played = 0
    ln = float(line)
    for raw in values:
        v = pd.to_numeric(raw, errors="coerce")
        if pd.isna(v):
            continue
        played += 1
        outcome = line_outcome(float(v), ln)
        if outcome == "over":
            over += 1
        elif outcome == "under":
            under += 1
        else:
            push += 1
    decided = over + under
    return LineCountResult(
        over=over,
        under=under,
        push=push,
        decided=decided,
        played=played,
        hit_rate_decided_over=(over / decided) if decided else None,
        hit_rate_decided_under=(under / decided) if decided else None,
        thin_decided=decided < THIN_DECIDED_MIN,
    )


def counts_from_stat_g_row(
    row: pd.Series | dict,
    *,
    line: float,
    n: int = 5,
) -> LineCountResult:
    vals: list[object] = []
    for i in range(1, n + 1):
        key = f"stat_g{i}"
        if isinstance(row, pd.Series):
            if key in row.index:
                vals.append(row.get(key))
        elif key in row:
            vals.append(row[key])
    return count_vs_line(vals, line)


def is_clean_l5_five(hits: object, decided: object = None, push: object = None) -> bool:
    """True when the play direction has 5 decided hits (no L5 pushes)."""
    h = pd.to_numeric(hits, errors="coerce")
    if pd.isna(h) or float(h) < 5:
        return False
    d = pd.to_numeric(decided, errors="coerce")
    if pd.notna(d):
        return float(d) >= 5.0 and float(h) >= 5.0
    p = pd.to_numeric(push, errors="coerce")
    if pd.notna(p):
        return float(p) == 0.0 and float(h) >= 5.0
    return float(h) >= 5.0


def attach_l5_push_columns(
    df: pd.DataFrame,
    *,
    line_col: str = "line",
    n: int = 5,
    min_games: int = 1,
) -> pd.DataFrame:
    """Write l5_over/under/push/decided + thin_decided from ``stat_g1..gN``."""
    if df is None or df.empty:
        return df
    g_cols = [c for c in (f"stat_g{i}" for i in range(1, n + 1)) if c in df.columns]
    if not g_cols or line_col not in df.columns:
        return df

    out = df.copy()
    line = pd.to_numeric(out[line_col], errors="coerce")
    over_l: list[float] = []
    under_l: list[float] = []
    push_l: list[float] = []
    decided_l: list[float] = []
    thin_l: list[float] = []
    hr_o: list[float] = []
    hr_u: list[float] = []
    played_l: list[float] = []
    for idx in out.index:
        ln = line.at[idx]
        vals = [out.at[idx, c] for c in g_cols]
        if pd.isna(ln):
            over_l.append(np.nan)
            under_l.append(np.nan)
            push_l.append(np.nan)
            decided_l.append(np.nan)
            thin_l.append(np.nan)
            hr_o.append(np.nan)
            hr_u.append(np.nan)
            played_l.append(np.nan)
            continue
        c = count_vs_line(vals, float(ln))
        over_l.append(float(c.over))
        under_l.append(float(c.under))
        push_l.append(float(c.push))
        decided_l.append(float(c.decided))
        thin_l.append(1.0 if c.thin_decided else 0.0)
        hr_o.append(c.hit_rate_decided_over if c.hit_rate_decided_over is not None else np.nan)
        hr_u.append(c.hit_rate_decided_under if c.hit_rate_decided_under is not None else np.nan)
        played_l.append(float(c.played))

    played_s = pd.Series(played_l, index=out.index, dtype=float)
    has_hist = (played_s >= float(min_games)) & line.notna()
    if not bool(has_hist.any()):
        return out

    def _write(col: str, values: list[float]) -> None:
        # step7/8 often load with dtype=str; Arrow string columns reject float setitem.
        # Drop+replace so we never setitem floats into a pandas StringDtype column.
        ser = pd.Series(values, index=out.index, dtype=float)
        if col in out.columns:
            base = pd.to_numeric(out[col], errors="coerce").astype(float)
            out.drop(columns=[col], inplace=True)
        else:
            base = pd.Series(np.nan, index=out.index, dtype=float)
        base.loc[has_hist] = ser.loc[has_hist]
        out[col] = base

    _write("l5_over", over_l)
    _write("l5_under", under_l)
    _write("l5_push", push_l)
    _write("l5_decided", decided_l)
    _write("last5_over", over_l)
    _write("last5_under", under_l)
    _write("last5_push", push_l)
    _write("last5_decided", decided_l)
    _write("line_hits_over_5", over_l)
    _write("line_hits_under_5", under_l)
    _write("line_hits_push_5", push_l)
    _write("line_hits_decided_5", decided_l)
    _write("thin_decided", thin_l)
    _write("hit_rate_decided_over_5", hr_o)
    _write("hit_rate_decided_under_5", hr_u)
    _write("line_games_played_5", played_l)
    return out
