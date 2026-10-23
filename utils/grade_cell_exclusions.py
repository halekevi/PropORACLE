"""Graded cells that must not count as HIT/MISS, for the grader and for training labels.

Each cell is a (sport, prop) pair whose stored actuals are not real stat values:

* Tennis Aces / Double Faults / 1st Set Aces: every stored actual through 2026-09-30 is 0
  (missing serve stats were coerced to 0), so every Over graded MISS and every Under HIT.
* CFB / NFL Pass Attempts: ``fetch_football_actuals`` never emitted the stat and the
  football zero-fill invented 0 for it.
* NHL Faceoffs Won: the stored actual is a percentage but the PrizePicks line is a count,
  so the comparison is meaningless until the units are reconciled.

``exclusion_reason`` is used at grade time (slate_grader) and at read time
(``build_retrain_dataset``, ``train_prop_model_nhl``, ``regrade_report``), so the history can be
filtered without rewriting the graded JSONs.
"""
from __future__ import annotations

import re
from typing import Any, Mapping

import numpy as np
import pandas as pd

# Last slate date with known zero-filled tennis serve stats. Later dates carry real serve
# stats (or no row at all, which already grades VOID), so a real 0 is not excluded there.
TENNIS_SERVE_ZERO_THROUGH = "2026-09-30"

TENNIS_SERVE_PROPS = frozenset({"aces", "double faults", "1st set aces"})
FOOTBALL_PASS_ATT_PROPS = frozenset({"pass attempts", "passing attempts", "pass att"})
NHL_FACEOFF_PROPS = frozenset({"faceoffs won", "faceoff wins", "faceoffs"})

REASON_TENNIS_SERVE = "EXCLUDED_ZERO_FILLED_TENNIS_SERVE_STAT"
REASON_PASS_ATT = "EXCLUDED_ZERO_FILLED_PASS_ATTEMPTS"
REASON_FACEOFF = "EXCLUDED_NHL_FACEOFF_UNITS_MISMATCH"

# For the report / docs.
CELLS: tuple[dict[str, str], ...] = (
    {"name": "tennis_serve_stats", "sport": "Tennis", "props": "Aces, Double Faults, 1st Set Aces",
     "reason": REASON_TENNIS_SERVE},
    {"name": "football_pass_attempts", "sport": "CFB/NFL", "props": "Pass Attempts (actual == 0)",
     "reason": REASON_PASS_ATT},
    {"name": "nhl_faceoffs_won", "sport": "NHL", "props": "Faceoffs Won", "reason": REASON_FACEOFF},
)


def norm_prop(prop: Any) -> str:
    s = str(prop or "").strip().lower()
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"\s*\(combo\)\s*", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _num(v: Any) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return float("nan")
    return f


def _date10(v: Any) -> str:
    s = str(v or "").strip()
    return s[:10] if len(s) >= 10 and s[4:5] == "-" else ""


def _is_sport(sport: Any, *names: str) -> bool:
    s = str(sport or "").strip().upper()
    return s in {n.upper() for n in names}


def exclusion_reason(
    sport: Any,
    prop: Any,
    actual: Any = None,
    date: Any = None,
) -> str:
    """Return the exclusion reason, or '' when the leg is a normal gradable row."""
    p = norm_prop(prop)
    if not p:
        return ""
    a = _num(actual)
    if _is_sport(sport, "Tennis") and p in TENNIS_SERVE_PROPS:
        d = _date10(date)
        if d:
            return REASON_TENNIS_SERVE if d <= TENNIS_SERVE_ZERO_THROUGH else ""
        # No date on the row: fall back to the signature of the bug (0 or missing).
        return REASON_TENNIS_SERVE if (np.isnan(a) or a == 0.0) else ""
    if _is_sport(sport, "CFB", "NFL", "NFLP", "CFB1H") and p in FOOTBALL_PASS_ATT_PROPS:
        if a == 0.0:
            return REASON_PASS_ATT
        return ""
    if _is_sport(sport, "NHL") and p in NHL_FACEOFF_PROPS:
        return REASON_FACEOFF
    return ""


def _col(df: pd.DataFrame, *names: str) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([None] * len(df), index=df.index)


def exclusion_reasons(df: pd.DataFrame) -> pd.Series:
    """Vectorised ``exclusion_reason`` over a graded frame ('' where not excluded)."""
    if df is None or df.empty:
        return pd.Series([], dtype=object, index=getattr(df, "index", None))
    sport = _col(df, "sport", "Sport").astype(str).str.strip().str.upper()
    prop = _col(df, "prop", "prop_type", "Prop").map(norm_prop)
    actual = pd.to_numeric(_col(df, "actual_value", "actual", "Actual"), errors="coerce")
    date = _col(df, "_date", "file_date", "game_date", "date", "Game Date").map(_date10)

    out = pd.Series("", index=df.index, dtype=object)

    ten = sport.eq("TENNIS") & prop.isin(TENNIS_SERVE_PROPS)
    ten &= (date.ne("") & date.le(TENNIS_SERVE_ZERO_THROUGH)) | (date.eq("") & (actual.isna() | actual.eq(0)))
    out[ten] = REASON_TENNIS_SERVE

    fb = sport.isin(["CFB", "NFL", "NFLP", "CFB1H"]) & prop.isin(FOOTBALL_PASS_ATT_PROPS) & actual.eq(0)
    out[fb] = REASON_PASS_ATT

    nhl = sport.eq("NHL") & prop.isin(NHL_FACEOFF_PROPS)
    out[nhl] = REASON_FACEOFF
    return out


def drop_excluded(df: pd.DataFrame) -> pd.DataFrame:
    """Rows of ``df`` that are not in an excluded cell."""
    if df is None or df.empty:
        return df
    return df.loc[exclusion_reasons(df).eq("")].copy()


def row_reason(row: Mapping[str, Any], *, actual: Any = None, date: Any = None, sport: Any = None) -> str:
    """Convenience wrapper for dict/Series rows."""
    get = row.get if hasattr(row, "get") else (lambda k, d=None: d)
    return exclusion_reason(
        sport if sport is not None else (get("sport") or get("Sport")),
        get("prop") or get("prop_type") or get("Prop") or get("prop_norm"),
        actual if actual is not None else (get("actual_value") if get("actual_value") is not None else get("actual")),
        date if date is not None else (get("game_date") or get("Game Date") or get("_date")),
    )
