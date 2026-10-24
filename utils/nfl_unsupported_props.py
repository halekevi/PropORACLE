"""NFL/CFB period + first-N / half-quarter props that lack L5 scaffolding.

Grading can settle these after the game via play-by-play
(``scripts/fetch_football_actuals.py``), but the boxscore L5 engine
(``step5b_attach_boxscore_stats.prop_value``) fuzzy-matches them onto
full-game PASS_YDS / RUSH_YDS / RUSH_ATT / etc. That fabricates hit rates.

Mark at step2 (``unsupported_prop=1``) and skip in step5 — same pattern as
Tennis 1st-set props. Ticket keep-gates already exclude them; this stops
silent fabricated L5 before ranking.
"""
from __future__ import annotations

import re
from typing import Any

# Explicit norms from slate_grader / fetch_football_actuals (underscore form).
PERIOD_SPLIT_PROP_NORMS: frozenset[str] = frozenset(
    {
        "halves_with_100_pass_yards",
        "halves_with_25_rush_yards",
        "halves_with_25_receiving_yards",
        "quarters_with_5_rush_yards",
        "quarters_with_5_receiving_yards",
        "quarters_with_1_reception",
        "quarters_with_25_pass_yards",
        "quarters_with_10_rush_yards",
        "yards_on_first_rush_attempt",
        "yards_on_first_reception",
        "rush_yards_in_first_5_attempts",
        "receiving_yards_in_first_2_receptions",
        "completions_in_first_10_pass_attempts",
    }
)

_PERIOD_SPLIT_RE = re.compile(
    r"("
    r"halves?_with_|"
    r"quarters?_with_|"
    r"yards_on_first_|"
    r"in_first_\d|"
    r"first_\d+_|"
    r"completions_in_first|"
    r"receiving_yards_in_first|"
    r"rush_yards_in_first"
    r")",
    re.I,
)


def norm_football_prop(raw: object) -> str:
    s = str(raw or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def is_football_period_split_prop(raw: object) -> bool:
    """True when L5 cannot be computed from full-game boxscores."""
    p = norm_football_prop(raw)
    if not p:
        return False
    if p in PERIOD_SPLIT_PROP_NORMS:
        return True
    return bool(_PERIOD_SPLIT_RE.search(p))


def unsupported_reason_for_prop(raw: object) -> str:
    if is_football_period_split_prop(raw):
        return "period_or_first_n_prop_no_box_l5"
    return ""


def mark_football_unsupported_props(
    df: Any,
    *,
    prop_col: str = "prop_type_normalized",
) -> Any:
    """Set unsupported_prop / unsupported_reason on a step2+ frame."""
    import pandas as pd

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    out = df.copy()
    col = prop_col if prop_col in out.columns else (
        "prop_norm" if "prop_norm" in out.columns else (
            "prop_type" if "prop_type" in out.columns else ""
        )
    )
    if not col:
        out["unsupported_prop"] = 0
        out["unsupported_reason"] = ""
        return out
    reasons = out[col].map(unsupported_reason_for_prop)
    # Preserve prior unsupported=1 from other sources.
    prior = (
        pd.to_numeric(out["unsupported_prop"], errors="coerce").fillna(0).astype(int)
        if "unsupported_prop" in out.columns
        else pd.Series(0, index=out.index)
    )
    flagged = reasons.astype(str).str.len().gt(0)
    out["unsupported_prop"] = prior.where(prior.eq(1), flagged.astype(int))
    if "unsupported_reason" in out.columns:
        blank = out["unsupported_reason"].isna() | out["unsupported_reason"].astype(str).str.strip().isin(
            ["", "nan", "None"]
        )
        out.loc[flagged & blank, "unsupported_reason"] = reasons[flagged & blank]
    else:
        out["unsupported_reason"] = ""
        out.loc[flagged, "unsupported_reason"] = reasons[flagged]
    return out
