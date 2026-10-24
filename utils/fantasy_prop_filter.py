"""Fantasy-score markets: listed with ticketable=false, never silently dropped.

Tickets and EV still skip fantasy. The full board listing keeps every row and
stamps ``ticketable=0`` (or False) so reconcile / ranker can show the prop.
"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

# Columns that may carry a human-readable prop label (checked in order).
_PROP_LABEL_COLUMNS: tuple[str, ...] = (
    "prop_type",
    "prop",
    "prop_name",
    "prop_type_norm",
    "Prop",
    "Prop Type",
    "stat_type",
    "prop_norm",
)

TICKETABLE_COL = "ticketable"


def _norm_label(text: object) -> str:
    s = str(text or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def is_fantasy_prop_label(text: object) -> bool:
    """True when the prop label is a fantasy-score market (not a real stat)."""
    s = _norm_label(text)
    if not s:
        return False
    return "fantasy" in s


def fantasy_prop_mask(df: pd.DataFrame) -> pd.Series:
    """Row mask: True where any known prop column is a fantasy market."""
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    mask = pd.Series(False, index=df.index)
    for col in _PROP_LABEL_COLUMNS:
        if col in df.columns:
            mask |= df[col].map(is_fantasy_prop_label)
    return mask


def mark_fantasy_ticketable(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Keep all rows; set ticketable=False on fantasy markets. Returns (df, n_flagged)."""
    if df is None or df.empty:
        return df, 0
    out = df.copy()
    mask = fantasy_prop_mask(out)
    n = int(mask.sum())
    if TICKETABLE_COL not in out.columns:
        out[TICKETABLE_COL] = True
    else:
        # Preserve prior False; only force fantasy off.
        out[TICKETABLE_COL] = out[TICKETABLE_COL].map(
            lambda v: bool(v) if str(v).strip().lower() not in {"0", "false", "no"} else False
        )
    if n:
        out.loc[mask, TICKETABLE_COL] = False
    return out, n


def drop_fantasy_props(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Deprecated drop path — now stamps ticketable=false and keeps rows.

    Returns (df_with_flag, n_fantasy_flagged) so callers that printed
    "Dropped N fantasy" still see a count without losing the props.
    """
    return mark_fantasy_ticketable(df)


def drop_fantasy_rows(rows: Iterable[dict]) -> tuple[list[dict], int]:
    """Keep fantasy rows; stamp ticketable=False. Returns (rows, n_flagged)."""
    out: list[dict] = []
    flagged = 0
    for row in rows:
        label = ""
        for key in ("stat_type", "prop_type", "prop", "prop_name"):
            if key in row and str(row.get(key) or "").strip():
                label = row[key]
                break
        rec = dict(row)
        if is_fantasy_prop_label(label):
            rec[TICKETABLE_COL] = False
            flagged += 1
        else:
            rec.setdefault(TICKETABLE_COL, True)
        out.append(rec)
    return out, flagged
