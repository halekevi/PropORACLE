"""Directional L5 average vs line gap — the ticket pack order.

OVER gap = L5 avg − line. UNDER gap = line − L5 avg. Bigger is better on
both sides. Raw stat units: a 40-yard pass gap ranks ahead of a 3-point
gap; there is no per-prop scaling.
"""
from __future__ import annotations

import math
from typing import Any

_L5_AVG_KEYS = (
    "avg_l5",
    "l5_avg",
    "stat_last5_avg",
    "last5_avg",
    "avg_L5",
    "Last 5 Avg",
    "L5 Avg",
)


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _side(r: dict[str, Any]) -> str:
    return str(r.get("side") or r.get("direction") or "").strip().upper()


def l5_line_gap(r: dict[str, Any]) -> float | None:
    line = _num(r.get("line"))
    avg = None
    for k in _L5_AVG_KEYS:
        avg = _num(r.get(k))
        if avg is not None:
            break
    if avg is not None and line is not None:
        raw = avg - line
    else:
        raw = _num(r.get("dist_l5"))
        if raw is None:
            return None
    return -raw if _side(r) == "UNDER" else raw


def l5_gap_sort_key(r: dict[str, Any]) -> tuple:
    """Biggest gap first; rows without an L5 average go last. Stable on ties."""
    gap = l5_line_gap(r)
    return (gap is None, -(gap or 0.0))
