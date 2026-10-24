"""Play-side cushion between a last-10 window and the posted line.

Overs and Goblins: (L10 average − line) / L10 sample std.
Standard Unders flip that: (line − L10 average) / L10 sample std.
A bigger number means the recent games sit further on the play side of the line.

This is a discovery feature. It does not admit a ticket.
"""
from __future__ import annotations

import math
from typing import Any, Sequence

_DNP = {"dnp", "no_actual", "minutes_unknown"}
_RETIRE = {"readings_differ", "walkover_chart_silent"}


def _finite(games: Sequence[object]) -> list[float]:
    out: list[float] = []
    for raw in games:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isnan(value):
            continue
        out.append(value)
    return out


def l5_clears(games: Sequence[object], line: float, side: str) -> bool:
    """True when the five most recent games all finish on the play side of the line.

    Games are newest first. A push (equal to the line) does not clear.
    """
    vals = _finite(list(games)[:5])
    if len(vals) < 5:
        return False
    side_u = str(side or "").strip().upper()
    if side_u == "OVER":
        return all(v > line for v in vals)
    if side_u == "UNDER":
        return all(v < line for v in vals)
    return False


def _window(games: Sequence[object], *, min_n: int = 8) -> list[float] | None:
    vals = _finite(list(games)[:10])
    if len(vals) < min_n:
        return None
    return vals


def _mean_sd(vals: Sequence[float]) -> tuple[float, float] | None:
    n = len(vals)
    if n < 2:
        return None
    avg = sum(vals) / n
    var = sum((v - avg) ** 2 for v in vals) / (n - 1)
    sd = math.sqrt(var)
    if sd <= 0:
        return None
    return avg, sd


def play_side_gap(stat: float, line: float, side: str) -> float | None:
    side_u = str(side or "").strip().upper()
    if side_u == "OVER":
        return stat - line
    if side_u == "UNDER":
        return line - stat
    return None


def cushion_z(games: Sequence[object], line: float, side: str) -> float | None:
    """(play-side L10 average gap) / L10 sample standard deviation."""
    vals = _window(games)
    if vals is None:
        return None
    moments = _mean_sd(vals)
    if moments is None:
        return None
    avg, sd = moments
    gap = play_side_gap(avg, line, side)
    if gap is None:
        return None
    return gap / sd


def median_gap(games: Sequence[object], line: float, side: str) -> float | None:
    vals = _window(games)
    if vals is None:
        return None
    ordered = sorted(vals)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        med = ordered[mid]
    else:
        med = (ordered[mid - 1] + ordered[mid]) / 2
    return play_side_gap(med, line, side)


def clear_count(games: Sequence[object], line: float, side: str, margin: float) -> int | None:
    """How many of the last-10 games clear the line by at least ``margin``."""
    vals = _window(games)
    if vals is None or margin < 0:
        return None
    side_u = str(side or "").strip().upper()
    if side_u == "OVER":
        return sum(1 for v in vals if v >= line + margin)
    if side_u == "UNDER":
        return sum(1 for v in vals if v <= line - margin)
    return None


def tercile_edges(values: Sequence[float]) -> tuple[float, float] | None:
    xs = sorted(float(v) for v in values)
    if len(xs) < 9:
        return None

    def at(p: float) -> float:
        i = (len(xs) - 1) * p
        lo = int(i)
        hi = min(lo + 1, len(xs) - 1)
        w = i - lo
        return xs[lo] * (1 - w) + xs[hi] * w

    low, high = at(1 / 3), at(2 / 3)
    if high < low:
        return None
    return low, high


def assign_tercile(value: float, edges: tuple[float, float]) -> str:
    low, high = edges
    if value < low:
        return "low"
    if value < high:
        return "mid"
    return "high"


def row_excluded(row: dict[str, Any]) -> str:
    """Why a graded row stays out of the cushion sample. Empty string means it stays."""
    status = str(
        row.get("minutes_status") or row.get("actual_status") or row.get("result_reason") or ""
    ).strip().lower()
    if status in _DNP:
        return "dnp"
    reason = str(row.get("void_reason") or row.get("dnp_reason") or "").lower()
    if "dnp" in reason or "minutes_unknown" in reason or "no_actual" in reason:
        return "dnp"
    reading = str(row.get("retirement_reading") or "")
    if reading in _RETIRE:
        return "retirement"
    if "walkover" in reason or "retir" in reason:
        return "retirement"
    return ""


def current_game_map(values: Sequence[object]) -> dict[str, object]:
    """current_stat_g1..g10, newest first. Empty slots stay blank. Not the prior-season pad."""
    vals = list(values)[:10]
    out: dict[str, object] = {}
    for i in range(1, 11):
        raw = vals[i - 1] if i - 1 < len(vals) else None
        if raw is None or raw == "":
            out[f"current_stat_g{i}"] = ""
        else:
            out[f"current_stat_g{i}"] = raw
    return out


def rank_tiebreak(row: dict[str, Any]) -> float:
    """Ascending sort value. A larger play-side cushion sorts first. Missing sorts last.

    This orders Overs that already cleared the gate. It does not remove a leg,
    change a price, or change a floor. Standard Unders are left unsorted:
    that side has no gap gradient.
    """
    pick = str(row.get("pick_type") or "").lower()
    side = str(row.get("side") or row.get("direction") or row.get("over_under") or "").strip().upper()
    if side in {"U"}:
        side = "UNDER"
    if "standard" in pick and side == "UNDER":
        return math.inf
    raw = row.get("gap_z")
    if raw is None or raw == "":
        raw = row.get("cushion_z")
    if raw is None or raw == "":
        side = str(row.get("side") or row.get("direction") or row.get("over_under") or "")
        line = row.get("line")
        try:
            line_f = float(line)
        except (TypeError, ValueError):
            return math.inf
        games = []
        for i in range(1, 11):
            slot = row.get(f"current_stat_g{i}")
            if slot in (None, ""):
                slot = row.get(f"Current G{i}")
            games.append(slot)
        if not any(g not in (None, "") for g in games):
            return math.inf
        raw = cushion_z(games, line_f, side)
    try:
        z = float(raw)
    except (TypeError, ValueError):
        return math.inf
    if math.isnan(z):
        return math.inf
    return -z


def screen_status(
    *,
    holdout_n: int,
    top_n: int,
    rising: bool,
    wilson_lo: float | None,
    breakeven: float,
    beats_shuffle: bool | None,
    ceiling: bool,
    data_gap: str = "",
) -> str:
    """PASS only when the holdout screen clears. The caller still does not wire a floor."""
    if data_gap or holdout_n < 200 or top_n < 30 or ceiling:
        return "OPEN"
    if rising and beats_shuffle is True and wilson_lo is not None and wilson_lo > breakeven:
        return "PASS"
    return "FAIL"
