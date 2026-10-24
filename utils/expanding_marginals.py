"""Walk-forward empirical hit rates by sport × stat × direction × line bucket.

This is the base-rate layer: P(hit) before any player projection. Lookups are
strictly as-of — grades on date D never enter the prior for D.
"""
from __future__ import annotations

import math
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from graded_line_quality_features import line_bucket  # noqa: E402

CellKey = tuple[str, str, str, str]
MID_BUCKET = "*"
COARSE_DIR = "*"
CELL_SHRINK_K = 20.0
MIN_CELL_N = 20.0


def _fold_stat(val: object) -> str:
    s = str(val or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    aliases = {
        "pts": "points",
        "reb": "rebounds",
        "ast": "assists",
        "stl": "steals",
        "blk": "blocks",
        "threepointsmade": "threes",
        "3ptmade": "threes",
        "fg3m": "threes",
        "3pm": "threes",
        "ptsrebsasts": "pra",
        "prareboundsassists": "pra",
    }
    return aliases.get(s, s)


def _fold_dir(val: object) -> str:
    d = str(val or "").strip().upper()
    if d in ("U", "UNDER"):
        return "UNDER"
    return "OVER"


def _fold_sport(val: object) -> str:
    return str(val or "").strip().upper()


def cell_key(sport: object, stat: object, direction: object, line: object) -> CellKey:
    return (
        _fold_sport(sport),
        _fold_stat(stat),
        _fold_dir(direction),
        str(line_bucket(line)),
    )


def mid_key(sport: object, stat: object, direction: object) -> CellKey:
    return (_fold_sport(sport), _fold_stat(stat), _fold_dir(direction), MID_BUCKET)


def coarse_key(sport: object, stat: object) -> CellKey:
    return (_fold_sport(sport), _fold_stat(stat), COARSE_DIR, MID_BUCKET)


def _hierarchy_keys(key: CellKey) -> tuple[CellKey, CellKey, CellKey]:
    sport, stat, direction, _bucket = key
    return key, (sport, stat, direction, MID_BUCKET), (sport, stat, COARSE_DIR, MID_BUCKET)


def _row_cell(row: Any) -> CellKey:
    getter = row.get if hasattr(row, "get") else lambda k, d=None: getattr(row, k, d)
    stat = (
        getter("prop")
        or getter("prop_type")
        or getter("stat_type")
        or getter("stat_norm")
        or ""
    )
    direction = getter("direction") or getter("bet_direction") or "OVER"
    line = getter("line") if getter("line") is not None else getter("line_score")
    return cell_key(getter("sport"), stat, direction, line)


def _row_date(row: Any) -> str:
    getter = row.get if hasattr(row, "get") else lambda k, d=None: getattr(row, k, d)
    for col in ("file_date", "game_date", "graded_date", "date", "slate_date"):
        raw = getter(col)
        if raw is None:
            continue
        s = str(raw).strip()[:10]
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return s
    return ""


def _row_hit(row: Any) -> float | None:
    getter = row.get if hasattr(row, "get") else lambda k, d=None: getattr(row, k, d)
    rb = getter("result_binary")
    try:
        if rb is not None and str(rb).strip() not in ("", "nan"):
            v = float(rb)
            if v in (0.0, 1.0):
                return v
    except (TypeError, ValueError):
        pass
    hit = getter("hit")
    try:
        if hit is not None and str(hit).strip() not in ("", "nan"):
            v = float(hit)
            if v in (0.0, 1.0):
                return v
    except (TypeError, ValueError):
        pass
    res = str(getter("result") or "").strip().upper()
    if res in ("HIT", "WIN", "WON"):
        return 1.0
    if res in ("MISS", "LOSS", "LOSE"):
        return 0.0
    return None


def compute_expanding_marginals_lookup(
    graded: pd.DataFrame | Iterable[Any],
    *,
    as_of: str | None = None,
    min_n: int = 1,
) -> dict[CellKey, dict[str, float]]:
    """Empirical P(hit) using only grades strictly before ``as_of`` (YYYY-MM-DD).

    If ``as_of`` is None, uses all rows (full-sample lookup; not for scoring a live slate).
    """
    if isinstance(graded, pd.DataFrame):
        rows: Iterable[Any] = (r for _, r in graded.iterrows())
    else:
        rows = graded

    hits: dict[CellKey, float] = defaultdict(float)
    n: dict[CellKey, float] = defaultdict(float)
    cutoff = str(as_of or "").strip()[:10]
    for row in rows:
        d = _row_date(row)
        if cutoff and (not d or d >= cutoff):
            continue
        y = _row_hit(row)
        if y is None:
            continue
        key = _row_cell(row)
        for k in _hierarchy_keys(key):
            n[k] += 1.0
            hits[k] += y

    out: dict[CellKey, dict[str, float]] = {}
    for key, nn in n.items():
        if nn < min_n:
            continue
        out[key] = {"n": nn, "hits": hits[key], "rate": hits[key] / nn}
    return out


def walk_forward_lookups_by_date(graded: pd.DataFrame) -> dict[str, dict[CellKey, dict[str, float]]]:
    """One expanding snapshot per file_date: grades strictly before that date."""
    if graded is None or graded.empty:
        return {}
    keys: list[CellKey] = []
    dates: list[str] = []
    ys: list[float] = []
    for _, row in graded.iterrows():
        d = _row_date(row)
        y = _row_hit(row)
        if not d or y is None:
            continue
        keys.append(_row_cell(row))
        dates.append(d)
        ys.append(y)
    if not dates:
        return {}
    work = pd.DataFrame({"_d": dates, "_y": ys, "_key": keys}).sort_values("_d")
    hits: dict[CellKey, float] = defaultdict(float)
    n: dict[CellKey, float] = defaultdict(float)
    out: dict[str, dict[CellKey, dict[str, float]]] = {}
    for d, g in work.groupby("_d", sort=True):
        snap: dict[CellKey, dict[str, float]] = {}
        for key, nn in n.items():
            if nn < 1:
                continue
            snap[key] = {"n": nn, "hits": hits[key], "rate": hits[key] / nn}
        out[str(d)[:10]] = snap
        for key, y in zip(g["_key"].tolist(), g["_y"].tolist()):
            for k in _hierarchy_keys(key):
                n[k] += 1.0
                hits[k] += float(y)
    return out


def lookup_base_rate(
    lookup: dict[CellKey, dict[str, float]],
    *,
    sport: object,
    stat: object,
    direction: object,
    line: object,
    default: float = 0.5,
) -> tuple[float, float]:
    """Return (rate, n). Missing cells fall back to (default, 0)."""
    rec = lookup.get(cell_key(sport, stat, direction, line))
    if not rec:
        return float(default), 0.0
    return float(rec["rate"]), float(rec["n"])


def _rec(lookup: dict[CellKey, dict[str, float]], key: CellKey) -> tuple[float, float]:
    rec = lookup.get(key)
    if not rec:
        return float("nan"), 0.0
    return float(rec["rate"]), float(rec["n"])


def _shrink_rate(child: float, child_n: float, parent: float, k: float) -> float:
    if child_n <= 0 or not math.isfinite(child):
        return parent if math.isfinite(parent) else 0.5
    if not math.isfinite(parent):
        return child
    k = max(float(k), 0.0)
    if k <= 0:
        return child
    w = child_n / (child_n + k)
    return w * child + (1.0 - w) * parent


def lookup_base_rate_hierarchical(
    lookup: dict[CellKey, dict[str, float]],
    *,
    sport: object,
    stat: object,
    direction: object,
    line: object,
    default: float = 0.5,
    shrink_k: float = CELL_SHRINK_K,
    min_n: float = MIN_CELL_N,
) -> dict[str, float | str | bool]:
    """Fine line-bucket shrunk toward stat×dir, then stat. Hold only if coarse is thin."""
    fine_k = cell_key(sport, stat, direction, line)
    m_k = mid_key(sport, stat, direction)
    c_k = coarse_key(sport, stat)
    fine_rate, fine_n = _rec(lookup, fine_k)
    mid_rate, mid_n = _rec(lookup, m_k)
    coarse_rate, coarse_n = _rec(lookup, c_k)

    hold_legacy = fine_n < float(min_n)
    hold_coarse = coarse_n < float(min_n)

    if fine_n >= float(min_n) and math.isfinite(fine_rate):
        rate, level = fine_rate, "fine"
    elif mid_n >= float(min_n) and math.isfinite(mid_rate):
        rate = _shrink_rate(fine_rate, fine_n, mid_rate, shrink_k)
        level = "mid"
    elif coarse_n >= float(min_n) and math.isfinite(coarse_rate):
        parent = _shrink_rate(mid_rate, mid_n, coarse_rate, shrink_k)
        rate = _shrink_rate(fine_rate, fine_n, parent, shrink_k)
        level = "coarse"
    else:
        parent = coarse_rate if math.isfinite(coarse_rate) else (
            mid_rate if math.isfinite(mid_rate) else default
        )
        rate = _shrink_rate(fine_rate, fine_n, parent, shrink_k)
        if not math.isfinite(rate):
            rate = float(default)
        level = "thin"

    return {
        "rate": float(rate),
        "n_fine": float(fine_n),
        "n_mid": float(mid_n),
        "n_coarse": float(coarse_n),
        "level": level,
        "hold_legacy": bool(hold_legacy),
        "hold_cell": bool(hold_coarse),
    }


def attach_hierarchical_base_rate(
    df: pd.DataFrame,
    lookup: dict[CellKey, dict[str, float]],
    *,
    default: float = 0.5,
    shrink_k: float = CELL_SHRINK_K,
    min_n: float = MIN_CELL_N,
) -> pd.DataFrame:
    out = df.copy()
    rates: list[float] = []
    ns: list[float] = []
    n_coarse: list[float] = []
    levels: list[str] = []
    hold_legacy: list[float] = []
    hold_cell: list[float] = []
    for _, row in out.iterrows():
        rec = lookup_base_rate_hierarchical(
            lookup,
            sport=row.get("sport", "NBA"),
            stat=row.get("prop") or row.get("prop_type") or row.get("stat_type"),
            direction=row.get("direction") or row.get("bet_direction"),
            line=row.get("line") if row.get("line") is not None else row.get("line_score"),
            default=default,
            shrink_k=shrink_k,
            min_n=min_n,
        )
        rates.append(float(rec["rate"]))
        ns.append(float(rec["n_fine"]))
        n_coarse.append(float(rec["n_coarse"]))
        levels.append(str(rec["level"]))
        hold_legacy.append(1.0 if rec["hold_legacy"] else 0.0)
        hold_cell.append(1.0 if rec["hold_cell"] else 0.0)
    out["base_rate"] = rates
    out["base_rate_n"] = ns
    out["base_rate_n_coarse"] = n_coarse
    out["base_rate_level"] = levels
    out["proj_hold_legacy_cell"] = hold_legacy
    out["proj_hold_cell"] = hold_cell
    return out


def attach_expanding_base_rate(
    df: pd.DataFrame,
    lookup: dict[CellKey, dict[str, float]],
    *,
    default: float = 0.5,
) -> pd.DataFrame:
    out = df.copy()
    rates: list[float] = []
    ns: list[float] = []
    for _, row in out.iterrows():
        rate, n = lookup_base_rate(
            lookup,
            sport=row.get("sport", "NBA"),
            stat=row.get("prop") or row.get("prop_type") or row.get("stat_type"),
            direction=row.get("direction") or row.get("bet_direction"),
            line=row.get("line") if row.get("line") is not None else row.get("line_score"),
            default=default,
        )
        rates.append(rate)
        ns.append(n)
    out["base_rate"] = rates
    out["base_rate_n"] = ns
    return out
