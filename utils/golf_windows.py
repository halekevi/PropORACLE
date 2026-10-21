"""Golf shared round windows + push-aware line counts.

Single helper for ``l5_all``, ``l5_course``, and ``skill_window`` so step4,
PP reconcile, and course-baseline projection cannot drift apart.

See ``Sports/Golf/GOLF_WINDOWS_RECONCILE_PROJECTION.md``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Iterable, Literal, Sequence

import numpy as np
import pandas as pd

WindowKind = Literal["l5_all", "l5_course", "skill_window"]

L5_ALL_N = 5
SKILL_WINDOW_N = 20
DEFAULT_COURSE_N = 5
THIN_DECIDED_MIN = 3

# Canonical column → accepted aliases (first present wins).
_COL_ALIASES: dict[str, tuple[str, ...]] = {
    "round_date": ("round_date", "tournament_date", "date"),
    "event_id": ("event_id", "espn_event_id", "tournament_id"),
    "course_id": ("course_id", "venue_id", "espn_course_id"),
    "round_num": ("round_num", "round"),
    "strokes": ("strokes",),
    "to_par": ("to_par",),
    "vs_field": ("vs_field",),
    "birdies": ("birdies",),
    "pars": ("pars",),
    "bogeys_plus": ("bogeys_plus", "bogeys_or_worse"),
    "player_key": ("player_key",),
}


@dataclass(frozen=True)
class LineCountResult:
    """Push-aware counts for one window vs a line."""

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


def _parse_date(val: object) -> date | None:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if not s or s.lower() in {"nan", "none", "nat"}:
        return None
    s = s[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()


def normalize_rounds(df: pd.DataFrame) -> pd.DataFrame:
    """Map alias columns onto the canonical round schema used by windows."""
    if df is None or df.empty:
        return pd.DataFrame(columns=list(_COL_ALIASES.keys()))
    out = df.copy()
    for canon, aliases in _COL_ALIASES.items():
        if canon in out.columns:
            continue
        for alt in aliases:
            if alt in out.columns:
                out[canon] = out[alt]
                break
        else:
            out[canon] = np.nan if canon not in {"course_id", "event_id", "player_key"} else ""
    out["round_date"] = out["round_date"].map(_parse_date)
    out["round_num"] = pd.to_numeric(out["round_num"], errors="coerce")
    out["course_id"] = out["course_id"].fillna("").astype(str).str.strip()
    out["event_id"] = out["event_id"].fillna("").astype(str).str.strip()
    for c in ("strokes", "to_par", "vs_field", "birdies", "pars", "bogeys_plus"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _as_of_date(as_of: date | datetime | str | None) -> date | None:
    if as_of is None:
        return None
    if isinstance(as_of, datetime):
        return as_of.date()
    if isinstance(as_of, date):
        return as_of
    return _parse_date(as_of)


def select_window(
    rounds: pd.DataFrame,
    *,
    kind: WindowKind,
    course_id: str | None = None,
    n_course: int = DEFAULT_COURSE_N,
    as_of: date | datetime | str | None = None,
) -> pd.DataFrame:
    """Return rounds for ``kind``, newest first.

    ``as_of`` is exclusive: only rounds with ``round_date`` strictly before
    that date are eligible (walk-forward guard).
    """
    work = normalize_rounds(rounds)
    if work.empty:
        return work.iloc[0:0].copy()

    cutoff = _as_of_date(as_of)
    if cutoff is not None:
        work = work[work["round_date"].notna() & (work["round_date"] < cutoff)].copy()

    work = work[work["round_date"].notna()].copy()
    if work.empty:
        return work

    if kind == "l5_course":
        cid = str(course_id or "").strip()
        if not cid:
            return work.iloc[0:0].copy()
        work = work[work["course_id"].astype(str) == cid].copy()
        n = max(0, int(n_course))
    elif kind == "skill_window":
        n = SKILL_WINDOW_N
    else:
        n = L5_ALL_N

    work = work.sort_values(
        ["round_date", "round_num"],
        ascending=[False, False],
        kind="mergesort",
    )
    if n <= 0:
        return work.iloc[0:0].copy()
    return work.head(n).copy()


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
    """Push-aware over/under/push/decided counts for a value sequence vs ``line``."""
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
    hr_o = (over / decided) if decided else None
    hr_u = (under / decided) if decided else None
    return LineCountResult(
        over=over,
        under=under,
        push=push,
        decided=decided,
        played=played,
        hit_rate_decided_over=hr_o,
        hit_rate_decided_under=hr_u,
        thin_decided=decided < THIN_DECIDED_MIN,
    )


def count_stat_window(
    rounds: pd.DataFrame,
    *,
    stat_col: str,
    line: float,
    kind: WindowKind,
    course_id: str | None = None,
    n_course: int = DEFAULT_COURSE_N,
    as_of: date | datetime | str | None = None,
) -> tuple[pd.DataFrame, LineCountResult]:
    """Select a window and count ``stat_col`` vs ``line``."""
    win = select_window(
        rounds,
        kind=kind,
        course_id=course_id,
        n_course=n_course,
        as_of=as_of,
    )
    if win.empty or stat_col not in win.columns:
        empty = count_vs_line([], line)
        return win, empty
    vals = pd.to_numeric(win[stat_col], errors="coerce").tolist()
    return win, count_vs_line(vals, line)


def player_windows(
    rounds: pd.DataFrame,
    *,
    stat_col: str,
    line: float,
    course_id: str | None = None,
    n_course: int = DEFAULT_COURSE_N,
    as_of: date | datetime | str | None = None,
) -> dict[str, object]:
    """Build all three windows + push-aware counts for one player/prop."""
    out: dict[str, object] = {}
    for kind in ("l5_all", "l5_course", "skill_window"):
        win, counts = count_stat_window(
            rounds,
            stat_col=stat_col,
            line=line,
            kind=kind,  # type: ignore[arg-type]
            course_id=course_id,
            n_course=n_course,
            as_of=as_of,
        )
        prefix = kind
        out[f"{prefix}_n"] = int(len(win))
        out[f"{prefix}_over"] = counts.over
        out[f"{prefix}_under"] = counts.under
        out[f"{prefix}_push"] = counts.push
        out[f"{prefix}_decided"] = counts.decided
        out[f"{prefix}_hit_rate_decided_over"] = counts.hit_rate_decided_over
        out[f"{prefix}_hit_rate_decided_under"] = counts.hit_rate_decided_under
        out[f"{prefix}_thin_decided"] = bool(counts.thin_decided)
        if kind == "l5_course":
            out["l5_course_n"] = int(len(win))
    return out


def counts_from_stat_g_row(
    row: pd.Series | dict,
    *,
    line: float,
    n: int = 5,
) -> LineCountResult:
    """Push-aware counts from already-attached ``stat_g1..gN`` (step5 path)."""
    vals: list[object] = []
    for i in range(1, n + 1):
        key = f"stat_g{i}"
        if isinstance(row, pd.Series):
            if key in row.index:
                vals.append(row.get(key))
        elif key in row:
            vals.append(row[key])
    return count_vs_line(vals, line)
