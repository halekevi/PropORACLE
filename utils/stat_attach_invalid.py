"""NFL/CFB props whose historical L5 / actuals were sibling-mapped or zero-filled.

Tagged 2026-10-25 after the Sunday attach audit. Same job as ``face_status``:
``analysis_legs()`` and ``ml.discover_gates`` drop these unless the caller
asks to include them. A future gate search must not treat these HIT/MISS
rows as real.

Sacks are **not** in this set. They stay on the 2026-10-24 pre-registration
as a dated amendment (excluded from the shadow until attach is non-zero
for starters) so they can come back without a new registration.
"""
from __future__ import annotations

import re
from typing import Any

from utils.nfl_unsupported_props import is_football_period_split_prop, norm_football_prop

TAGGED = "2026-10-25"
SPORTS = frozenset({"NFL", "NFLP", "CFB", "CFB1H"})

# Folded (alnum) forms. Sibling-mapped or all-zero attach on 2026-10-25.
_YPC = frozenset(
    {
        "ypc",
        "yardspercarry",
        "rushingypc",
        "rushypc",
        "yardsperrush",
        "yardsperrushattempt",
        "rushingyardsperattempt",
        "rushingyardspercarry",
        "yardspercarryavg",
    }
)
_LONGEST_RECEPTION = frozenset(
    {
        "longestreception",
        "reclong",
        "longestrec",
        "longestcatch",
        "recyardslongest",
        "receivinglong",
        "longestrecyards",
    }
)
_COMPLETION = frozenset(
    {
        "completionpercentage",
        "completionpct",
        "completionspct",
        "passcompletionpercentage",
        "passcompletionpct",
        "passcomppct",
        "cmppct",
        "comppct",
        "passcompletions",
        "passcompletion",
        "passcmp",
        "completions",
        "longestcompletion",
        "longestpass",
        "longestpasscompletion",
        "passlong",
        "passinglong",
    }
)

_REASON = {
    "ypc": "invalid_mapping:YPC_used_rush_yards",
    "longest_reception": "invalid_mapping:longest_reception_used_catch_count",
    "completion": "invalid_mapping:completion_stat_zero_or_sibling",
    "period": "invalid_mapping:period_or_first_n_no_own_stat",
}


def _fold(raw: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(raw or "").lower())


def _sport(raw: object) -> str:
    return str(raw or "").strip().upper()


def analysis_invalid_reason(sport: object, prop: object) -> str:
    """Reason string when this sport+prop is invalid for analysis, else ''."""
    if _sport(sport) not in SPORTS:
        return ""
    if is_football_period_split_prop(prop) or is_football_period_split_prop(norm_football_prop(prop)):
        return _REASON["period"]
    fold = _fold(prop)
    if not fold:
        return ""
    if fold in _YPC or fold.endswith("ypc"):
        return _REASON["ypc"]
    if fold in _LONGEST_RECEPTION:
        return _REASON["longest_reception"]
    if fold in _COMPLETION:
        return _REASON["completion"]
    return ""


def is_analysis_invalid_prop(sport: object, prop: object) -> bool:
    return bool(analysis_invalid_reason(sport, prop))


def tag_rows(rows: list[dict[str, Any]], *, sport_key: str = "") -> tuple[list[dict[str, Any]], dict]:
    """Drop invalid-mapping props. Returns (kept, meta)."""
    kept: list[dict[str, Any]] = []
    dropped = 0
    by_reason: dict[str, int] = {}
    for r in rows:
        sp = str(r.get("sport") or sport_key or "").upper()
        prop = r.get("prop") or r.get("prop_canon") or r.get("prop_type") or ""
        reason = analysis_invalid_reason(sp, prop)
        if reason:
            dropped += 1
            by_reason[reason] = by_reason.get(reason, 0) + 1
            continue
        kept.append(r)
    return kept, {"dropped": dropped, "by_reason": by_reason, "tagged": TAGGED}
