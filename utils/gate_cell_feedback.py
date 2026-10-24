"""Stage 7: demote a gate cell whose rolling hit rate loses to its payout.

Eligibility reads data/reports/gate_cell_demotions.json. A missing file
demotes nothing. The grader writes that file when a cell's hit rate falls
under (1/multiplier) ** (1/legs) for the payout that cell is used on.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

_PATH = Path(__file__).resolve().parents[1] / "data" / "reports" / "gate_cell_demotions.json"


def breakeven_leg_p(multiplier: float, n_legs: int) -> float:
    """Per-leg hit rate a Power slip needs so P(all hit) * multiplier = 1."""
    if multiplier <= 1 or n_legs <= 0:
        return 1.0
    return (1.0 / float(multiplier)) ** (1.0 / int(n_legs))


def should_demote(hit_rate: float, *, multiplier: float, n_legs: int) -> bool:
    return float(hit_rate) < breakeven_leg_p(multiplier, n_legs)


@lru_cache(maxsize=1)
def _demoted_keys() -> frozenset[tuple[str, str, str, str]]:
    if not _PATH.is_file():
        return frozenset()
    try:
        raw = json.loads(_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return frozenset()
    cells = raw.get("cells") if isinstance(raw, dict) else None
    if not isinstance(cells, list):
        return frozenset()
    out: set[tuple[str, str, str, str]] = set()
    for c in cells:
        if not isinstance(c, dict):
            continue
        out.add(
            (
                str(c.get("sport") or "").strip().upper(),
                str(c.get("prop") or "").strip().lower(),
                str(c.get("pick_type") or "").strip().lower(),
                str(c.get("direction") or c.get("side") or "").strip().upper(),
            )
        )
    return frozenset(out)


def cell_is_demoted(r: dict[str, Any]) -> bool:
    pick = str(r.get("pick_type") or "").strip().lower()
    if "goblin" in pick:
        pick = "goblin"
    elif "standard" in pick:
        pick = "standard"
    key = (
        str(r.get("sport") or "").strip().upper(),
        str(r.get("prop") or r.get("prop_type") or "").strip().lower(),
        pick,
        str(r.get("side") or r.get("direction") or "").strip().upper(),
    )
    return key in _demoted_keys()
