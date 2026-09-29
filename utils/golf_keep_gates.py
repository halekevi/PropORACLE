"""Golf ticket gate. REPLACES the default L5/L10/D stack.

Golf has no opponent, so D is not part of this gate. A golf prop is on only
when it clears L5 = 5 and L10 >= 8. There is no silent default behind this.
"""
from __future__ import annotations

from typing import Any

GOLF_SPORTS = frozenset({"GOLF", "PGA"})


def _num(v: object) -> float | None:
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _side(r: dict[str, Any]) -> str:
    s = str(r.get("side") or r.get("direction") or "").strip().upper()
    return s if s in {"OVER", "UNDER"} else ""


def _pick(r: dict[str, Any]) -> str:
    p = str(r.get("pick_type") or "").strip().lower()
    if "goblin" in p:
        return "Goblin"
    if "demon" in p:
        return "Demon"
    if "standard" in p:
        return "Standard"
    return ""


def _count(r: dict[str, Any], side: str, window: str) -> float | None:
    if side == "UNDER":
        keys = (f"{window}_under", f"last{window[1:]}_under", window)
    else:
        keys = (f"{window}_over", f"last{window[1:]}_over", window)
    for k in keys:
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def golf_ticket_eligible(r: dict[str, Any]) -> bool:
    """Goblin OVER or Standard O/U with L5=5 and L10>=8. No D."""
    sport = str(r.get("sport") or "").strip().upper()
    if sport not in GOLF_SPORTS:
        return False
    pick = _pick(r)
    side = _side(r)
    if pick == "Demon" or side not in {"OVER", "UNDER"}:
        return False
    if pick == "Goblin" and side != "OVER":
        return False
    if pick not in {"Goblin", "Standard"}:
        return False
    l5 = _count(r, side, "l5")
    l10 = _count(r, side, "l10")
    return l5 is not None and l5 >= 5 and l10 is not None and l10 >= 8
