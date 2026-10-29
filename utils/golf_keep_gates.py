"""Golf ticket gate — observe-only until a keep-gate cell is pre-registered.

Golf is unregistered in ``ticket_keep_registry`` (same CBB rule: no tickets
until a cell exists). This function stays False so a leftover import cannot
ticket L5=5+L10>=8. The keep-gate scan runs on the round cache over the
Tour break; forward shadow starts when the Tour returns.
"""
from __future__ import annotations

from typing import Any

GOLF_SPORTS = frozenset({"GOLF", "PGA"})


def golf_ticket_eligible(r: dict[str, Any]) -> bool:
    """Always off. Do not ticket Golf until a cell is pre-registered."""
    return False


def golf_observe_l5_l10(r: dict[str, Any]) -> bool:
    """L5=5 + L10>=8, no D — list/observe helper, not a ticket gate."""
    sport = str(r.get("sport") or "").strip().upper()
    if sport not in GOLF_SPORTS:
        return False
    pick = str(r.get("pick_type") or "").strip().lower()
    side = str(r.get("side") or r.get("direction") or "").strip().upper()
    if "demon" in pick or side not in {"OVER", "UNDER"}:
        return False
    if "goblin" in pick and side != "OVER":
        return False
    if "goblin" not in pick and "standard" not in pick:
        return False

    def _count(window: str) -> float | None:
        keys = (
            (f"{window}_under", f"last{window[1:]}_under", window)
            if side == "UNDER"
            else (f"{window}_over", f"last{window[1:]}_over", window)
        )
        for k in keys:
            try:
                return float(r.get(k))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
        return None

    l5 = _count("l5")
    l10 = _count("l10")
    return l5 is not None and l5 >= 5 and l10 is not None and l10 >= 8
