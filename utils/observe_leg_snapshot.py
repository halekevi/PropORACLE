"""Leg fields stored with a payout quote at scrape time.

Goblin-70 status is computed from the leg's L5, L10, and D as posted then.
A missing window stays null. It is not treated as a gate failure.
"""
from __future__ import annotations

from typing import Any


def _num(raw: object) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN
        return None
    return v


def _side(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if s in ("O", "OVER", "HIGHER"):
        return "OVER"
    if s in ("U", "UNDER", "LOWER"):
        return "UNDER"
    return ""


def _pick(raw: object) -> str:
    s = str(raw or "").strip().lower()
    if "gob" in s:
        return "Goblin"
    if "demon" in s:
        return "Demon"
    if "std" in s or s == "standard" or s == "":
        return "Standard"
    return str(raw or "").strip()


def _dir_count(leg: dict, side: str, window: str) -> float | None:
    if side == "OVER":
        keys = (f"{window}_over", f"last{window[1:]}_over")
    else:
        keys = (f"{window}_under", f"last{window[1:]}_under")
    for key in keys:
        v = _num(leg.get(key))
        if v is not None:
            return v
    return _num(leg.get(window))


def count_gate(leg: dict) -> bool | None:
    """L5=5, L10>=8, directional D. None when the window is incomplete."""
    from utils.defense_tiers import d_aligned

    side = _side(leg.get("direction") or leg.get("dir"))
    sport = str(leg.get("sport") or "").strip().upper()
    prop = str(leg.get("prop") or leg.get("prop_type") or "")
    if not side or not sport:
        return None
    l5 = _dir_count(leg, side, "l5")
    l10 = _dir_count(leg, side, "l10")
    tier = str(leg.get("def_tier") or "").strip()
    if l5 is None or l10 is None or not tier:
        return None
    if l5 != 5 or l10 < 8:
        return False
    return bool(d_aligned(sport, side, tier, prop))


def leg_snapshot(leg: dict) -> dict[str, Any]:
    """Compact quote-time record: line, standard line, gap, and gate status."""
    side = _side(leg.get("direction") or leg.get("dir"))
    line = _num(leg.get("line") or leg.get("played_line"))
    standard = _num(leg.get("standard_line") or leg.get("std_line"))
    gap = _num(leg.get("line_distance") or leg.get("delta"))
    if (gap is None or gap <= 0) and line is not None and standard is not None:
        gap = abs(standard - line)
    pick = _pick(leg.get("pick_type") or leg.get("pick"))
    gate = count_gate(leg)
    return {
        "player": str(leg.get("player") or "").strip(),
        "prop": str(leg.get("prop") or leg.get("prop_type") or "").strip(),
        "sport": str(leg.get("sport") or "").strip().upper(),
        "pick_type": pick,
        "direction": side,
        "line": line,
        "standard_line": standard,
        "gap": None if gap is None else round(float(gap), 4),
        "l5": _dir_count(leg, side, "l5") if side else None,
        "l10": _dir_count(leg, side, "l10") if side else None,
        "def_tier": str(leg.get("def_tier") or "").strip(),
        "role_tier": str(leg.get("role_tier") or "").strip(),
        "count_gate": gate,
        "goblin70": gate if pick == "Goblin" else None,
    }
