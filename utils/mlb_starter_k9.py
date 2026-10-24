"""MLB starter K/9 helpers for hitter-strikeout soft context."""

from __future__ import annotations

from typing import Any, Mapping, Optional


def ip_to_outs(ip: object) -> float | None:
    """Baseball IP notation (e.g. 132.1 → 397 outs) → outs count."""
    try:
        v = float(ip)
    except (TypeError, ValueError):
        return None
    if v != v or v < 0:
        return None
    whole = int(v)
    frac = round(v - whole, 1)
    # .1 = 1 out, .2 = 2 outs
    if abs(frac - 0.1) < 1e-6:
        outs_frac = 1
    elif abs(frac - 0.2) < 1e-6:
        outs_frac = 2
    elif abs(frac) < 1e-6:
        outs_frac = 0
    else:
        # Already decimal innings (rare) — treat fractional as outs/3
        outs_frac = int(round(frac * 10)) % 10
        if outs_frac > 2:
            outs_frac = int(round(frac * 3))
    return float(whole * 3 + outs_frac)


def k9_from_so_ip(strikeouts: object, innings_pitched: object) -> float | None:
    """Season K/9 from SO and IP."""
    try:
        so = float(strikeouts)
    except (TypeError, ValueError):
        return None
    outs = ip_to_outs(innings_pitched)
    if outs is None or outs <= 0 or so != so:
        return None
    ip = outs / 3.0
    if ip <= 0:
        return None
    return round(so * 9.0 / ip, 2)


def starter_k9_from_staff_rec(rec: Mapping[str, Any] | None) -> float | None:
    if not rec:
        return None
    return k9_from_so_ip(rec.get("strikeouts"), rec.get("innings_pitched"))


def resolve_hitter_k_soft_k9(
    *,
    vs_hand_k9: object = None,
    season_k9: object = None,
) -> float | None:
    """Prefer vs-batter-hand K/9; fall back to season starter K/9."""
    for v in (vs_hand_k9, season_k9):
        try:
            f = float(v)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if f == f and f > 0:
            return f
    return None


# Soft bands around MLB ~8.5 K/9 average.
HIGH_STARTER_K9 = 10.0
LOW_STARTER_K9 = 7.0


def hitter_k_soft_bump(k9: Optional[float], direction: str, *, weight: float = 0.025) -> float:
    """
    Soft proj bump for hitter_strikeouts from starter K/9.

    High K/9 → more batter Ks → OVER; low → UNDER.
    Does not touch staff-tier D invert.
    """
    if k9 is None:
        return 0.0
    d = str(direction or "OVER").upper().strip()
    if k9 >= HIGH_STARTER_K9:
        return weight if d == "OVER" else -weight
    if k9 <= LOW_STARTER_K9:
        return -weight if d == "OVER" else weight
    return 0.0
