"""Stable payout identity for live_cdp floor merge (ticket_id primary, sig fallback).

Leg order does not matter. A sound sig requires, for every leg:
  player, prop, direction, line, pick_type
plus ticket-level Power/Flex and leg count.

When more than one live ticket shares a sig, callers must skip all of them —
a wrong live_cdp floor is worse than a missing one.
"""
from __future__ import annotations

from typing import Any


def _norm(s: Any) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _line_key(v: Any) -> str:
    try:
        return f"{float(v):.3f}"
    except (TypeError, ValueError):
        return ""


def ticket_product_token(ticket_or_rec: dict[str, Any] | None) -> str:
    """Normalize Power vs Flex. Default power (CDP capture primary path)."""
    if not isinstance(ticket_or_rec, dict):
        return "power"
    pay = ticket_or_rec.get("payout") if isinstance(ticket_or_rec.get("payout"), dict) else {}
    for raw in (
        ticket_or_rec.get("play"),
        ticket_or_rec.get("product"),
        ticket_or_rec.get("ticket_type"),
        ticket_or_rec.get("ticket_type_captured"),
        pay.get("ticket_type"),
    ):
        s = str(raw or "").strip().lower()
        if not s:
            continue
        if "flex" in s:
            return "flex"
        if "power" in s:
            return "power"
    if ticket_or_rec.get("flex_payout") not in (None, "", 0, 0.0) and ticket_or_rec.get(
        "power_payout"
    ) in (None, "", 0, 0.0):
        return "flex"
    return "power"


def leg_parts_for_sig(legs: list | None) -> list[str]:
    parts: list[str] = []
    for leg in legs or []:
        if not isinstance(leg, dict):
            continue
        parts.append(
            "|".join(
                [
                    _norm(leg.get("player")),
                    _norm(leg.get("prop_type") or leg.get("prop")),
                    _norm(leg.get("direction") or leg.get("dir") or "over"),
                    _line_key(leg.get("line")),
                    _norm(leg.get("pick_type") or leg.get("pick") or ""),
                ]
            )
        )
    return sorted(p for p in parts if p and not p.endswith("||||") and p.count("|") == 4)


def ticket_payout_sig(
    ticket_or_rec: dict[str, Any] | None = None,
    *,
    legs: list | None = None,
    product: str | None = None,
    n_legs: int | None = None,
) -> str:
    """Full payout signature. Empty string when legs are unusable."""
    src = ticket_or_rec if isinstance(ticket_or_rec, dict) else {}
    raw_legs = legs if legs is not None else src.get("legs")
    parts = leg_parts_for_sig(raw_legs if isinstance(raw_legs, list) else [])
    if not parts:
        return ""
    n = int(n_legs) if n_legs is not None else 0
    if n <= 0:
        try:
            n = int(src.get("n_legs") or 0)
        except (TypeError, ValueError):
            n = 0
    if n <= 0:
        n = len(parts)
    prod = _norm(product) if product else ticket_product_token(src)
    if prod not in ("flex", "power"):
        prod = "flex" if "flex" in prod else "power"
    # Prefix so product/n_legs cannot collide with a leg field.
    return f"{prod}|{n}||" + "||".join(parts)


def ambiguous_sig_keys(tickets: list[dict[str, Any]]) -> set[str]:
    """Sigs that appear on more than one ticket in the current card."""
    counts: dict[str, int] = {}
    for t in tickets:
        if not isinstance(t, dict):
            continue
        sig = ticket_payout_sig(t)
        if not sig:
            continue
        counts[sig] = counts.get(sig, 0) + 1
    return {s for s, n in counts.items() if n > 1}
