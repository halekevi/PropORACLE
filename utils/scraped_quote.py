"""Fail-closed rule for “real scraped N-correct” quotes.

Promotion bar and P&L shadow count a quote as real only when
``payout_source`` is a scrape family **and** ``captured_at`` is non-null.
Blank sources, mix_floor / goblin_3 / median / p25 pins never qualify —
even if a multiple is present. That way the next labeling bug cannot
contaminate the forward test unnoticed.
"""
from __future__ import annotations

from typing import Any

SCRAPED_PAYOUT_SOURCES = frozenset({"live_cdp", "n_correct_live"})


def is_real_scraped_quote(source: object, captured_at: object) -> bool:
    src = str(source or "").strip().lower()
    if src not in SCRAPED_PAYOUT_SOURCES:
        return False
    cap = str(captured_at or "").strip()
    if not cap or cap.lower() in {"none", "null"}:
        return False
    return True


def real_scraped_payout_x(row: dict[str, Any]) -> float | None:
    """Numeric all-hit multiple when the row clears is_real_scraped_quote."""
    if not isinstance(row, dict):
        return None
    src = row.get("payout_source")
    cap = row.get("captured_at")
    pay = row.get("payout") if isinstance(row.get("payout"), dict) else {}
    if pay:
        src = src or pay.get("payout_source")
        cap = cap or pay.get("captured_at")
    if not is_real_scraped_quote(src, cap):
        return None
    for raw in (
        row.get("scraped_payout"),
        row.get("all_hit_x"),
        row.get("quote"),
        row.get("sweep_payout_x"),
        pay.get("sweep_payout_x") if pay else None,
        pay.get("display_min_x") if pay else None,
        pay.get("power_min_x") if pay else None,
    ):
        if raw in (None, ""):
            continue
        try:
            px = float(raw)
        except (TypeError, ValueError):
            continue
        if px > 0:
            return px
    return None
