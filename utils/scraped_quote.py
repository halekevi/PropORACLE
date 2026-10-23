"""Fail-closed rule for “real scraped N-correct” quotes.

Promotion bar and P&L shadow count a quote as real only when
``payout_source`` is a scrape family **and** ``captured_at`` is non-null.
Blank sources, mix_floor / goblin_3 / median / p25 pins never qualify —
even if a multiple is present. That way the next labeling bug cannot
contaminate the forward test unnoticed.
"""
from __future__ import annotations

from typing import Any, Iterable

SCRAPED_PAYOUT_SOURCES = frozenset({"live_cdp", "n_correct_live"})


def is_real_scraped_quote(source: object, captured_at: object) -> bool:
    src = str(source or "").strip().lower()
    if src not in SCRAPED_PAYOUT_SOURCES:
        return False
    cap = str(captured_at or "").strip()
    if not cap or cap.lower() in {"none", "null"}:
        return False
    return True


def _row_source_and_capture(row: dict[str, Any]) -> tuple[object, object]:
    src = row.get("payout_source")
    cap = row.get("captured_at")
    pay = row.get("payout") if isinstance(row.get("payout"), dict) else {}
    if pay:
        src = src or pay.get("payout_source")
        cap = cap or pay.get("captured_at")
    return src, cap


def real_scraped_payout_x(row: dict[str, Any]) -> float | None:
    """Numeric all-hit multiple when the row clears is_real_scraped_quote."""
    if not isinstance(row, dict):
        return None
    src, cap = _row_source_and_capture(row)
    if not is_real_scraped_quote(src, cap):
        return None
    pay = row.get("payout") if isinstance(row.get("payout"), dict) else {}
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


def count_real_quotes(rows: Iterable[dict[str, Any]]) -> tuple[int, int]:
    """Return ``(real_n, total_n)`` for slip-shaped dicts."""
    total = 0
    real = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        total += 1
        src, cap = _row_source_and_capture(row)
        if is_real_scraped_quote(src, cap):
            real += 1
    return real, total


def format_real_quote_coverage(real_n: int, total_n: int, *, label: str = "") -> str:
    """e.g. ``real_quote 2 of 5 slips (3 excluded)``."""
    excluded = max(0, int(total_n) - int(real_n))
    prefix = f"{label}: " if label else ""
    return (
        f"{prefix}real_quote {int(real_n)} of {int(total_n)} slips "
        f"({excluded} excluded)"
    )


def log_real_quote_coverage(
    rows: Iterable[dict[str, Any]],
    *,
    label: str = "",
    print_fn=print,
) -> tuple[int, int]:
    """Print coverage for this run; return ``(real_n, total_n)``."""
    real_n, total_n = count_real_quotes(rows)
    if total_n <= 0:
        return real_n, total_n
    print_fn(format_real_quote_coverage(real_n, total_n, label=label))
    return real_n, total_n


def iter_payload_tickets(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten group tickets from a /tickets-style payload."""
    out: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return out
    for g in payload.get("groups") or []:
        if not isinstance(g, dict):
            continue
        for t in g.get("tickets") or []:
            if isinstance(t, dict):
                out.append(t)
    for t in payload.get("tickets") or []:
        if isinstance(t, dict):
            out.append(t)
    return out
