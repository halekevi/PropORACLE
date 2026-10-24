#!/usr/bin/env python3
"""PrizePicks CDP Submit wrapped with the slate exposure ledger.

Any manual/session submit path must use ``submit_lineup_guarded`` (or call
``assert_clears_exposure`` first). Clicking Submit without the check is the
Dampier/Overton failure mode.
"""

from __future__ import annotations

import time
from typing import Any, Iterable

from utils.manual_slip_exposure import (
    ExposureBlocked,
    assert_clears_exposure,
    default_slate_date,
    register_after_submit,
)


def _click_submit(page, frame) -> str:
    for loc in (
        frame.get_by_role("button", name="Submit Lineup"),
        frame.get_by_text("Submit Lineup", exact=True),
        page.get_by_role("button", name="Submit Lineup"),
        page.get_by_text("Submit Lineup", exact=True),
    ):
        try:
            loc.first.click(timeout=2500)
            return "clicked_submit"
        except Exception:
            continue
    try:
        ok = page.evaluate(
            """() => {
              const els = Array.from(document.querySelectorAll('button, [role=button]'));
              for (const el of els) {
                const t = (el.innerText || '').trim();
                if (t === 'Submit Lineup' || t === 'Submit') { el.click(); return t; }
              }
              return '';
            }"""
        )
        if ok:
            return f"js:{ok}"
    except Exception:
        pass
    return "submit_not_found"


def submit_lineup_guarded(
    page,
    frame,
    legs: Iterable[dict[str, Any]],
    *,
    slate_date: str | None = None,
    explicit_anchor_player: str | None = None,
    register: bool = True,
    ticket_id: str = "",
    scraped_payout_x: float | None = None,
) -> str:
    """Run exposure check → Submit → optional session register.

    Raises ``ExposureBlocked`` before any click when the slip fails the ledger.
    Callers must treat that as a hard stop (print + exit 2) — never map it to a
    soft string like ``blocked_exposure`` and continue into confirm loops.
    """
    d = str(slate_date or default_slate_date())[:10]
    rows = [dict(leg) for leg in legs if isinstance(leg, dict)]
    assert_clears_exposure(
        rows,
        slate_date=d,
        explicit_anchor_player=explicit_anchor_player,
    )
    result = _click_submit(page, frame)
    if result in {"submit_not_found"}:
        return result
    time.sleep(0.4)
    if register:
        register_after_submit(
            rows,
            slate_date=d,
            explicit_anchor_player=explicit_anchor_player,
            ticket_id=ticket_id,
        )
    _stamp_submit_payout(
        slate_date=d,
        ticket_id=ticket_id,
        legs=rows,
        scraped_payout_x=scraped_payout_x,
    )
    return result


def _stamp_submit_payout(
    *,
    slate_date: str,
    ticket_id: str,
    legs: list[dict[str, Any]],
    scraped_payout_x: float | None,
) -> None:
    """Persist the scraped N-correct multiple on the dated archive at submit."""
    try:
        mult = float(scraped_payout_x) if scraped_payout_x is not None else 0.0
    except (TypeError, ValueError):
        return
    if mult <= 0:
        return
    from pathlib import Path

    from utils.archive_payout_stamp import stamp_scraped_payouts_on_archives

    root = Path(__file__).resolve().parents[1]
    patch = {
        "by_ticket_id": {
            ticket_id or "submit": {
                "ticket_id": ticket_id,
                "legs": legs,
                "n_legs": len(legs),
                "power_min_x": mult,
                "display_min_x": mult,
                "payout_source": "live_cdp",
                "play": "power",
            }
        }
    }
    stamp_scraped_payouts_on_archives(slate_date, patch, root=root)


__all__ = [
    "ExposureBlocked",
    "assert_clears_exposure",
    "submit_lineup_guarded",
]
