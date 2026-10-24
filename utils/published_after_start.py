"""Tickets published after their games had started.

The 2026-10-23 dual card went up after that slate's NBA tips. Nothing on it
could have been played as built, and a post-tip refresh can leak into inputs.
Those slips stay off the forward counter, the P&L shadow, and win-rate.

Do not VOID them. Stamp ``invalid_reason=published_after_start`` and the
exclude flags, same shape as ``stale_board``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

INVALID_REASON = "published_after_start"
AFTER_START_SLATE_DATES = frozenset({"2026-10-23"})


def ticket_published_after_start_reason(
    ticket: dict[str, Any] | None,
    *,
    slate_date: str = "",
    group_name: str = "",
) -> str | None:
    if not isinstance(ticket, dict):
        return None
    if str(ticket.get("invalid_reason") or "").strip().lower() == INVALID_REASON:
        return INVALID_REASON
    ds = str(
        slate_date
        or ticket.get("date")
        or ticket.get("slate_date")
        or ""
    )[:10]
    if ds in AFTER_START_SLATE_DATES:
        return INVALID_REASON
    return None


def ticket_is_published_after_start(
    ticket: dict[str, Any] | None,
    *,
    slate_date: str = "",
    group_name: str = "",
) -> bool:
    return (
        ticket_published_after_start_reason(
            ticket, slate_date=slate_date, group_name=group_name
        )
        is not None
    )


def annotate_published_after_start_ticket(
    ticket: dict[str, Any],
    *,
    slate_date: str = "",
    group_name: str = "",
) -> bool:
    """Stamp invalid/published_after_start and exclude flags. Never VOID."""
    existing = str(ticket.get("invalid_reason") or "").strip().lower()
    if existing == "stale_board":
        return False
    reason = ticket_published_after_start_reason(
        ticket, slate_date=slate_date, group_name=group_name
    )
    if not reason:
        return False
    ticket["invalid"] = True
    ticket["invalid_reason"] = reason
    ticket["exclude_from_winrate"] = True
    ticket["exclude_from_book"] = True
    ticket["exclude_from_forward"] = True
    res = str(ticket.get("result") or "").strip().upper()
    if res == "VOID":
        ticket["result"] = "INVALID"
    return True


def stamp_ticket_json_file(path: Path) -> int:
    """Annotate an on-disk ticket payload. Returns stamps applied."""
    p = Path(path)
    if not p.is_file():
        return 0
    import json

    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    if not isinstance(payload, dict):
        return 0
    from utils.stale_board_tickets import annotate_ticket_payload

    n = annotate_ticket_payload(payload)
    if n:
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return n
