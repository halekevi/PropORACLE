"""Slate calendar day is US Eastern, taken from the event start.

ESPN and several league APIs stamp start times in UTC. Slicing that timestamp
to ``YYYY-MM-DD`` files an 8pm Eastern match on the next day. A date-only
value (no time) is already a calendar day and is left as-is: treating it as
UTC midnight would shift it backward.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def eastern_slate_date(raw: object) -> str:
    """``YYYY-MM-DD`` in America/New_York for an event start. Empty if unknown."""
    s = str(raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    head = s[:10]
    if _DATE_ONLY.fullmatch(head) and len(s) == 10:
        return head
    text = s.replace("Z", "+00:00")
    if re.match(r"^\d{4}-\d{2}-\d{2} ", text):
        text = text.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(text[:32])
    except ValueError:
        return head if _DATE_ONLY.fullmatch(head) else ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ET).date().isoformat()


def event_start_raw(event: dict[str, Any] | None) -> str:
    """ESPN event or competition start string, including the offset when present."""
    if not isinstance(event, dict):
        return ""
    raw = str(event.get("date") or event.get("startDate") or event.get("startTimeUTC") or "").strip()
    if raw:
        return raw
    comps = event.get("competitions") or []
    if comps and isinstance(comps[0], dict):
        return str(comps[0].get("date") or comps[0].get("startDate") or "").strip()
    return ""


def on_eastern_slate(raw: object, slate_date: str) -> bool:
    """True when the start falls on the Eastern slate day."""
    want = str(slate_date or "").strip()[:10]
    got = eastern_slate_date(raw)
    return bool(want) and got == want


def keep_event_on_slate(start_raw: object, slate_date: str, queried_day: object) -> bool:
    """Keep an event on ``slate_date``.

    A parsed start must fall on that Eastern day. A start that does not parse
    stays on the scoreboard page it was listed on, so a neighbor-page query
    cannot drop it or copy it onto the wrong file.
    """
    want = str(slate_date or "").strip()[:10]
    if not want:
        return False
    et = eastern_slate_date(start_raw)
    if et:
        return et == want
    src = str(queried_day or "").strip()[:10]
    return src == want
