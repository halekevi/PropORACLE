"""Grade a leg from its own start, and void only a real miss of that start.

A stored start is usable when it parses to an Eastern date and the clock is
not 23:00 ET. That 23:00 stamp is shared across unrelated legs, so it is a
placeholder, not a start.

The match window is six hours either side of a usable start. The actuals file
for the Eastern date of the start is the grade source. A start within six
hours of Eastern midnight also checks the neighboring date. If the player is
in one of those files, that row is the grade. If a usable start has no row
inside the window, the leg is a real void (the match was not found). A missing
start, or the 23:00 placeholder, is unresolved. It is not a void, and it does
not pay the slip.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from utils.eastern_slate_date import eastern_slate_date

ET = ZoneInfo("America/New_York")
START_MATCH_WINDOW = timedelta(hours=6)
PLACEHOLDER_CLOCK = "T23:00:00"

REAL_VOID_MARKERS = (
    "WALKOVER",
    "RETIRE",
    "W/O",
    "POSTPON",
    "CANCEL",
)


def is_placeholder_start(raw: object) -> bool:
    return PLACEHOLDER_CLOCK in str(raw or "")


def usable_start_date(raw: object) -> str:
    """Eastern date of a real start. Empty when the stamp is missing or 23:00."""
    text = str(raw or "").strip()
    if not text or is_placeholder_start(text):
        return ""
    return eastern_slate_date(text) or ""


def _parsed(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text or is_placeholder_start(text):
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ET)


def grade_dates_for_start(raw: object) -> list[str]:
    """Eastern dates whose actuals files can hold this start.

    The start's own Eastern date is first. The neighbor is included only when
    the start sits inside the six-hour window of midnight.
    """
    day = usable_start_date(raw)
    if not day:
        return []
    out = [day]
    dt = _parsed(raw)
    if dt is None:
        return out
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    next_midnight = midnight + timedelta(days=1)
    if dt - midnight <= START_MATCH_WINDOW:
        prev = (dt.date() - timedelta(days=1)).isoformat()
        if prev not in out:
            out.append(prev)
    elif next_midnight - dt <= START_MATCH_WINDOW:
        nxt = (dt.date() + timedelta(days=1)).isoformat()
        if nxt not in out:
            out.append(nxt)
    return out


def within_window(stored_start: object, event_start: object) -> bool:
    """True when both stamps parse and the event is within six hours."""
    left = _parsed(stored_start)
    right = _parsed(event_start)
    if left is None or right is None:
        return False
    return abs(left - right) <= START_MATCH_WINDOW


def missing_actual_disposition(start_raw: object, void_note: object = "") -> str:
    """How a leg with no box score is classified.

    ``VOID`` is a real void: walkover, retirement, postponement, or a usable
    start with no match inside the window. ``UNRESOLVED`` is a fetch gap or a
    placeholder stamp. Only ``VOID`` drops out of the slip.
    """
    note = str(void_note or "").upper()
    if any(tok in note for tok in REAL_VOID_MARKERS):
        return "VOID"
    if usable_start_date(start_raw):
        return "VOID"
    return "UNRESOLVED"
