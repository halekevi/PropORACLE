"""Zero actuals with zero minutes are DNP. Unknown minutes stay out of cells."""
from __future__ import annotations


def minutes_status(minutes: float | None) -> str:
    """played, dnp, or minutes_unknown."""
    if minutes is None:
        return "minutes_unknown"
    if minutes <= 0:
        return "dnp"
    return "played"
