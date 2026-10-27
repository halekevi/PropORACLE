"""As-of cutoff for step4 / box L5: tonight's game cannot enter the window.

If step4 reruns after a live box lands, ``game_date < slate`` keeps L5 and
the weekend forward log from including the result they are supposed to
predict. Per-row ``game_date`` wins when the slate mixes tips; ``slate_date``
is the fallback.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any


def normalize_slate_date(raw: object) -> str:
    """Return YYYY-MM-DD or empty."""
    if raw is None:
        return ""
    if isinstance(raw, datetime):
        return raw.date().isoformat()
    if isinstance(raw, date):
        return raw.isoformat()
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "none", "nat"}:
        return ""
    digits = s.replace("-", "").replace("/", "")[:8]
    if len(digits) == 8 and digits.isdigit():
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return s[:10] if len(s) >= 10 and s[4] == "-" else ""


def row_l5_cutoff(row: Any, slate_date: object = "") -> str:
    """Per-row game date, then the slate date. Empty means no SQL ceiling."""
    if row is not None:
        getter = row.get if hasattr(row, "get") else None
        if getter is not None:
            for key in (
                "game_date",
                "Game Date",
                "game_start",
                "start_time",
                "slate_date",
            ):
                cut = normalize_slate_date(getter(key))
                if cut:
                    return cut
    return normalize_slate_date(slate_date)


def box_date_before_slate(box_date: object, slate_date: object) -> bool:
    """True when the box is strictly before the slate (keep). No slate → keep."""
    cut = normalize_slate_date(slate_date)
    if not cut:
        return True
    box = normalize_slate_date(box_date)
    if not box:
        return False
    return box < cut


def game_date_before_slate_sql(
    slate_date: object,
    *,
    col: str = "game_date",
) -> tuple[str, tuple]:
    """``AND game_date < ?`` so tonight cannot enter L5. Empty slate → no clause."""
    cut = normalize_slate_date(slate_date)
    if not cut:
        return "", ()
    return f" AND {col} < ?", (cut,)
