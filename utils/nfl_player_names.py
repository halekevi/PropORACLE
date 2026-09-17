"""Normalize NFL player names for cache / participation joins.

Handles PrizePicks vs nflverse drift:
  - ``DJ Moore`` ↔ ``D.J. Moore``
  - ``James Cook III`` ↔ ``James Cook``
  - punctuation / extra whitespace
"""

from __future__ import annotations

import re

_SUFFIX_RE = re.compile(
    r"\b(jr|sr|ii|iii|iv|v)\b\.?$",
    re.IGNORECASE,
)


def norm_nfl_player_name(s: object) -> str:
    """Lowercase, strip periods/apostrophes, drop generational suffixes."""
    t = str(s or "").strip().lower()
    if not t:
        return ""
    t = t.replace(".", "").replace("'", "").replace("\u2019", "")
    t = re.sub(r"[-_]+", " ", t)
    t = " ".join(t.split())
    t = _SUFFIX_RE.sub("", t).strip()
    return " ".join(t.split())
