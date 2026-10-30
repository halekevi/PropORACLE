"""
Shared player-name normalization for actuals CSV, slate grader joins, ticket eval,
and injury sidecars.

Folds Unicode diacritics to ASCII and strips Jr./Sr./II–IV suffixes so slate, ESPN,
and injury rows agree on lookup keys.
"""

from __future__ import annotations

import re
import unicodedata

_SUFFIXES = re.compile(r"\s+(Jr\.?|Sr\.?|II|III|IV|V)$", re.IGNORECASE)

# Letters that do not NFD-decompose to a base + mark (ø strips to nothing).
# Transliterate first so PP "Hojgaard" / "Olesen" match ESPN "Højgaard" / "Olesen".
_TRANSLIT = str.maketrans(
    {
        "ø": "o",
        "Ø": "O",
        "æ": "ae",
        "Æ": "AE",
        "ß": "ss",
        "ð": "d",
        "Ð": "D",
        "þ": "th",
        "Þ": "Th",
    }
)


def normalize_player_name(name: str) -> str:
    """
    Normalize a display name for joins (actuals, slate, injuries):
    1. Transliterate non-decomposing letters (ø→o, æ→ae, ß→ss)
    2. NFD + ASCII (Vučević → Vucevic, Dončić → Doncic, Åberg → Aberg)
    3. Strip Jr./Sr./II/III/IV/V suffixes (Kelly Oubre Jr. → Kelly Oubre)
    4. Trim whitespace
    """
    if name is None:
        return ""
    if isinstance(name, float) and name != name:  # NaN
        return ""
    s = str(name).strip().translate(_TRANSLIT)
    s = unicodedata.normalize("NFD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.replace(".", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = _SUFFIXES.sub("", s).strip()
    return s
