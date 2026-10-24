"""PrizePicks CBB / WCBB board family.

Verified 2026-08-24 via partner-api.prizepicks.com/leagues (off-season):
  20  CBB     projections_count=0
  189 NCAAB    projections_count=0
  290 CBB1H    projections_count=0
  155 CBB2H    projections_count=0
  WCBB not listed (women's tab is dark until November).

PrizePicks display names at tipoff are expected to be **NCAABB Mens** and
**NCAABB Women**. IDs can move; resolve_board_league_id() matches live names
and falls back to the constants below.

WCBB fallback 176 is the last in-season id used by ui_runner/commands.json.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Iterable

# Men's full-game (PP names: CBB, NCAAB, NCAABB Mens)
CBB = "20"
NCAAB = "189"

# Women's full-game (PP names: NCAABB Women, WCBB, CWBB). Dark off-season.
WCBB = "176"

# Period boards (empty until conference play / later in the season)
CBB1H = "290"
CBB2H = "155"

DEFAULT_CBB_BOARDS: dict[str, str] = {
    CBB: "CBB",
}

DEFAULT_WCBB_BOARDS: dict[str, str] = {
    WCBB: "WCBB",
}

PERIOD_BOARDS: dict[str, str] = {
    CBB1H: "CBB1H",
    CBB2H: "CBB2H",
}

LEAGUES_URL = "https://partner-api.prizepicks.com/leagues"

# Normalized needles. Women is checked first so "NCAABB WOMEN" never maps to CBB.
_WCBB_EXACT = frozenset({"WCBB", "CWBB", "NCAABB WOMEN", "NCAABB WOMENS", "NCAAB WOMEN", "NCAAB WOMENS"})
_CBB_EXACT = frozenset({"CBB", "NCAAB", "NCAABB", "NCAABB MEN", "NCAABB MENS", "NCAAB MEN", "NCAAB MENS"})


def _norm_name(name: str) -> str:
    s = str(name or "").upper().replace("'", "").replace(".", "")
    s = " ".join(s.split())
    s = s.replace("WOMEN'S", "WOMENS").replace("MEN'S", "MENS")
    return s


def classify_pp_league_name(name: str) -> str | None:
    """Map a PrizePicks league display name to cbb / wcbb / cbb1h / cbb2h."""
    n = _norm_name(name)
    if not n:
        return None
    if "1H" in n.replace(" ", "") or n.endswith("1H"):
        if "WOMEN" in n or n.startswith("WCBB") or n.startswith("CWBB"):
            return None
        if "CBB" in n or "NCAAB" in n:
            return "cbb1h"
    if "2H" in n.replace(" ", "") or n.endswith("2H"):
        if "WOMEN" in n:
            return None
        if "CBB" in n or "NCAAB" in n:
            return "cbb2h"
    if n in _WCBB_EXACT or "WOMEN" in n and ("NCAAB" in n or "CBB" in n or "COLLEGE" in n):
        return "wcbb"
    if n in _CBB_EXACT:
        return "cbb"
    if "WOMEN" in n:
        return None
    if n == "CBB" or n == "NCAAB":
        return "cbb"
    return None


def _fallback_id(board: str) -> str:
    b = str(board or "").strip().lower()
    if b == "wcbb":
        return WCBB
    if b == "cbb1h":
        return CBB1H
    if b == "cbb2h":
        return CBB2H
    return CBB


def pick_league_id_from_rows(board: str, rows: Iterable[dict]) -> str | None:
    """Choose a live league id for board ('cbb' or 'wcbb') from /leagues rows."""
    want = str(board or "").strip().lower()
    if want not in {"cbb", "wcbb", "cbb1h", "cbb2h"}:
        return None
    hits: list[tuple[int, str]] = []
    for row in rows:
        attrs = row.get("attributes") or {}
        name = str(attrs.get("name") or "")
        lid = str(row.get("id") or "").strip()
        if not lid:
            continue
        kind = classify_pp_league_name(name)
        if kind != want:
            continue
        count = int(attrs.get("projections_count") or 0)
        hits.append((count, lid))
    if not hits:
        return None
    hits.sort(key=lambda x: x[0], reverse=True)
    return hits[0][1]


def fetch_pp_league_rows(timeout: int = 20) -> list[dict]:
    req = urllib.request.Request(
        LEAGUES_URL,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    return list(payload.get("data") or [])


def resolve_board_league_id(board: str, league_id: str = "") -> str:
    """Resolve PrizePicks league id for 'cbb' or 'wcbb'.

    Explicit --league_id wins. Otherwise match live /leagues names, then constants.
    """
    explicit = str(league_id or "").strip()
    if explicit:
        return explicit
    try:
        live = pick_league_id_from_rows(board, fetch_pp_league_rows())
        if live:
            return live
    except Exception:
        pass
    return _fallback_id(board)
