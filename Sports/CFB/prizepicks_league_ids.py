"""PrizePicks CFB board family.

Verified 2026-09-03 via partner-api.prizepicks.com/leagues
(projections_count at check: CFB=4791, CFB1H=139).
"""

from __future__ import annotations

import json
from typing import Iterable

from utils.prizepicks_http import make_pp_session

# Full game
CFB = "15"

# Period boards
CFB1H = "184"
CFB2H = "150"
CFB4Q = "153"

# Season-long (not wired)
CFBSZN = "172"

DEFAULT_CFB_BOARDS: dict[str, str] = {
    CFB: "CFB",
}

PERIOD_BOARDS: dict[str, str] = {
    CFB1H: "CFB1H",
}

LEAGUES_URL = "https://partner-api.prizepicks.com/leagues"


def _norm_name(name: str) -> str:
    s = str(name or "").upper().replace("'", "").replace(".", "")
    return " ".join(s.split())


def classify_pp_league_name(name: str) -> str | None:
    n = _norm_name(name).replace(" ", "")
    if not n:
        return None
    if n in {"CFB1H", "NCAAF1H", "NCAABB1H"} or (n.endswith("1H") and "CFB" in n):
        return "cfb1h"
    if n in {"CFB2H"} or (n.endswith("2H") and "CFB" in n):
        return "cfb2h"
    if n in {"CFB", "NCAAF", "NCAAFB"}:
        return "cfb"
    return None


def pick_league_id_from_rows(board: str, rows: Iterable[dict]) -> str | None:
    want = str(board or "").strip().lower()
    if want not in {"cfb", "cfb1h", "cfb2h"}:
        return None
    hits: list[tuple[int, str]] = []
    for row in rows:
        attrs = row.get("attributes") or {}
        name = str(attrs.get("name") or "")
        lid = str(row.get("id") or "").strip()
        if not lid:
            continue
        if classify_pp_league_name(name) != want:
            continue
        count = int(attrs.get("projections_count") or 0)
        hits.append((count, lid))
    if not hits:
        return None
    hits.sort(key=lambda x: x[0], reverse=True)
    return hits[0][1]


def fetch_pp_league_rows(timeout: int = 20) -> list[dict]:
    session = make_pp_session({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    r = session.get(LEAGUES_URL, timeout=timeout)
    r.raise_for_status()
    payload = r.json() if hasattr(r, "json") else json.loads(r.text)
    return list(payload.get("data") or [])


def _fallback_id(board: str) -> str:
    b = str(board or "").strip().lower()
    if b == "cfb1h":
        return CFB1H
    if b == "cfb2h":
        return CFB2H
    return CFB


def resolve_board_league_id(board: str, league_id: str = "") -> str:
    """Resolve PrizePicks league id for 'cfb' or 'cfb1h'. Explicit --league_id wins."""
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
