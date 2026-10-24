"""Live PrizePicks sides. A pool leg stays only when the card offers that side.

The board file can label a Demon as Standard. Demons and Goblins are More
only. A Standard offers Less and More unless the projection's wager list
says otherwise. Nothing is ranked from a line the live card does not list.
"""
from __future__ import annotations

from typing import Any

from utils.prop_norm import canon_prop

# PrizePicks league ids. Period boards are their own ids (not the full-game
# parent). NBA1H=84 / NBA1Q=192 — mapping them to NBA=7 made every 1H/1Q
# line look card_side_missing against the full-game book.
LEAGUE_IDS = {
    "MLB": "2",
    "WNBA": "3",
    "WNBA1H": "193",
    "WNBA1Q": "308",
    "TENNIS": "5",
    "GOLF": "6",
    "NBA": "7",
    "NBA1H": "84",
    "NBA1Q": "192",
    "NHL": "8",
    "NFL": "9",
    "CFB": "15",
    "CFB1H": "15",
    "CBB": "20",
    "WCBB": "20",
    "SOCCER": "82",
    "SOC": "82",
}
_DEAD_STATUS = {"removed", "settled", "suspended", "closed", "finished", "inactive"}


def player_key(name: object) -> str:
    return " ".join(str(name or "").lower().split())


def line_key(line: object) -> int | None:
    try:
        value = float(line)
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    return int(round(value * 100))


def _book_pick(odds: object) -> str:
    raw = str(odds or "").strip().lower()
    if "demon" in raw:
        return "Demon"
    if "goblin" in raw:
        return "Goblin"
    if "standard" in raw:
        return "Standard"
    return ""


def _row_pick(row: dict[str, Any]) -> str:
    return _book_pick(row.get("pick_type") or row.get("odds_type"))


def _row_side(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or "").strip().upper()
    if raw in {"O", "OVER", "MORE"}:
        return "OVER"
    if raw in {"U", "UNDER", "LESS"}:
        return "UNDER"
    return ""


def _wager_sides(raw: object, pick: str) -> set[str]:
    found: set[str] = set()

    def take(text: object) -> None:
        token = str(text or "").strip().lower()
        if token in {"more", "over"}:
            found.add("OVER")
        elif token in {"less", "under"}:
            found.add("UNDER")

    if isinstance(raw, str):
        take(raw)
    elif isinstance(raw, dict):
        for key, val in raw.items():
            if val:
                take(key)
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                take(item.get("type") or item.get("name") or item.get("wager_type"))
            else:
                take(item)
    if found:
        if pick == "Goblin":
            return found & {"OVER"}
        if pick == "Demon":
            return set()
        # A Standard projection often lists only "over" here. The board still
        # shows Less and More. That single value is the default side.
        if pick == "Standard" and found == {"OVER"}:
            return {"OVER", "UNDER"}
        return found
    if pick == "Goblin":
        return {"OVER"}
    if pick == "Standard":
        return {"OVER", "UNDER"}
    return set()


class CardBook:
    """(player, prop, line, pick, side) the live card is offering."""

    def __init__(self, keys: set[tuple[str, str, int, str, str]]):
        self.keys = keys

    @classmethod
    def empty(cls) -> CardBook:
        return cls(set())

    def offers_side(self, row: dict[str, Any]) -> bool:
        key = self.row_key(row)
        return key is not None and key in self.keys

    @staticmethod
    def row_key(row: dict[str, Any]) -> tuple[str, str, int, str, str] | None:
        sport = str(row.get("sport") or "").strip().upper()
        player = player_key(row.get("player"))
        prop = canon_prop(sport, row.get("prop_canon") or row.get("prop") or row.get("prop_type"))
        line = line_key(row.get("line"))
        pick = _row_pick(row)
        side = _row_side(row)
        if not player or not prop or line is None or not pick or not side:
            return None
        return (player, prop, line, pick, side)


def _player_names(included: list[dict]) -> dict[str, str]:
    names: dict[str, str] = {}
    for obj in included:
        if str(obj.get("type") or "") not in {"new_player", "player"}:
            continue
        attrs = obj.get("attributes") or {}
        name = attrs.get("display_name") or attrs.get("name") or ""
        if obj.get("id") is not None and name:
            names[str(obj.get("id"))] = str(name)
    return names


def index_projections(data: list[dict], included: list[dict], sport: str) -> set[tuple]:
    """One key per offered side. A Demon adds nothing. A Goblin adds Over only."""
    names = _player_names(included)
    keys: set[tuple] = set()
    sport_name = str(sport or "").strip().upper()
    for row in data:
        if not isinstance(row, dict):
            continue
        attrs = row.get("attributes") or {}
        status = str(attrs.get("status") or "").strip().lower()
        if status in _DEAD_STATUS:
            continue
        rel = ((row.get("relationships") or {}).get("new_player") or {}).get("data") or {}
        if not rel:
            rel = ((row.get("relationships") or {}).get("player") or {}).get("data") or {}
        player = player_key(names.get(str(rel.get("id") or ""), ""))
        prop = canon_prop(sport_name, attrs.get("stat_type") or attrs.get("stat_display_name"))
        line = line_key(attrs.get("line_score"))
        pick = _book_pick(attrs.get("odds_type"))
        if not player or not prop or line is None or not pick:
            continue
        for side in _wager_sides(attrs.get("allowed_wager_types"), pick):
            keys.add((player, prop, line, pick, side))
    return keys


def load_live_card_book(
    sports: list[str] | None = None,
    *,
    cdp_url: str = "http://127.0.0.1:9222",
) -> CardBook | None:
    """Read projections through the open PrizePicks tab. None when that tab is down."""
    wanted = [str(s or "").strip().upper() for s in (sports or list(LEAGUE_IDS))]
    league_ids = []
    seen = set()
    for sport in wanted:
        lid = LEAGUE_IDS.get(sport)
        if not lid or lid in seen:
            continue
        seen.add(lid)
        league_ids.append((sport, lid))
    if not league_ids:
        return CardBook.empty()
    try:
        from playwright.sync_api import sync_playwright

        from utils.prizepicks_cdp import fetch_projections_inpage, pick_cdp_warmed_page
    except Exception:
        return None
    keys: set[tuple] = set()
    try:
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(cdp_url, timeout=15000)
            if not browser.contexts:
                return None
            page = pick_cdp_warmed_page(browser.contexts[0], browser=browser)
            if page is None:
                return None
            for sport, lid in league_ids:
                data, included, status, _url = fetch_projections_inpage(
                    page, lid, per_page=1000, max_pages=8, request_timeout_ms=30000
                )
                if status and int(status) >= 400:
                    return None
                if not data and int(status or 0) == 0:
                    return None
                keys |= index_projections(data, included, sport)
    except Exception:
        return None
    if not keys:
        return None
    return CardBook(keys)
