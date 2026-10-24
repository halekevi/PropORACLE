"""Soccer ticket eligibility follows the competitions we can settle.

A leg is eligible only when its game appears on a covered competition's ESPN
scoreboard for the Eastern slate day. Club names are not a block list: Norwich
in the Championship is on ``eng.2`` and stays eligible; Norwich in a cup that
the scoreboard fetch does not request stays off. Gate cells for Shots, SOT,
and Saves were fit on graded rows from those same competitions.
"""
from __future__ import annotations

import json
import re
import urllib.request
from datetime import date, timedelta
from functools import lru_cache
from typing import Any, Callable

from utils.eastern_slate_date import eastern_slate_date, event_start_raw, keep_event_on_slate

_DROP = frozenset({"FC", "CF", "SC", "AFC", "AC", "CD", "EC", "THE", "DE", "DA", "VS"})
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _soccer_leagues() -> list[tuple[str, str]]:
    import sys
    from pathlib import Path

    scripts = Path(__file__).resolve().parents[1] / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    from fetch_actuals import SOCCER_LEAGUES

    return list(SOCCER_LEAGUES)


def _tokens(text: str) -> set[str]:
    raw = re.sub(r"[^A-Z0-9]+", " ", str(text or "").upper())
    return {part for part in raw.split() if part not in _DROP and len(part) >= 3}


def _side_hits(leg_side: str, blob: str) -> bool:
    tokens = _tokens(leg_side)
    blob_tokens = _tokens(blob)
    if not tokens or not blob_tokens:
        return False
    strong = {tok for tok in tokens if len(tok) >= 4} or tokens
    return all(any(tok in other or other in tok for other in blob_tokens) for tok in strong)


def _team_blobs(event: dict[str, Any]) -> list[str]:
    comps = event.get("competitions") or []
    comp = comps[0] if comps and isinstance(comps[0], dict) else event
    blobs: list[str] = []
    for competitor in comp.get("competitors") or []:
        if not isinstance(competitor, dict):
            continue
        team = competitor.get("team") or {}
        if not isinstance(team, dict):
            team = {}
        pieces = [
            team.get("displayName"),
            team.get("shortDisplayName"),
            team.get("name"),
            team.get("abbreviation"),
            team.get("location"),
        ]
        blob = " ".join(str(p) for p in pieces if p)
        if blob.strip():
            blobs.append(blob)
    return blobs


def event_matches_sides(side_a: str, side_b: str, event: dict[str, Any]) -> bool:
    """True when the two clubs assign to the two sides of this event."""
    blobs = _team_blobs(event)
    if len(blobs) < 2:
        return False
    a0, a1 = _side_hits(side_a, blobs[0]), _side_hits(side_a, blobs[1])
    b0, b1 = _side_hits(side_b, blobs[0]), _side_hits(side_b, blobs[1])
    return (a0 and b1) or (a1 and b0)


def match_sides(row: dict[str, Any]) -> tuple[str, str] | None:
    """Club pair for a soccer leg. None when the row does not name a match."""
    sport = str(row.get("sport") or "").strip().upper().replace(" ", "").replace("-", "")
    soccer = sport in {"SOCCER", "SOC"} or sport.startswith("SOCCER")
    matchup = str(row.get("matchup") or row.get("game") or "")
    if "(SOCCER" in matchup.upper():
        soccer = True
    if not soccer:
        return None
    found = re.search(r"(.+?)\s+vs\.?\s+(.+?)(?:\s*\(|$)", matchup, flags=re.IGNORECASE)
    if found:
        left, right = found.group(1).strip(), found.group(2).strip()
        if left and right:
            return left, right
    team = str(row.get("team") or "").strip()
    opp = str(row.get("opp_team") or row.get("opponent") or row.get("opp") or "").strip()
    if team and opp and team.upper() != opp.upper():
        return team, opp
    return None


def slate_day(row: dict[str, Any]) -> str:
    for key in ("start_time", "game_time", "slate_date", "game_date", "date"):
        got = eastern_slate_date(row.get(key))
        if got:
            return got
    return ""


def _fetch_json(url: str, timeout: float = 20.0) -> dict[str, Any] | None:
    req = urllib.request.Request(url, headers=_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _default_fetch(league_id: str, ymd: str) -> dict[str, Any] | None:
    stamp = ymd.replace("-", "")
    urls = (
        f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league_id}/scoreboard?dates={stamp}",
        f"https://site.web.api.espn.com/apis/site/v2/sports/soccer/{league_id}/scoreboard?dates={stamp}",
    )
    for url in urls:
        payload = _fetch_json(url)
        if payload is not None:
            return payload
    return None


FetchScoreboard = Callable[[str, str], dict[str, Any] | None]


@lru_cache(maxsize=32)
def _covered_events(slate_date: str) -> tuple[dict[str, Any], ...] | None:
    return covered_events(slate_date)


def covered_events(
    slate_date: str,
    *,
    fetch: FetchScoreboard | None = None,
) -> tuple[dict[str, Any], ...] | None:
    """Events on covered leagues whose Eastern start is ``slate_date``.

    None means every scoreboard request failed. An empty tuple means the
    boards answered and listed no game that day.
    """
    want = str(slate_date or "").strip()[:10]
    if len(want) != 10:
        return None
    getter = fetch or _default_fetch
    nxt = (date.fromisoformat(want) + timedelta(days=1)).isoformat()
    found: list[dict[str, Any]] = []
    seen: set[str] = set()
    any_ok = False
    for league_id, _name in _soccer_leagues():
        for ymd in (want, nxt):
            payload = getter(league_id, ymd)
            if payload is None:
                continue
            any_ok = True
            for event in payload.get("events") or []:
                if not isinstance(event, dict):
                    continue
                if not keep_event_on_slate(event_start_raw(event), want, ymd):
                    continue
                eid = str(event.get("id") or "")
                if eid and eid in seen:
                    continue
                if eid:
                    seen.add(eid)
                # Stamp league for summary/boxscore lookups (Saves settlement).
                tagged = dict(event)
                tagged["_league_id"] = league_id
                found.append(tagged)
    if not any_ok:
        return None
    return tuple(found)


def clear_covered_events_cache() -> None:
    _covered_events.cache_clear()


def game_on_covered_board(
    row: dict[str, Any],
    *,
    fetch: FetchScoreboard | None = None,
) -> bool | None:
    """True when this soccer match is on a covered scoreboard.

    None when the row does not name a match (the other gates still apply).
    False when the match is named and it is not on a covered board, including
    when the boards could not be read.
    """
    sides = match_sides(row)
    if sides is None:
        return None
    day = slate_day(row)
    if not day:
        return False
    if fetch is None:
        events = _covered_events(day)
    else:
        events = covered_events(day, fetch=fetch)
    if not events:
        return False
    side_a, side_b = sides
    return any(event_matches_sides(side_a, side_b, event) for event in events)


def _name_key(raw: object) -> str:
    return " ".join(str(raw or "").casefold().split())


def find_covered_event(
    row: dict[str, Any],
    *,
    fetch: FetchScoreboard | None = None,
) -> dict[str, Any] | None:
    """Matching covered event for this soccer row, or None."""
    sides = match_sides(row)
    if sides is None:
        return None
    day = slate_day(row)
    if not day:
        return None
    events = covered_events(day, fetch=fetch) if fetch else _covered_events(day)
    if not events:
        return None
    side_a, side_b = sides
    for event in events:
        if event_matches_sides(side_a, side_b, event):
            return event
    return None


def _summary_for_event(event: dict[str, Any]) -> dict[str, Any] | None:
    eid = str(event.get("id") or "").strip()
    league = str(event.get("_league_id") or "").strip()
    if not eid or not league:
        # Fall back to leagues block on the event when stamp is missing.
        leagues = event.get("leagues") or event.get("league")
        if isinstance(leagues, list) and leagues:
            league = str(
                (leagues[0] or {}).get("slug")
                or (leagues[0] or {}).get("abbreviation")
                or ""
            ).strip()
        elif isinstance(leagues, dict):
            league = str(leagues.get("slug") or leagues.get("abbreviation") or "").strip()
    if not eid or not league:
        return None
    urls = (
        f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/summary?event={eid}",
        f"https://site.web.api.espn.com/apis/site/v2/sports/soccer/{league}/summary?event={eid}",
    )
    for url in urls:
        payload = _fetch_json(url)
        if payload is not None:
            return payload
    return None


def event_has_boxscore_player_rows(event: dict[str, Any]) -> bool | None:
    """True when the ESPN summary has settleable player stat rows.

    None when the summary could not be fetched. False when the game is known
    but boxscore/rosters carry no player stats (América–Necaxa shape).
    """
    summary = _summary_for_event(event)
    if summary is None:
        return None
    players = (summary.get("boxscore") or {}).get("players") or []
    if isinstance(players, list) and players:
        for block in players:
            for group in (block or {}).get("statistics") or []:
                if group.get("athletes"):
                    return True
    for team_block in summary.get("rosters") or []:
        if not isinstance(team_block, dict):
            continue
        for entry in team_block.get("roster") or []:
            if not isinstance(entry, dict):
                continue
            stats = entry.get("stats") or []
            if stats and any(
                isinstance(s, dict) and s.get("value") not in (None, "", "--")
                for s in stats
            ):
                return True
    return False


def player_is_roster_starter(
    event: dict[str, Any],
    player: object,
) -> bool | None:
    """True when ``player`` is marked starter on the ESPN roster for this event.

    None when the summary/roster cannot be read. False when the roster loads
    and the name is missing or not a starter (Malagón-on-PP / Cota-started).
    """
    want = _name_key(player)
    if not want:
        return False
    summary = _summary_for_event(event)
    if summary is None:
        return None
    rosters = summary.get("rosters") or []
    if not rosters:
        return None
    for team_block in rosters:
        if not isinstance(team_block, dict):
            continue
        for entry in team_block.get("roster") or []:
            if not isinstance(entry, dict):
                continue
            ath = entry.get("athlete") or {}
            name = _name_key(ath.get("displayName") or ath.get("fullName"))
            if not name:
                continue
            if name != want and want not in name and name not in want:
                # last-name fallback for Luis Malagón / Ángel Malagón collisions:
                # require full-string containment only; last-name alone is too weak
                continue
            return bool(entry.get("starter"))
    return False


def _roster_has_stat_rows(summary: dict[str, Any]) -> bool:
    players = (summary.get("boxscore") or {}).get("players") or []
    if isinstance(players, list):
        for block in players:
            for group in (block or {}).get("statistics") or []:
                if group.get("athletes"):
                    return True
    for team_block in summary.get("rosters") or []:
        if not isinstance(team_block, dict):
            continue
        for entry in team_block.get("roster") or []:
            if not isinstance(entry, dict):
                continue
            stats = entry.get("stats") or []
            if stats and any(
                isinstance(s, dict) and s.get("value") not in (None, "", "--")
                for s in stats
            ):
                return True
    return False


@lru_cache(maxsize=64)
def league_has_box_history(league_id: str) -> bool:
    """True when a recent completed event in this league returned player stats.

    This game's boxscore is empty before kickoff. Coverage is a property of
    the competition, not of tonight's file.
    """
    league = str(league_id or "").strip()
    if not league:
        return False
    # Walk back a few days from "today" via scoreboard pages already used
    # by covered_events callers. A single event with stat rows is enough.
    from datetime import date as _date

    today = _date.today()
    for back in range(0, 14):
        ymd = (today - timedelta(days=back)).isoformat().replace("-", "")
        payload = _default_fetch(league, ymd)
        if not payload:
            continue
        for event in payload.get("events") or []:
            if not isinstance(event, dict):
                continue
            status = ((event.get("status") or {}).get("type") or {})
            state = str(status.get("state") or "")
            if state not in {"post", "in"} and not status.get("completed"):
                continue
            tagged = dict(event)
            tagged["_league_id"] = league
            summary = _summary_for_event(tagged)
            if summary and _roster_has_stat_rows(summary):
                return True
    return False


def competition_has_box_history(row: dict[str, Any]) -> bool:
    """League behind this row has produced settleable boxes before."""
    event = find_covered_event(row)
    league = str((event or {}).get("_league_id") or "").strip()
    if not league:
        return False
    return league_has_box_history(league)


def keeper_lineup_status(row: dict[str, Any]) -> str:
    """``starter``, ``not_starter``, or ``unposted``.

    ``unposted`` when this event has no starter flags yet (typical at a 1AM
    or 9AM build). That does not block the leg.
    """
    event = find_covered_event(row)
    if event is None:
        return "unposted"
    summary = _summary_for_event(event)
    if summary is None:
        return "unposted"
    want = _name_key(row.get("player") or row.get("player_name"))
    saw_starter = False
    matched = False
    is_starter = False
    for team_block in summary.get("rosters") or []:
        if not isinstance(team_block, dict):
            continue
        for entry in team_block.get("roster") or []:
            if not isinstance(entry, dict):
                continue
            if entry.get("starter"):
                saw_starter = True
            ath = entry.get("athlete") or {}
            name = _name_key(ath.get("displayName") or ath.get("fullName"))
            if not name or not want:
                continue
            if name == want or want in name or name in want:
                matched = True
                is_starter = bool(entry.get("starter"))
    if not saw_starter:
        return "unposted"
    if matched and is_starter:
        return "starter"
    return "not_starter"


def saves_leg_settleable(row: dict[str, Any]) -> bool:
    """Saves may be built before kickoff.

    Require the competition's past boxes to carry player stats. If tonight's
    lineup is already posted, the named keeper must be a starter. An unposted
    lineup does not block. This game's boxscore is not required.
    """
    if match_sides(row) is None:
        return True
    if not competition_has_box_history(row):
        return False
    return keeper_lineup_status(row) != "not_starter"
