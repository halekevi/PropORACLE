"""Count made dunks from ESPN NBA play-by-play.

PrizePicks Dunks is made dunks only (putback, alley-oop, tip, reverse, driving).
Misses and blocked attempts do not count. Credit the shooter (first athlete).
OT counts — this is a full-game counting stat, not regulation-only.
"""
from __future__ import annotations

import re
from collections import Counter
from typing import Any, Iterable, Mapping

import requests

_DUNK_RE = re.compile(r"dunk", re.I)
_MAKE_RE = re.compile(r"\bmakes\b", re.I)
_MISS_RE = re.compile(r"\bmiss(?:es|ed)\b", re.I)
_BLOCK_RE = re.compile(r"\bblock(?:s|ed)\b", re.I)

_ESPN_PBP = "https://cdn.espn.com/core/nba/playbyplay?gameId={eid}&xhr=1"
_ESPN_SUMMARY = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={eid}"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _blob(play: Mapping[str, Any]) -> str:
    type_text = str(((play.get("type") or {}) if isinstance(play.get("type"), dict) else {}).get("text") or "")
    text = str(play.get("text") or "")
    return f"{type_text} {text}".strip()


def shooter_athlete_id(play: Mapping[str, Any]) -> str:
    parts = play.get("participants") or []
    if isinstance(parts, list):
        for p in parts:
            if not isinstance(p, dict):
                continue
            aid = str((p.get("athlete") or {}).get("id", "") or "").strip()
            if aid:
                return aid
    ath = play.get("athlete") or play.get("endAthlete") or {}
    if isinstance(ath, dict):
        return str(ath.get("id", "") or "").strip()
    return ""


def is_made_dunk_play(play: Mapping[str, Any]) -> bool:
    """True when this PBP event is a made dunk credited to a shooter."""
    if not isinstance(play, dict):
        return False
    blob = _blob(play)
    if not blob or not _DUNK_RE.search(blob):
        return False
    if _MISS_RE.search(blob) or _BLOCK_RE.search(blob):
        return False
    if play.get("shootingPlay") is True and play.get("scoringPlay") is False:
        return False
    if _MAKE_RE.search(blob):
        return True
    if play.get("scoringPlay") is True:
        try:
            if float(play.get("scoreValue") or 0) >= 2:
                return True
        except (TypeError, ValueError):
            return True
    result = str(play.get("shotResult") or play.get("result") or "").strip().lower()
    return result in {"made", "make", "scored"}


def dunks_by_athlete_id(plays: Iterable[Any]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for play in plays or []:
        if not is_made_dunk_play(play):
            continue
        aid = shooter_athlete_id(play)
        if aid:
            counts[aid] += 1
    return dict(counts)


def plays_from_summary_box(box: Mapping[str, Any] | None) -> list:
    if not isinstance(box, dict):
        return []
    gp = box.get("gamepackageJSON")
    if isinstance(gp, dict) and isinstance(gp.get("plays"), list):
        return gp["plays"]
    if isinstance(box.get("plays"), list):
        return box["plays"]
    return []


def _get_json(url: str, *, session: requests.Session | None, timeout: float) -> dict:
    getter = session.get if session is not None else requests.get
    r = getter(url, headers=_HEADERS, timeout=timeout)
    r.raise_for_status()
    payload = r.json() or {}
    return payload if isinstance(payload, dict) else {}


def espn_plays_for_event(
    event_id: str,
    box: Mapping[str, Any] | None = None,
    *,
    session: requests.Session | None = None,
    timeout: float = 25.0,
) -> list:
    """Plays from a summary JSON if present, else ESPN summary / PBP XHR."""
    found = plays_from_summary_box(box)
    if found:
        return found
    eid = str(event_id or "").strip()
    if not eid:
        return []
    try:
        payload = _get_json(_ESPN_SUMMARY.format(eid=eid), session=session, timeout=timeout)
        found = plays_from_summary_box(payload)
        if found:
            return found
    except Exception:
        pass
    try:
        payload = _get_json(_ESPN_PBP.format(eid=eid), session=session, timeout=timeout)
        found = plays_from_summary_box(payload)
        if found:
            return found
        gp2 = payload.get("gamepackageJSON", {}) or {}
        pl = gp2.get("plays") or []
        return pl if isinstance(pl, list) else []
    except Exception:
        return []


def apply_dunks_to_box_rows(rows: list[dict], plays: Iterable[Any]) -> list[dict]:
    """Stamp ``dunks`` on NBA box rows keyed by espn_athlete_id (0 if none)."""
    counts = dunks_by_athlete_id(plays)
    for r in rows:
        aid = str(r.get("espn_athlete_id") or "").strip()
        r["dunks"] = float(counts.get(aid, 0))
    return rows


def dunk_actual_rows(
    athlete_meta: Mapping[str, tuple[str, str]],
    plays: Iterable[Any],
) -> list[dict]:
    """One Dunks actuals row per boxscore athlete (0.0 if they did not dunk)."""
    counts = dunks_by_athlete_id(plays)
    rows: list[dict] = []
    for aid, (player_name, team_abbr) in athlete_meta.items():
        rows.append(
            {
                "player": player_name,
                "team": team_abbr,
                "prop_type": "Dunks",
                "actual": float(counts.get(str(aid), 0)),
            }
        )
    return rows
