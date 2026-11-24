"""League on/off status from ESPN's season calendar.

A league goes inactive the day after its championship game is final and stays
inactive until the day before its first preseason (or regular-season) game, so the
9PM day-ahead run can fetch that first slate. Year-round sports (soccer, tennis,
golf) are not covered and keep whatever config/active_sports.json says.

ESPN's postseason window ends on a placeholder date (MLB 2026 lists Nov 12), so the
championship is the latest-dated postseason event, and it must be completed.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

REPO = Path(__file__).resolve().parents[1]
CACHE = REPO / "data" / "cache" / "season_calendar.json"
ET = ZoneInfo("America/New_York")
CORE = "https://sports.core.api.espn.com/v2/sports/{path}"

LEAGUE_PATH = {
    "nba": "basketball/leagues/nba",
    "wnba": "basketball/leagues/wnba",
    "nfl": "football/leagues/nfl",
    "nhl": "hockey/leagues/nhl",
    "mlb": "baseball/leagues/mlb",
    "cfb": "football/leagues/college-football",
    "cbb": "basketball/leagues/mens-college-basketball",
    "wcbb": "basketball/leagues/womens-college-basketball",
}

# PropORACLE board key -> league whose calendar it follows (period boards follow the parent).
BOARD_LEAGUE = {
    "NBA": "nba", "NBA1Q": "nba", "NBA1H": "nba",
    "WNBA": "wnba", "WNBA1Q": "wnba", "WNBA1H": "wnba",
    "NFL": "nfl", "NFL1Q": "nfl", "NFL1H": "nfl",
    "NHL": "nhl", "NHL1P": "nhl",
    "MLB": "mlb",
    "CFB": "cfb", "CFB1H": "cfb",
    "CBB": "cbb",
    "WCBB": "wcbb",
}

_CACHE_TTL_SEC = 20 * 3600


def _get(url: str) -> dict[str, Any]:
    r = requests.get(url, timeout=20, params={"limit": 1000})
    r.raise_for_status()
    return r.json()


def _et_day(iso: str) -> date | None:
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).astimezone(ET).date()
    except ValueError:
        return None


def _season_types(path: str, year: int) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    try:
        items = _get(CORE.format(path=f"{path}/seasons/{year}/types")).get("items") or []
    except requests.RequestException:
        return out
    for it in items:
        try:
            t = _get(it["$ref"])
        except (requests.RequestException, KeyError):
            continue
        out[int(t.get("type") or 0)] = {"start": t.get("startDate"), "end": t.get("endDate")}
    return out


def _event_days(path: str, year: int, season_type: int) -> list[tuple[date, str]]:
    """(ET game day, event $ref) for every event in a season type."""
    try:
        refs = [i["$ref"] for i in _get(CORE.format(path=f"{path}/seasons/{year}/types/{season_type}/events")).get("items") or []]
    except (requests.RequestException, KeyError):
        return []

    def one(ref: str) -> tuple[date, str] | None:
        try:
            d = _et_day(_get(ref).get("date") or "")
        except requests.RequestException:
            return None
        return (d, ref) if d else None

    with ThreadPoolExecutor(max_workers=8) as ex:
        return sorted(x for x in ex.map(one, refs) if x)


def _event_completed(ref: str) -> bool:
    try:
        ev = _get(ref)
        status_ref = ((ev.get("competitions") or [{}])[0].get("status") or {}).get("$ref")
        if not status_ref:
            return False
        return bool(((_get(status_ref).get("type") or {}).get("completed")))
    except (requests.RequestException, IndexError, AttributeError):
        return False


def _in_window(day: date, w: dict[str, Any]) -> bool:
    s, e = _et_day(w.get("start") or ""), _et_day(w.get("end") or "")
    return bool(s and e and s <= day < e)


def league_status(league: str, today: date | None = None) -> dict[str, Any]:
    """{'active': bool | None, 'reason': str, 'off_from': str, 'resume_on': str} for one league."""
    path = LEAGUE_PATH[league]
    today = today or datetime.now(ET).date()
    for year in (today.year - 1, today.year, today.year + 1):
        types = _season_types(path, year)
        current = next((t for t, w in types.items() if _in_window(today, w)), None)
        if current is None:
            continue

        def resume_info(next_year: int) -> dict[str, Any]:
            pre = _event_days(path, next_year, 1) or _event_days(path, next_year, 2)
            if pre:
                first = pre[0][0]
            else:
                nxt = _season_types(path, next_year)
                first = _et_day((nxt.get(1) or nxt.get(2) or {}).get("start") or "")
            return {"resume_on": (first - timedelta(days=1)).isoformat() if first else ""}

        if current in (2, 5):
            return {"active": True, "reason": f"{year} regular season / play-in"}
        if current == 1:
            info = resume_info(year)
            if info["resume_on"] and today.isoformat() < info["resume_on"]:
                return {"active": False, "reason": f"{year} preseason not started", **info}
            return {"active": True, "reason": f"{year} preseason"}
        if current == 3:
            post = _event_days(path, year, 3)
            if post:
                last_day, last_ref = post[-1]
                if last_day < today and _event_completed(last_ref):
                    return {
                        "active": False,
                        "reason": f"{year} championship final {last_day.isoformat()}",
                        "off_from": (last_day + timedelta(days=1)).isoformat(),
                        **resume_info(year + 1),
                    }
            return {"active": True, "reason": f"{year} postseason in progress"}
        if current == 4:
            return {"active": False, "reason": f"{year} off-season", **resume_info(year + 1)}
    return {"active": None, "reason": "no ESPN season window covers today"}


def calendar(*, refresh: bool = False) -> dict[str, Any]:
    """All leagues, cached for ~20h in data/cache/season_calendar.json."""
    today = datetime.now(ET).date().isoformat()
    if not refresh and CACHE.is_file():
        try:
            cached = json.loads(CACHE.read_text(encoding="utf-8"))
            if cached.get("date") == today and time.time() - float(cached.get("at") or 0) < _CACHE_TTL_SEC:
                return cached
        except (ValueError, OSError):
            pass
    leagues = {}
    for lg in LEAGUE_PATH:
        try:
            leagues[lg] = league_status(lg)
        except Exception as exc:  # network / schema drift: unknown, never flip a sport on a failure
            leagues[lg] = {"active": None, "reason": f"error: {exc}"}
    out = {"date": today, "at": time.time(), "leagues": leagues}
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out


def board_off_season(key: str) -> str | None:
    """Reason string when the calendar says this board's league is off, else None."""
    league = BOARD_LEAGUE.get(str(key or "").strip().upper())
    if not league:
        return None
    st = (calendar().get("leagues") or {}).get(league) or {}
    return st.get("reason") if st.get("active") is False else None


if __name__ == "__main__":
    print(json.dumps(calendar(refresh=True), indent=2))
