#!/usr/bin/env python3
"""Backfill final scores plus open/close spread, total and moneyline (with prices) from ESPN.

One row per finished game in ``data/cache/game_lines_history.db`` (table ``game_history``).
Scores come from the ESPN site scoreboard; lines come from the ESPN core odds endpoint,
which keeps the open and close numbers after the game ends. Re-runs skip games already stored
with odds unless ``--refresh`` is passed.

    py -3.14 scripts/game_lines/backfill_game_lines_history.py --sports NBA NFL CFB NHL --start 2024-09-01
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[2]
DB_PATH = REPO / "data" / "cache" / "game_lines_history.db"
ET = ZoneInfo("America/New_York")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"

# sport -> (espn sport, espn league, scoreboard group or None)
SPORTS: dict[str, tuple[str, str, str | None]] = {
    "NBA": ("basketball", "nba", None),
    "NFL": ("football", "nfl", None),
    "CFB": ("football", "college-football", "80"),
    "NHL": ("hockey", "nhl", None),
    "CBB": ("basketball", "mens-college-basketball", "50"),
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS game_history (
    sport TEXT NOT NULL,
    event_id TEXT NOT NULL,
    game_date TEXT NOT NULL,
    start_utc TEXT,
    season_year INTEGER,
    season_type INTEGER,
    neutral_site INTEGER,
    home TEXT, away TEXT, home_id TEXT, away_id TEXT,
    home_score REAL, away_score REAL,
    provider TEXT,
    spread_open REAL, spread_close REAL,
    home_spread_price_close REAL, away_spread_price_close REAL,
    total_open REAL, total_close REAL,
    over_price_close REAL, under_price_close REAL,
    home_ml_open REAL, away_ml_open REAL,
    home_ml_close REAL, away_ml_close REAL,
    odds_status TEXT,
    fetched_at TEXT,
    PRIMARY KEY (sport, event_id)
);
CREATE INDEX IF NOT EXISTS ix_gh_sport_date ON game_history(sport, game_date);
"""


def _get(url: str, retries: int = 3) -> dict[str, Any] | None:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            time.sleep(1.5 * (attempt + 1))
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return None


def _num(raw: Any) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if s in ("", "OFF", "N/A", "NONE"):
        return None
    if s in ("EVEN", "EV", "PK", "PICK"):
        return 100.0 if s in ("EVEN", "EV") else 0.0
    try:
        return float(s.replace("+", ""))
    except ValueError:
        return None


def _american(block: Any) -> float | None:
    if not isinstance(block, dict):
        return None
    return _num(block.get("american") or block.get("alternateDisplayValue"))


def _pick_item(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pre-game book line: skip live-odds providers, prefer the lowest priority number."""
    pre = [i for i in items if "live" not in str((i.get("provider") or {}).get("name", "")).lower()]
    pool = pre or items
    if not pool:
        return None
    return sorted(pool, key=lambda i: int((i.get("provider") or {}).get("priority") or 99))[0]


def parse_odds(item: dict[str, Any]) -> dict[str, Any]:
    h = item.get("homeTeamOdds") or {}
    a = item.get("awayTeamOdds") or {}
    ho, hc = h.get("open") or {}, h.get("close") or {}
    ao, ac = a.get("open") or {}, a.get("close") or {}
    to, tc = item.get("open") or {}, item.get("close") or {}
    spread_close = _american(hc.get("pointSpread"))
    if spread_close is None:
        spread_close = _num(item.get("spread"))
    total_close = _american(tc.get("total"))
    if total_close is None:
        total_close = _num(item.get("overUnder"))
    return {
        "provider": (item.get("provider") or {}).get("name"),
        "spread_open": _american(ho.get("pointSpread")),
        "spread_close": spread_close,
        "home_spread_price_close": _american(hc.get("spread")) or _num(h.get("spreadOdds")),
        "away_spread_price_close": _american(ac.get("spread")) or _num(a.get("spreadOdds")),
        "total_open": _american(to.get("total")),
        "total_close": total_close,
        "over_price_close": _american(tc.get("over")) or _num(item.get("overOdds")),
        "under_price_close": _american(tc.get("under")) or _num(item.get("underOdds")),
        "home_ml_open": _american(ho.get("moneyLine")),
        "away_ml_open": _american(ao.get("moneyLine")),
        "home_ml_close": _american(hc.get("moneyLine")) or _num(h.get("moneyLine")),
        "away_ml_close": _american(ac.get("moneyLine")) or _num(a.get("moneyLine")),
    }


def scoreboard_games(sport: str, day: date) -> list[dict[str, Any]]:
    sp, lg, group = SPORTS[sport]
    url = (f"https://site.api.espn.com/apis/site/v2/sports/{sp}/{lg}/scoreboard"
           f"?dates={day:%Y%m%d}&limit=500")
    if group:
        url += f"&groups={group}"
    payload = _get(url) or {}
    out: list[dict[str, Any]] = []
    for ev in payload.get("events", []):
        comp = (ev.get("competitions") or [{}])[0]
        status = ((comp.get("status") or {}).get("type") or {})
        if not status.get("completed"):
            continue
        teams = {c.get("homeAway"): c for c in comp.get("competitors", [])}
        home, away = teams.get("home"), teams.get("away")
        if not home or not away:
            continue
        start = ev.get("date") or ""
        try:
            gd = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(ET).date().isoformat()
        except ValueError:
            gd = day.isoformat()
        season = ev.get("season") or {}
        out.append({
            "sport": sport,
            "event_id": str(ev.get("id")),
            "game_date": gd,
            "start_utc": start,
            "season_year": season.get("year"),
            "season_type": season.get("type"),
            "neutral_site": 1 if comp.get("neutralSite") else 0,
            "home": (home.get("team") or {}).get("abbreviation"),
            "away": (away.get("team") or {}).get("abbreviation"),
            "home_id": str((home.get("team") or {}).get("id") or ""),
            "away_id": str((away.get("team") or {}).get("id") or ""),
            "home_score": _num(home.get("score")),
            "away_score": _num(away.get("score")),
        })
    return out


def fetch_odds(sport: str, event_id: str) -> tuple[dict[str, Any], str]:
    sp, lg, _ = SPORTS[sport]
    url = (f"https://sports.core.api.espn.com/v2/sports/{sp}/leagues/{lg}"
           f"/events/{event_id}/competitions/{event_id}/odds")
    payload = _get(url)
    if payload is None:
        return {}, "no_odds"
    item = _pick_item(payload.get("items") or [])
    if not item:
        return {}, "no_odds"
    odds = parse_odds(item)
    if odds["spread_close"] is None and odds["total_close"] is None and odds["home_ml_close"] is None:
        return odds, "empty"
    return odds, "ok"


def upsert(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    cols = list(row.keys())
    conn.execute(
        f"INSERT OR REPLACE INTO game_history ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
        [row[c] for c in cols],
    )


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sports", nargs="+", default=["NBA", "NFL", "CFB", "NHL"], choices=sorted(SPORTS))
    ap.add_argument("--start", default="2024-09-01")
    ap.add_argument("--end", default="", help="default: yesterday (ET)")
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else datetime.now(ET).date() - timedelta(days=1)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)

    for sport in args.sports:
        have = {r[0] for r in conn.execute(
            "SELECT event_id FROM game_history WHERE sport=? AND odds_status='ok'", (sport,))}
        days = list(daterange(start, end))
        games: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for fut in as_completed([ex.submit(scoreboard_games, sport, d) for d in days]):
                games.extend(fut.result())
        uniq = {g["event_id"]: g for g in games}
        todo = [g for g in uniq.values() if args.refresh or g["event_id"] not in have]
        print(f"[{sport}] {len(uniq)} finished games {start}..{end}; fetching odds for {len(todo)}", flush=True)

        stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        status_counts: dict[str, int] = {}
        done = 0
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(fetch_odds, sport, g["event_id"]): g for g in todo}
            for fut in as_completed(futs):
                g = futs[fut]
                odds, status = fut.result()
                row = dict(g)
                for k in ("provider", "spread_open", "spread_close", "home_spread_price_close",
                          "away_spread_price_close", "total_open", "total_close", "over_price_close",
                          "under_price_close", "home_ml_open", "away_ml_open", "home_ml_close", "away_ml_close"):
                    row[k] = odds.get(k)
                row["odds_status"] = status
                row["fetched_at"] = stamp
                upsert(conn, row)
                status_counts[status] = status_counts.get(status, 0) + 1
                done += 1
                if done % 500 == 0:
                    conn.commit()
                    print(f"  [{sport}] {done}/{len(todo)} {status_counts}", flush=True)
        conn.commit()
        total = conn.execute("SELECT COUNT(*), SUM(odds_status='ok') FROM game_history WHERE sport=?",
                             (sport,)).fetchone()
        print(f"[{sport}] stored {total[0]} games, {total[1]} with odds; this run {status_counts}", flush=True)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
