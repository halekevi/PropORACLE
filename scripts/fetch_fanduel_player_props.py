#!/usr/bin/env python3
"""
Fetch FanDuel sportsbook player-prop ladders (mains + milestones / alts) via sbapi.

Uses the public NJ sbapi event-page tabs with usePlayerPropsVirtualMarket=true.
No Odds API.

Examples:
  py -3.14 scripts/fetch_fanduel_player_props.py --sports nba,nfl \\
      --output outputs/DATE/books/fanduel_props_ladder.csv --slate-date DATE
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRIPT_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.book_line_ladder import LADDER_COLUMNS, normalize_ladder, utc_now_iso  # noqa: E402

try:
    from curl_cffi.requests import Session as CurlSession

    def _new_session():
        return CurlSession(impersonate="chrome136")

except Exception:  # pragma: no cover
    import requests

    def _new_session():
        return requests.Session()


SBAPI = "https://sbapi.nj.sportsbook.fanduel.com/api"
FD_P = {
    "betexRegion": "GBR",
    "capiJurisdiction": "intl",
    "currencyCode": "USD",
    "exchangeLocale": "en_US",
    "includePrices": "true",
    "language": "en",
    "regionCode": "NAMERICA",
    "usePlayerPropsVirtualMarket": "true",
    "_ak": "FhMFpcPWXMeyZxOx",
}

SPORT_TABS: Dict[str, List[str]] = {
    "nba": ["player-points", "player-rebounds", "player-assists", "player-combos", "player-threes"],
    "nfl": ["passing-props", "receiving-props", "rushing-props"],
    "nhl": ["player-props"],
}

MILESTONE_PROP = [
    (r"PTS_\+_REB_\+_AST", "Pts+Rebs+Asts"),
    (r"PTS_\+_REB", "Pts+Rebs"),
    (r"PTS_\+_AST", "Pts+Asts"),
    (r"REB_\+_AST", "Rebs+Asts"),
    (r"POINTS", "Points"),
    (r"REBOUNDS", "Rebounds"),
    (r"ASSISTS", "Assists"),
    (r"MADE_THREES", "3-PT Made"),
]

OU_PROP = [
    (r"PTS_\+_REB_\+_AST|POINTS_\+_REBOUNDS_\+_ASSISTS|PRA", "Pts+Rebs+Asts"),
    (r"PTS_\+_REB|POINTS_\+_REBOUNDS", "Pts+Rebs"),
    (r"PTS_\+_AST|POINTS_\+_ASSISTS", "Pts+Asts"),
    (r"REB_\+_AST|REBOUNDS_\+_ASSISTS", "Rebs+Asts"),
    (r"3_POINT|THREES", "3-PT Made"),
    (r"TOTAL_POINTS", "Points"),
    (r"TOTAL_REBOUNDS", "Rebounds"),
    (r"TOTAL_ASSISTS", "Assists"),
    (r"PASSING_YARDS", "Pass Yards"),
    (r"RECEIVING_YARDS", "Receiving Yards"),
    (r"RECEPTIONS", "Receptions"),
    (r"RUSHING_YARDS", "Rush Yards"),
    (r"RUSHING_ATTEMPT", "Rush Attempts"),
]

NFL_ALT_RUNNER = re.compile(
    r"^(?P<player>.+?)\s+(?P<n>\d+)\+\s+(?P<what>Yards|Receptions)$",
    re.I,
)


def _fd_odds(runner: dict) -> Any:
    o = (runner.get("winRunnerOdds") or {}).get("americanDisplayOdds") or {}
    return o.get("americanOdds")


def list_events(session, sport: str) -> List[Tuple[str, str]]:
    r = session.get(
        f"{SBAPI}/content-managed-page",
        params={**FD_P, "page": "CUSTOM", "customPageId": sport},
        headers={
            "Accept": "application/json",
            "Origin": "https://sportsbook.fanduel.com",
            "Referer": "https://sportsbook.fanduel.com/",
        },
        timeout=45,
    )
    r.raise_for_status()
    ev = (r.json().get("attachments") or {}).get("events") or {}
    out = []
    for k, v in ev.items():
        name = str((v or {}).get("name") or "")
        if "@" in name and "(" not in name:
            out.append((str(k), name))
    return out


def parse_markets(sport: str, markets: dict) -> List[dict]:
    rows: List[dict] = []
    sport_u = sport.upper()
    for m in (markets or {}).values():
        if not isinstance(m, dict):
            continue
        mt = str(m.get("marketType") or "")
        if "QUARTER" in mt or "HALF" in mt or "ODD/EVEN" in mt:
            continue
        runners = m.get("runners") or []
        mm = re.match(r"^(?:TO_SCORE_|TO_RECORD_)?(\d+)\+_(.+)$", mt)
        if sport == "nba" and mm:
            n = int(mm.group(1))
            rest = mm.group(2)
            prop = next((p for rx, p in MILESTONE_PROP if re.search(rx, rest)), None)
            if not prop:
                continue
            for rn in runners:
                if not isinstance(rn, dict):
                    continue
                rows.append(
                    {
                        "sport": sport_u,
                        "player": rn.get("runnerName"),
                        "prop": prop,
                        "line": n - 0.5,
                        "american_odds": _fd_odds(rn),
                        "kind": "alt",
                        "side": "OVER",
                        "source_event_id": "",
                    }
                )
            continue
        if "_ALT_" in mt:
            for rn in runners:
                if not isinstance(rn, dict):
                    continue
                g = NFL_ALT_RUNNER.match(str(rn.get("runnerName") or ""))
                if not g:
                    continue
                what = g.group("what").lower()
                if what == "receptions":
                    prop = "Receptions"
                elif "RUSHING" in mt:
                    prop = "Rush Yards"
                elif "PASSING" in mt:
                    prop = "Pass Yards"
                else:
                    prop = "Receiving Yards"
                rows.append(
                    {
                        "sport": sport_u,
                        "player": g.group("player"),
                        "prop": prop,
                        "line": int(g.group("n")) - 0.5,
                        "american_odds": _fd_odds(rn),
                        "kind": "alt",
                        "side": "OVER",
                        "source_event_id": "",
                    }
                )
            continue
        prop = next((p for rx, p in OU_PROP if re.search(rx, mt)), None)
        if not prop:
            continue
        for rn in runners:
            if not isinstance(rn, dict):
                continue
            nm = str(rn.get("runnerName") or "")
            if not nm.lower().endswith(" over"):
                continue
            try:
                ln = float(rn.get("handicap"))
            except (TypeError, ValueError):
                continue
            if ln <= 0:
                continue
            kind = "alt" if re.search(r"_(LOW|HIGH)$", mt) else "main"
            rows.append(
                {
                    "sport": sport_u,
                    "player": nm[:-5].strip(),
                    "prop": prop,
                    "line": ln,
                    "american_odds": _fd_odds(rn),
                    "kind": kind,
                    "side": "OVER",
                    "source_event_id": "",
                }
            )
    return rows


def fetch_sport(session, sport: str, *, sleep_s: float = 0.1) -> List[dict]:
    tabs = SPORT_TABS.get(sport) or ["player-props"]
    rows: List[dict] = []
    events = list_events(session, sport)
    print(f"[fanduel] {sport} games={len(events)}")
    for eid, name in events:
        for tab in tabs:
            try:
                r = session.get(
                    f"{SBAPI}/event-page",
                    params={**FD_P, "eventId": eid, "tab": tab},
                    headers={
                        "Accept": "application/json",
                        "Origin": "https://sportsbook.fanduel.com",
                        "Referer": "https://sportsbook.fanduel.com/",
                    },
                    timeout=45,
                )
            except Exception as e:
                print(f"  [fanduel] WARN {sport} {eid} {tab}: {e}")
                continue
            if r.status_code != 200:
                continue
            mk = (r.json().get("attachments") or {}).get("markets") or {}
            part = parse_markets(sport, mk)
            for row in part:
                row["source_event_id"] = eid
            rows.extend(part)
            time.sleep(sleep_s)
        print(f"  [fanduel] {name[:48]!r} cumulative_rungs={len(rows)}")
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch FanDuel player-prop ladders")
    ap.add_argument("--sports", default="nba,nfl", help="Comma list: nba,nfl,nhl")
    ap.add_argument("--output", required=True, help="Ladder CSV path")
    ap.add_argument("--slate-date", default="", help="YYYY-MM-DD stamped on rows")
    ap.add_argument("--min-rows", type=int, default=0)
    args = ap.parse_args()

    sports = [s.strip().lower() for s in args.sports.split(",") if s.strip()]
    fetched_at = utc_now_iso()
    slate_date = (args.slate_date or fetched_at[:10]).strip()
    session = _new_session()
    all_rows: List[dict] = []
    for sport in sports:
        try:
            all_rows.extend(fetch_sport(session, sport))
        except Exception as e:
            print(f"[fanduel] WARN sport {sport}: {e}")

    raw = pd.DataFrame(all_rows)
    ladder = normalize_ladder(raw, book="fanduel", slate_date=slate_date, fetched_at=fetched_at)
    if not ladder.empty:
        ladder = ladder.drop_duplicates(
            subset=["sport", "player", "prop", "line", "side", "kind", "source_event_id"],
            keep="first",
        )
    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    ladder.to_csv(args.output, index=False, encoding="utf-8-sig")
    n_main = int((ladder["kind"] == "main").sum()) if len(ladder) else 0
    print(f"[ok] FanDuel {len(ladder)} rungs ({n_main} main) -> {args.output}")
    if len(ladder) < args.min_rows:
        sys.exit(1)


if __name__ == "__main__":
    main()
