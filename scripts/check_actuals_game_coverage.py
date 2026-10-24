"""Alert when a final scoreboard game has no players in that day's actuals file.

Does not write the actuals file. A game is missing when neither of its teams
appears in the file.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from fetch_football_actuals import _espn_get_json, _scoreboard_urls  # noqa: E402
from utils.eastern_slate_date import event_start_raw, keep_event_on_slate  # noqa: E402

LEAGUE_PATH = {
    "nfl": "football/nfl",
    "cfb": "football/college-football",
}


def _teams(event: dict) -> list[str]:
    comps = event.get("competitions") or []
    comp = comps[0] if comps and isinstance(comps[0], dict) else {}
    out = []
    for competitor in comp.get("competitors") or []:
        team = (competitor or {}).get("team") or {}
        abbr = str(team.get("abbreviation") or "").strip().upper()
        if abbr:
            out.append(abbr)
    return out


def _final(event: dict) -> bool:
    status = (event.get("status") or {}).get("type") or {}
    return str(status.get("state") or "") == "post" or bool(status.get("completed"))


def missing_games(league: str, slate_date: str, actuals: Path) -> list[str]:
    path = LEAGUE_PATH[league]
    query = [
        slate_date,
        (date.fromisoformat(slate_date) + timedelta(days=1)).isoformat(),
    ]
    have: set[str] = set()
    if actuals.is_file():
        df = pd.read_csv(actuals, dtype=str).fillna("")
        if "team" in df.columns:
            have = {str(t).strip().upper() for t in df["team"] if str(t).strip()}
    missing: list[str] = []
    seen: set[str] = set()
    for qday in query:
        payload = _espn_get_json(_scoreboard_urls(path, qday)) or {}
        for event in payload.get("events") or []:
            if not keep_event_on_slate(event_start_raw(event), slate_date, qday):
                continue
            if not _final(event):
                continue
            eid = str(event.get("id") or "")
            if eid and eid in seen:
                continue
            if eid:
                seen.add(eid)
            teams = _teams(event)
            if teams and not any(t in have for t in teams):
                name = str(event.get("shortName") or eid)
                missing.append(f"{name} ({', '.join(teams)})")
    return missing


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True, choices=sorted(LEAGUE_PATH))
    ap.add_argument("--date", required=True)
    ap.add_argument("--actuals", required=True)
    args = ap.parse_args()
    slate = str(args.date)[:10]
    missing = missing_games(args.league, slate, Path(args.actuals))
    if not missing:
        print(f"[coverage] {args.league} {slate}: every final scoreboard game has a team in the actuals file")
        return
    print(f"ALERT [coverage] {args.league} {slate}: {len(missing)} final game(s) missing from actuals")
    for line in missing:
        print(f"  {line}")


if __name__ == "__main__":
    main()
