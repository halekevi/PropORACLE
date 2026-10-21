"""Thursday-Saturday FBS scoreboard with spread and total."""
from __future__ import annotations

import json
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
DATES = ["20261015", "20261016", "20261017"]


def get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "PropORACLE/1.0"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    rows = []
    for d in DATES:
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
            f"?dates={d}&limit=500&groups=80"
        )
        data = get(url)
        for ev in data.get("events") or []:
            comp = (ev.get("competitions") or [{}])[0]
            teams = {}
            for c in comp.get("competitors") or []:
                t = c.get("team") or {}
                teams[c.get("homeAway")] = {
                    "id": str(t.get("id") or ""),
                    "abbr": t.get("abbreviation") or "",
                    "name": t.get("shortDisplayName") or t.get("displayName") or "",
                }
            raw = ev.get("date") or ""
            try:
                when = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(ET)
                kick = when.strftime("%a %m/%d %I:%M %p")
            except Exception:
                kick = raw
            odds = (comp.get("odds") or [None])[0] or {}
            home_odds = odds.get("homeTeamOdds") or {}
            away_odds = odds.get("awayTeamOdds") or {}
            rows.append(
                {
                    "kick": kick,
                    "away": (teams.get("away") or {}).get("abbr"),
                    "away_id": (teams.get("away") or {}).get("id"),
                    "away_name": (teams.get("away") or {}).get("name"),
                    "home": (teams.get("home") or {}).get("abbr"),
                    "home_id": (teams.get("home") or {}).get("id"),
                    "home_name": (teams.get("home") or {}).get("name"),
                    "spread": odds.get("spread"),
                    "details": odds.get("details"),
                    "total": odds.get("overUnder"),
                    "home_spread": (home_odds.get("spread") if isinstance(home_odds, dict) else None),
                    "provider": ((odds.get("provider") or {}).get("name")),
                }
            )
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()
