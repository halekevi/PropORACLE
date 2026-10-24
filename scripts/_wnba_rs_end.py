"""Find the last 2026 WNBA regular-season date on the ESPN scoreboard."""
from __future__ import annotations

import json
from datetime import date, timedelta
from urllib.request import Request, urlopen

UA = {"User-Agent": "Mozilla/5.0"}


def main() -> None:
    day = date(2026, 9, 18)
    stop = date(2026, 10, 8)
    while day <= stop:
        stamp = day.strftime("%Y%m%d")
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/"
            f"scoreboard?dates={stamp}"
        )
        req = Request(url, headers=UA)
        with urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        events = payload.get("events") or []
        types = sorted(
            {
                str((ev.get("season") or {}).get("slug") or "")
                for ev in events
            }
        )
        print(f"{day.isoformat()} n={len(events)} {types}")
        day += timedelta(days=1)


if __name__ == "__main__":
    main()
