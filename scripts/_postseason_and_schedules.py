"""Confirm 2026 MLB and WNBA regular-season ends, and Oct 7 WNBA games."""
from __future__ import annotations

import json
from urllib.request import Request, urlopen

UA = {"User-Agent": "Mozilla/5.0"}


def get(url: str) -> dict:
    req = Request(url, headers=UA)
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    mlb = get(
        "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
        "&startDate=2026-09-20&endDate=2026-10-08&gameType=R,F,D,L,W"
    )
    by = {}
    for d in mlb.get("dates") or []:
        types = sorted({g.get("gameType") for g in d.get("games") or []})
        by[d.get("date")] = (len(d.get("games") or []), types)
    print("MLB")
    for day, (n, types) in sorted(by.items()):
        print(f"  {day} n={n} types={types}")

    for day in ("20260920", "20261007"):
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/"
            f"scoreboard?dates={day}"
        )
        try:
            payload = get(url)
        except Exception as exc:
            print(f"WNBA {day} ERR {exc}")
            continue
        events = payload.get("events") or []
        print(f"WNBA {day} events={len(events)}")
        for ev in events[:12]:
            season = ev.get("season") or {}
            comps = (ev.get("competitions") or [{}])[0]
            names = []
            for c in comps.get("competitors") or []:
                team = c.get("team") or {}
                names.append(team.get("abbreviation") or team.get("displayName"))
            print(
                f"  {ev.get('date')} type={season.get('type')} "
                f"slug={season.get('slug')} {names}"
            )


if __name__ == "__main__":
    main()
