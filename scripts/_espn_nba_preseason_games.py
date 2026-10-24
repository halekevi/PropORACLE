"""List all 2026 NBA preseason games from ESPN scoreboard API."""
from __future__ import annotations

import json
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "reports" / "nba_preseason_espn_2026.txt"

SCOREBOARD = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
    "scoreboard?dates={ymd}&limit=100"
)


def fetch_day(d: date) -> list[dict]:
    url = SCOREBOARD.format(ymd=d.strftime("%Y%m%d"))
    req = urllib.request.Request(url, headers={"User-Agent": "PropORACLE/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    out = []
    for ev in data.get("events") or []:
        comp = (ev.get("competitions") or [{}])[0]
        status = ((comp.get("status") or {}).get("type") or {}).get("description", "")
        season_type = ((ev.get("season") or {}).get("type"))  # 1=pre, 2=reg, 3=post
        teams = {}
        score = {}
        for c in comp.get("competitors") or []:
            ha = c.get("homeAway", "")
            t = (c.get("team") or {}).get("abbreviation") or (
                (c.get("team") or {}).get("shortDisplayName") or "?"
            )
            teams[ha] = t
            score[ha] = c.get("score")
        out.append(
            {
                "date": d.isoformat(),
                "id": ev.get("id"),
                "name": ev.get("shortName") or ev.get("name"),
                "season_type": season_type,
                "status": status,
                "away": teams.get("away"),
                "home": teams.get("home"),
                "away_score": score.get("away"),
                "home_score": score.get("home"),
            }
        )
    return out


def main() -> None:
    # NBA preseason typically early Oct through day before opener.
    # Cover Sep 25 – Oct 21 2026 to catch everything.
    start = date(2026, 9, 25)
    end = date(2026, 10, 21)
    all_games: list[dict] = []
    d = start
    while d <= end:
        try:
            all_games.extend(fetch_day(d))
        except Exception as exc:
            all_games.append({"date": d.isoformat(), "error": str(exc)})
        d += timedelta(days=1)

    pre = [g for g in all_games if g.get("season_type") == 1]
    reg = [g for g in all_games if g.get("season_type") == 2]
    other = [g for g in all_games if g.get("season_type") not in {1, 2, None} and "error" not in g]
    errs = [g for g in all_games if "error" in g]

    lines = [
        f"ESPN NBA scoreboard scan {start} → {end}",
        f"preseason (season.type=1): {len(pre)}",
        f"regular   (season.type=2): {len(reg)}",
        f"other/err: {len(other)+len(errs)}",
        "",
        "===== PRESEASON =====",
    ]
    for g in sorted(pre, key=lambda x: (x["date"], x.get("name") or "")):
        lines.append(
            f"{g['date']}  {g.get('away','?'):>3} @ {g.get('home','?'):<3}  "
            f"{g.get('away_score','-'):>3}-{g.get('home_score','-'):<3}  "
            f"{g.get('status','')}  id={g.get('id')}"
        )

    # CLE focus (Tyson context)
    cle = [
        g
        for g in pre
        if g.get("away") in {"CLE", "CAVS"} or g.get("home") in {"CLE", "CAVS"}
    ]
    lines.append("")
    lines.append(f"===== CLE preseason (n={len(cle)}) =====")
    for g in cle:
        lines.append(
            f"{g['date']}  {g.get('away')} @ {g.get('home')}  "
            f"{g.get('away_score')}-{g.get('home_score')}  {g.get('status')}"
        )

    lines.append("")
    lines.append("===== REGULAR (same window, for contrast) =====")
    for g in sorted(reg, key=lambda x: (x["date"], x.get("name") or "")):
        lines.append(
            f"{g['date']}  {g.get('away','?'):>3} @ {g.get('home','?'):<3}  "
            f"{g.get('away_score','-'):>3}-{g.get('home_score','-'):<3}  "
            f"{g.get('status','')}"
        )

    text = "\n".join(lines) + "\n"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text, encoding="utf-8")
    print(text)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
