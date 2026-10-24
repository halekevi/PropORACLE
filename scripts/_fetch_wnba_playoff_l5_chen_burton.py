"""Fetch Oct 2026 WNBA playoff boxes for Chen/Burton and re-check L5=5."""
from __future__ import annotations

import json
import time
import urllib.request
from datetime import date, timedelta

SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates={yyyymmdd}"
SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/summary?event={event_id}"

TARGETS = {
    "kaitlyn chen": "pts",
    "veronica burton": "pa",  # pts+ast
}


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _parse_box(summary: dict, game_date: str, event_id: str) -> list[dict]:
    rows = []
    box = (summary or {}).get("boxscore") or {}
    players = box.get("players") or []
    comps = summary.get("header", {}).get("competitions", [{}])
    comp = comps[0] if comps else {}
    teams = {}
    for c in comp.get("competitors") or []:
        teams[c.get("homeAway")] = (c.get("team") or {}).get("abbreviation") or ""
    home, away = teams.get("home", ""), teams.get("away", "")
    for side in players:
        team = ((side.get("team") or {}).get("abbreviation") or "").upper()
        for stat_group in side.get("statistics") or []:
            keys = [str(k).lower() for k in (stat_group.get("keys") or [])]
            names = [str(n).lower() for n in (stat_group.get("names") or [])]
            labels = keys or names
            for athlete in stat_group.get("athletes") or []:
                a = athlete.get("athlete") or {}
                name = str(a.get("displayName") or "").strip()
                if not name:
                    continue
                stats = athlete.get("stats") or []
                # map by label index
                d = {labels[i]: stats[i] for i in range(min(len(labels), len(stats)))}
                # common ESPN keys: MIN, PTS, REB, AST
                def _num(*cands):
                    for c in cands:
                        for k, v in d.items():
                            if c in k.replace(" ", ""):
                                try:
                                    return float(str(v).replace("--", "0") or 0)
                                except ValueError:
                                    return 0.0
                    return 0.0

                mins = _num("min", "minutes")
                pts = _num("pts", "points")
                reb = _num("reb", "rebounds")
                ast = _num("ast", "assists")
                rows.append(
                    {
                        "game_date": game_date,
                        "event_id": event_id,
                        "player": name,
                        "team": team,
                        "home": home,
                        "away": away,
                        "minutes": mins,
                        "pts": pts,
                        "reb": reb,
                        "ast": ast,
                        "pa": pts + ast,
                    }
                )
    return rows


def main():
    # Sep 1 through Oct 21 2026
    start = date(2026, 9, 1)
    end = date(2026, 10, 21)
    all_rows: list[dict] = []
    d = start
    events_seen = set()
    while d <= end:
        ymd = d.strftime("%Y%m%d")
        try:
            data = _get(SCOREBOARD.format(yyyymmdd=ymd))
        except Exception as e:
            print(f"{ymd} scoreboard fail: {e}")
            d += timedelta(days=1)
            continue
        events = data.get("events") or []
        if events:
            print(f"{ymd}: {len(events)} events")
        for ev in events:
            eid = str(ev.get("id") or "")
            if not eid or eid in events_seen:
                continue
            # only final/completed
            status = ((ev.get("status") or {}).get("type") or {}).get("name") or ""
            if status not in {"STATUS_FINAL", "STATUS_FULL_TIME"} and "final" not in status.lower():
                # still try — playoff games
                pass
            events_seen.add(eid)
            gdate = str(ev.get("date") or "")[:10] or d.isoformat()
            try:
                summary = _get(SUMMARY.format(event_id=eid))
                rows = _parse_box(summary, gdate, eid)
                all_rows.extend(rows)
                print(f"  event {eid} {gdate}: {len(rows)} player rows")
            except Exception as e:
                print(f"  event {eid} fail: {e}")
            time.sleep(0.15)
        d += timedelta(days=1)
        time.sleep(0.05)

    print(f"\nTotal player-rows={len(all_rows)} events={len(events_seen)}")

    # Filter to targets
    for pname, prop in TARGETS.items():
        mine = [r for r in all_rows if r["player"].lower() == pname and (r["minutes"] or 0) > 0]
        mine.sort(key=lambda r: r["game_date"], reverse=True)
        print(f"\n=== {pname.title()} playoff/Sep-Oct boxes n={len(mine)} ===")
        for r in mine[:12]:
            print(
                f"  {r['game_date']} {r['away']}@{r['home']} team={r['team']} "
                f"min={r['minutes']:.0f} pts={r['pts']:.0f} ast={r['ast']:.0f} pa={r['pa']:.0f}"
            )
        if len(mine) < 5:
            print("  INCOMPLETE L5 — fewer than 5 games in Sep–Oct fetch")
            continue
        l5 = mine[:5]
        if prop == "pts":
            vals = [r["pts"] for r in l5]
            line, side = 4.5, "OVER"
            label = "Points"
        else:
            vals = [r["pa"] for r in l5]
            line, side = 13.5, "UNDER"
            label = "Pts+Asts"
        dates = [r["game_date"] for r in l5]
        mins = [r["minutes"] for r in l5]
        if side == "OVER":
            clears = [v > line for v in vals]
        else:
            clears = [v < line for v in vals]
        print(f"  L5 {label} {side} {line}:")
        for i in range(5):
            mark = "CLEAR" if clears[i] else "MISS"
            print(f"    {dates[i]} min={mins[i]:.0f} val={vals[i]:g} {mark}")
        print(f"  L5=5={all(clears)}  hits={sum(clears)}/5  avg={sum(vals)/5:.2f}")


if __name__ == "__main__":
    main()
