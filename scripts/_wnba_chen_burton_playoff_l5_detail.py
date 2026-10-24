"""Print Chen/Burton L5 playoff lines with ESPN opponent, score, and true game date."""
from __future__ import annotations

import json
import sqlite3
import time
import urllib.request
from pathlib import Path

DB = Path(r"H:\PropORACLE\data\cache\proporacle_ref.db")
SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/summary?event={event_id}"
SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates={yyyymmdd}"


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 PropORACLE"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _event_meta(event_id: str) -> dict:
    s = _get(SUMMARY.format(event_id=event_id))
    header = s.get("header") or {}
    comps = header.get("competitions") or [{}]
    comp = comps[0] if comps else {}
    # ESPN game date (UTC ISO) vs local display
    iso = str(comp.get("date") or header.get("season") or s.get("gameInfo", {}).get("venue", {}) or "")
    game_iso = str(comp.get("date") or "")
    status = ((comp.get("status") or {}).get("type") or {}).get("description") or ""
    home = away = ""
    home_score = away_score = None
    for c in comp.get("competitors") or []:
        abbr = ((c.get("team") or {}).get("abbreviation") or "").upper()
        score = c.get("score")
        try:
            score_f = float(score) if score not in (None, "") else None
        except (TypeError, ValueError):
            score_f = None
        if c.get("homeAway") == "home":
            home, home_score = abbr, score_f
        else:
            away, away_score = abbr, score_f
    # Prefer competition date; also try boxscore game date fields
    local_date = ""
    # ESPN often has season.year / week; use date field converted
    if game_iso:
        # Keep calendar date in US/Pacific-ish by using the ISO date part;
        # also fetch scoreboard around that day for local slate date.
        local_date = game_iso[:10]
    return {
        "event_id": str(event_id),
        "iso": game_iso,
        "date_utc": local_date,
        "away": away,
        "home": home,
        "away_score": away_score,
        "home_score": home_score,
        "status": status,
        "name": str(header.get("name") or header.get("shortName") or ""),
    }


def _scoreboard_label(ymd: str, event_id: str) -> dict | None:
    """Find event on scoreboard day; returns local slate date from that board."""
    try:
        data = _get(SCOREBOARD.format(yyyymmdd=ymd.replace("-", "")))
    except Exception:
        return None
    for ev in data.get("events") or []:
        if str(ev.get("id") or "") != str(event_id):
            continue
        comps = ev.get("competitions") or [{}]
        comp = comps[0] if comps else {}
        home = away = ""
        hs = aws = None
        for c in comp.get("competitors") or []:
            abbr = ((c.get("team") or {}).get("abbreviation") or "").upper()
            try:
                sc = float(c.get("score")) if c.get("score") not in (None, "") else None
            except (TypeError, ValueError):
                sc = None
            if c.get("homeAway") == "home":
                home, hs = abbr, sc
            else:
                away, aws = abbr, sc
        return {
            "board_date": ymd,
            "event_date": str(ev.get("date") or "")[:10],
            "iso": str(ev.get("date") or ""),
            "away": away,
            "home": home,
            "away_score": aws,
            "home_score": hs,
            "name": str(ev.get("name") or ev.get("shortName") or ""),
            "status": ((ev.get("status") or {}).get("type") or {}).get("description") or "",
        }
    return None


def player_l5(con: sqlite3.Connection, player: str) -> list[dict]:
    cur = con.execute(
        "SELECT game_date, event_id, team, minutes, pts, ast, pa "
        "FROM wnba WHERE lower(player)=? AND game_date>='2026-09-01' "
        "AND COALESCE(minutes,0)>0 "
        "ORDER BY game_date DESC, event_id DESC LIMIT 8",
        (player.lower(),),
    )
    cols = ["game_date", "event_id", "team", "minutes", "pts", "ast", "pa"]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> None:
    con = sqlite3.connect(str(DB))
    for player, prop, side, line in (
        ("Kaitlyn Chen", "Points", "OVER", 4.5),
        ("Veronica Burton", "Pts+Asts", "UNDER", 13.5),
    ):
        rows = player_l5(con, player)[:5]
        print(f"\n=== {player} | {prop} {side} {line} (playoff L5) ===")
        for i, r in enumerate(rows, 1):
            eid = str(r["event_id"])
            meta = _event_meta(eid)
            time.sleep(0.2)
            # Probe board dates around stored date and UTC date (±1 day)
            from datetime import date, timedelta

            anchors = set()
            for raw in (r["game_date"], meta.get("date_utc"), meta.get("iso", "")[:10]):
                if not raw:
                    continue
                try:
                    d0 = date.fromisoformat(str(raw)[:10])
                except ValueError:
                    continue
                for delta in (-1, 0, 1):
                    anchors.add((d0 + timedelta(days=delta)).isoformat())
            board_hits = []
            for ymd in sorted(anchors):
                hit = _scoreboard_label(ymd, eid)
                if hit:
                    board_hits.append(hit)
                time.sleep(0.08)
            # Dedupe
            seen = set()
            uniq = []
            for h in board_hits:
                key = (h["board_date"], h.get("iso"))
                if key in seen:
                    continue
                seen.add(key)
                uniq.append(h)

            val = r["pts"] if prop == "Points" else (r["pa"] if r["pa"] is not None else (r["pts"] or 0) + (r["ast"] or 0))
            clear = (val > line) if side == "OVER" else (val < line)
            score = ""
            if meta["away_score"] is not None and meta["home_score"] is not None:
                score = f"{meta['away']} {int(meta['away_score'])}-{int(meta['home_score'])} {meta['home']}"
            print(
                f"  {i}. DB_date={r['game_date']}  event={eid}  team={r['team']}  "
                f"min={r['minutes']:g}  {prop[:6]}={val:g}  {'CLEAR' if clear else 'MISS'}"
            )
            print(
                f"     ESPN summary: {meta['away']}@{meta['home']}  score={score or 'n/a'}  "
                f"iso={meta['iso']}  utc_date={meta['date_utc']}  status={meta['status']}"
            )
            print(f"     header: {meta['name']}")
            if uniq:
                for h in uniq:
                    sc = ""
                    if h["away_score"] is not None and h["home_score"] is not None:
                        sc = f"{h['away']} {int(h['away_score'])}-{int(h['home_score'])} {h['home']}"
                    print(
                        f"     scoreboard listed under {h['board_date']}: "
                        f"event_date={h['event_date']} iso={h['iso']}  {sc}  ({h['name']})"
                    )
            else:
                print("     scoreboard: no hit in ±1d window")

        # Resolve notes for known mismatches in this L5
        print("\n  Date resolution notes for this L5:")
        for r in rows:
            eid = str(r["event_id"])
            meta = _event_meta(eid)
            time.sleep(0.15)
            iso = meta.get("iso") or ""
            # If tip is late evening US Pacific previous calendar UTC day...
            print(
                f"    event {eid}: stored DB game_date={r['game_date']}; "
                f"ESPN competition date ISO={iso}"
            )

    # Explicit resolve for Finals G2 and SF date confusion
    print("\n=== Resolve contested dates (GS playoff events) ===")
    # Pull GS events in early/mid Oct from DB
    cur = con.execute(
        "SELECT DISTINCT game_date, event_id FROM wnba "
        "WHERE game_date BETWEEN '2026-10-01' AND '2026-10-21' "
        "AND team IN ('GS','ATL','LV') "
        "ORDER BY game_date, event_id"
    )
    events = cur.fetchall()
    seen_e = set()
    for gdate, eid in events:
        if eid in seen_e:
            continue
        seen_e.add(eid)
        meta = _event_meta(str(eid))
        time.sleep(0.2)
        # Check which scoreboard days list it
        from datetime import date, timedelta

        d0 = date.fromisoformat(str(gdate)[:10])
        boards = []
        for delta in (-1, 0, 1):
            ymd = (d0 + timedelta(days=delta)).isoformat()
            hit = _scoreboard_label(ymd, str(eid))
            if hit:
                boards.append(ymd)
            time.sleep(0.08)
        score = ""
        if meta["away_score"] is not None and meta["home_score"] is not None:
            score = f"{meta['away']} {int(meta['away_score'])}-{int(meta['home_score'])} {meta['home']}"
        print(
            f"  DB={gdate} event={eid}  {meta['away']}@{meta['home']}  {score}  "
            f"ISO={meta['iso']}  on_scoreboards={boards}  name={meta['name']}"
        )

    con.close()


if __name__ == "__main__":
    main()
