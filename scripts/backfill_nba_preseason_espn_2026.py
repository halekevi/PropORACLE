"""ONE-TIME 2026 NBA preseason box ingest (done) — not an L5 pad.

Filled root proporacle_ref.db so the early-season ``pre_`` bridge had boxes
to read. Card-window L5 never uses these rows. Do not wire into nightly
pipelines; nightly freshness is ``build_boxscore_ref`` + ``assert_nba_box_fresh``.

1. Scoreboard 2026-09-25..2026-10-21, season.type=1 (67 games expected)
2. Full box per game + starter flag
3. Diff vs root DB → missing games / players / stat mismatches
4. Upsert missing; ESPN wins on conflict
5. Sync Sports/NBA copy
6. Write report JSON/TXT

  py -3.14 scripts/backfill_nba_preseason_espn_2026.py
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "Sports" / "NBA" / "scripts"))

from build_boxscore_ref import (  # noqa: E402
    ESPN_TO_SLATE_ABBREV,
    _parse_bball_boxscore,
    _parse_made_att,
    init_db,
)

ROOT_DB = ROOT / "data" / "cache" / "proporacle_ref.db"
SPORTS_DB = ROOT / "Sports" / "NBA" / "data" / "cache" / "proporacle_ref.db"
REPORT_TXT = ROOT / "data" / "reports" / "nba_preseason_backfill_2026.txt"
REPORT_JSON = ROOT / "data" / "reports" / "nba_preseason_backfill_2026.json"

SCOREBOARD = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
    "scoreboard?dates={ymd}&limit=100"
)
SUMMARY = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
    "summary?event={eid}"
)

COMPARE_COLS = (
    "minutes",
    "pts",
    "reb",
    "ast",
    "stl",
    "blk",
    "tov",
    "fgm",
    "fga",
    "fg3m",
    "fg3a",
    "fg2m",
    "fg2a",
    "ftm",
    "fta",
)


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "PropORACLE/1.0"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read().decode("utf-8"))


def list_preseason_events(start: date, end: date) -> list[dict]:
    out: list[dict] = []
    d = start
    while d <= end:
        data = _get(SCOREBOARD.format(ymd=d.strftime("%Y%m%d")))
        for ev in data.get("events") or []:
            if (ev.get("season") or {}).get("type") != 1:
                continue
            status = ((ev.get("status") or {}).get("type") or {}).get("name", "")
            if status and status not in {"STATUS_FINAL", "STATUS_FULL_TIME"}:
                # also accept completed
                completed = ((ev.get("status") or {}).get("type") or {}).get(
                    "completed", False
                )
                if not completed:
                    continue
            comps = (ev.get("competitions") or [{}])[0]
            teams = {}
            for c in comps.get("competitors") or []:
                teams[c.get("homeAway")] = (
                    (c.get("team") or {}).get("abbreviation") or "?"
                )
            out.append(
                {
                    "date": d.isoformat(),
                    "event_id": str(ev.get("id")),
                    "home": teams.get("home"),
                    "away": teams.get("away"),
                    "name": ev.get("shortName") or ev.get("name"),
                }
            )
        d += timedelta(days=1)
        time.sleep(0.05)
    # dedupe by event_id
    seen = set()
    uniq = []
    for g in out:
        if g["event_id"] in seen:
            continue
        seen.add(g["event_id"])
        uniq.append(g)
    return uniq


def parse_box_with_starter(
    box: dict, event_id: str, game_date: str, home: str, away: str
) -> list[dict]:
    """Same as build_boxscore_ref parse, plus starter flag."""
    rows = _parse_bball_boxscore(box, event_id, game_date, home, away, "NBA")
    # Re-walk athletes to attach starter
    starter_map: dict[tuple[str, str], int] = {}
    for bteam in box.get("boxscore", {}).get("players", []):
        t_raw = bteam.get("team", {}).get("abbreviation", "")
        t_abbr = ESPN_TO_SLATE_ABBREV.get(t_raw, t_raw)
        for sg in bteam.get("statistics", []):
            for ath in sg.get("athletes", []):
                name = ath.get("athlete", {}).get("displayName", "")
                if not name:
                    continue
                starter_map[(name, t_abbr)] = 1 if ath.get("starter") else 0
    for r in rows:
        r["starter"] = starter_map.get((r["player"], r["team"]), 0)
        r["season_window"] = "preseason"
        r["preseason_year"] = 2026
    return rows


def ensure_starter_col(con: sqlite3.Connection) -> None:
    cols = {r[1] for r in con.execute("PRAGMA table_info(nba)")}
    if "starter" not in cols:
        con.execute("ALTER TABLE nba ADD COLUMN starter INTEGER")
    if "season_window" not in cols:
        con.execute("ALTER TABLE nba ADD COLUMN season_window TEXT")
    if "preseason_year" not in cols:
        con.execute("ALTER TABLE nba ADD COLUMN preseason_year INTEGER")
    con.commit()


def fetch_one(ev: dict) -> tuple[dict, list[dict] | None, str | None]:
    try:
        box = _get(SUMMARY.format(eid=ev["event_id"]))
        rows = parse_box_with_starter(
            box, ev["event_id"], ev["date"], ev["home"], ev["away"]
        )
        return ev, rows, None
    except Exception as exc:
        return ev, None, str(exc)


def db_index(con: sqlite3.Connection) -> dict[tuple, dict]:
    """Key = (event_id, player, team) → row dict."""
    cols = [
        "event_id",
        "game_date",
        "player",
        "team",
        "starter",
        *COMPARE_COLS,
    ]
    # starter may be missing on old DBs before alter
    have = {r[1] for r in con.execute("PRAGMA table_info(nba)")}
    cols = [c for c in cols if c in have or c in {"event_id", "game_date", "player", "team"}]
    sql = f"SELECT {', '.join(cols)} FROM nba WHERE game_date BETWEEN '2026-09-25' AND '2026-10-19'"
    out = {}
    for row in con.execute(sql):
        d = dict(zip(cols, row))
        key = (str(d["event_id"]), str(d["player"]), str(d["team"]))
        out[key] = d
    return out


def _f(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def mismatch(espn: dict, db: dict, tol: float = 0.51) -> list[str]:
    diffs = []
    for c in COMPARE_COLS:
        a, b = _f(espn.get(c)), _f(db.get(c))
        if a is None and b is None:
            continue
        if a is None or b is None:
            diffs.append(f"{c}:espn={a}/db={b}")
            continue
        if abs(a - b) > tol:
            diffs.append(f"{c}:{b}->{a}")
    if "starter" in espn and db.get("starter") is not None:
        if int(espn.get("starter") or 0) != int(db.get("starter") or 0):
            diffs.append(f"starter:{db.get('starter')}->{espn.get('starter')}")
    elif "starter" in espn and db.get("starter") is None:
        diffs.append(f"starter:None->{espn.get('starter')}")
    return diffs


def upsert_rows(con: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    # Only columns that exist on table
    have = {r[1] for r in con.execute("PRAGMA table_info(nba)")}
    cols = [c for c in rows[0].keys() if c in have]
    placeholders = ", ".join("?" * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO nba ({col_names}) VALUES ({placeholders})"
    data = [[r.get(c) for c in cols] for r in rows]
    with con:
        con.executemany(sql, data)
    return len(rows)


def main() -> int:
    print(f"DB: {ROOT_DB}")
    con = init_db(ROOT_DB)
    ensure_starter_col(con)

    print("Listing ESPN preseason events...")
    events = list_preseason_events(date(2026, 9, 25), date(2026, 10, 21))
    print(f"  ESPN preseason games: {len(events)}")

    espn_rows: list[dict] = []
    fetch_errors: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(fetch_one, ev) for ev in events]
        for i, fut in enumerate(as_completed(futs), 1):
            ev, rows, err = fut.result()
            if err:
                fetch_errors.append({**ev, "error": err})
                print(f"  ERR {ev['date']} {ev['event_id']}: {err}")
            else:
                espn_rows.extend(rows or [])
            if i % 10 == 0:
                print(f"  fetched {i}/{len(events)} games, rows={len(espn_rows)}")

    print(f"ESPN boxes total: {len(espn_rows)} player-rows across {len(events)} games")

    before = db_index(con)
    espn_by_event = {e["event_id"] for e in events}
    db_events = {k[0] for k in before}
    missing_games = sorted(espn_by_event - db_events)
    extra_db_games = sorted(db_events - espn_by_event)

    missing_players: list[dict] = []
    conflicts: list[dict] = []
    matches = 0
    to_upsert: list[dict] = []

    espn_keys = set()
    for r in espn_rows:
        key = (str(r["event_id"]), str(r["player"]), str(r["team"]))
        espn_keys.add(key)
        db = before.get(key)
        if db is None:
            missing_players.append(
                {
                    "event_id": r["event_id"],
                    "date": r["game_date"],
                    "player": r["player"],
                    "team": r["team"],
                }
            )
            to_upsert.append(r)
            continue
        diffs = mismatch(r, db)
        if diffs:
            conflicts.append(
                {
                    "event_id": r["event_id"],
                    "date": r["game_date"],
                    "player": r["player"],
                    "team": r["team"],
                    "diffs": diffs,
                }
            )
            to_upsert.append(r)  # ESPN wins
        else:
            # still refresh starter / season_window tags
            if db.get("starter") is None or db.get("starter") != r.get("starter"):
                to_upsert.append(r)
            else:
                matches += 1

    n_added = upsert_rows(con, to_upsert)
    print(f"Upserted {n_added} rows (missing+conflicts+starter tags)")

    # Sync Sports/NBA copy: re-upsert same rows there
    sports_con = init_db(SPORTS_DB)
    ensure_starter_col(sports_con)
    n_sports = upsert_rows(sports_con, espn_rows)  # full ESPN set into sports copy
    # Also copy any Oct 20-21 RS already in root that sports might need
    print(f"Sports/NBA sync upserted {n_sports} ESPN preseason rows")

    # Tag season_window on existing rows by date heuristic if null
    with con:
        con.execute(
            "UPDATE nba SET season_window='preseason', preseason_year=2026 "
            "WHERE game_date BETWEEN '2026-10-01' AND '2026-10-19' "
            "AND (season_window IS NULL OR season_window='')"
        )
        con.execute(
            "UPDATE nba SET season_window='regular' "
            "WHERE game_date >= '2026-10-20' "
            "AND (season_window IS NULL OR season_window='')"
        )

    after = db_index(con)
    after_events = {k[0] for k in after}
    still_missing_games = sorted(espn_by_event - after_events)

    lines = [
        "NBA 2026 preseason ESPN backfill",
        f"espn_games={len(events)}  espn_player_rows={len(espn_rows)}",
        f"fetch_errors={len(fetch_errors)}",
        f"db_before_keys={len(before)}  db_after_keys={len(after)}",
        f"missing_games_before={len(missing_games)}  still_missing={len(still_missing_games)}",
        f"missing_player_rows={len(missing_players)}  conflicts={len(conflicts)}  exact_matches={matches}",
        f"upserted={n_added}  sports_sync_rows={n_sports}",
        "",
        "===== MISSING GAMES (before) =====",
    ]
    for eid in missing_games:
        ev = next((e for e in events if e["event_id"] == eid), None)
        lines.append(f"  {eid}  {ev}")
    lines.append("")
    lines.append(f"===== CONFLICTS (ESPN wins) n={len(conflicts)} =====")
    for c in conflicts[:80]:
        lines.append(
            f"  {c['date']} {c['player']} ({c['team']}) {', '.join(c['diffs'])}"
        )
    if len(conflicts) > 80:
        lines.append(f"  ... {len(conflicts) - 80} more")
    lines.append("")
    lines.append(f"===== MISSING PLAYERS sample n={len(missing_players)} =====")
    for m in missing_players[:40]:
        lines.append(f"  {m['date']} {m['player']} ({m['team']}) eid={m['event_id']}")
    if len(missing_players) > 40:
        lines.append(f"  ... {len(missing_players) - 40} more")
    lines.append("")
    lines.append("===== FETCH ERRORS =====")
    for e in fetch_errors:
        lines.append(f"  {e}")

    # Spot-check 10 players vs ESPN re-fetch
    spot_names = [
        "Jaylon Tyson",
        "Chet Holmgren",
        "LeBron James",
        "VJ Edgecombe",
        "Donovan Mitchell",
        "Evan Mobley",
        "Tyrese Maxey",
        "Jalen Williams",
        "Cason Wallace",
        "Anfernee Simons",
    ]
    lines.append("")
    lines.append("===== SPOT-CHECK 10 players (ESPN vs DB) =====")
    spot_match = 0
    spot_n = 0
    for name in spot_names:
        # find one ESPN row
        hits = [r for r in espn_rows if r["player"] == name]
        if not hits:
            lines.append(f"  {name}: no ESPN preseason row")
            continue
        r = sorted(hits, key=lambda x: x["game_date"], reverse=True)[0]
        key = (str(r["event_id"]), name, str(r["team"]))
        db = after.get(key)
        spot_n += 1
        if db is None:
            lines.append(f"  MISS {name} {r['game_date']} not in DB after upsert")
            continue
        diffs = mismatch(r, db)
        if not diffs:
            spot_match += 1
            lines.append(
                f"  MATCH {name} {r['game_date']} min={r['minutes']} "
                f"pts={r['pts']} fg2a={r['fg2a']} starter={r['starter']}"
            )
        else:
            lines.append(f"  HOLD {name} {r['game_date']} {diffs}")
    rate = (100.0 * spot_match / spot_n) if spot_n else 0.0
    lines.append(f"spot-check match rate: {spot_match}/{spot_n} ({rate:.0f}%)")

    # Players on tonight's board with zero 2026 preseason
    board = ROOT / "outputs" / "2026-10-22" / "nba" / "step1_pp_props_today.csv"
    no_pre: list[str] = []
    if board.exists():
        import pandas as pd

        bdf = pd.read_csv(board)
        pcol = "player_name" if "player_name" in bdf.columns else "player"
        if pcol not in bdf.columns:
            for c in bdf.columns:
                if "player" in c.lower():
                    pcol = c
                    break
        players = sorted({str(x).strip() for x in bdf[pcol].dropna().unique()})
        for p in players:
            n = con.execute(
                "SELECT count(*) FROM nba WHERE player=? AND game_date BETWEEN "
                "'2026-10-01' AND '2026-10-19'",
                (p,),
            ).fetchone()[0]
            if n == 0:
                no_pre.append(p)
        lines.append("")
        lines.append(f"===== BOARD PLAYERS WITH NO 2026 PRESEASON (n={len(no_pre)}) =====")
        for p in no_pre[:60]:
            lines.append(f"  {p}")
        if len(no_pre) > 60:
            lines.append(f"  ... {len(no_pre) - 60} more")

    text = "\n".join(lines) + "\n"
    REPORT_TXT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_TXT.write_text(text, encoding="utf-8")
    meta = {
        "espn_games": len(events),
        "espn_player_rows": len(espn_rows),
        "missing_games_before": missing_games,
        "still_missing_games": still_missing_games,
        "n_missing_players": len(missing_players),
        "n_conflicts": len(conflicts),
        "n_upserted": n_added,
        "sports_sync_rows": n_sports,
        "spot_check": {"match": spot_match, "n": spot_n, "rate": rate},
        "no_preseason_board_players": no_pre[:200],
        "conflicts_sample": conflicts[:50],
    }
    REPORT_JSON.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(text)
    print(f"wrote {REPORT_TXT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
