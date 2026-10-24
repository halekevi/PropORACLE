#!/usr/bin/env python3
"""
Fetch game spreads and totals from ESPN into data/cache/game_lines.db.

Spread and total come from the core odds object (spread, overUnder) only.
The details string is never parsed: it mixes moneyline and spread text by sport.
Win probability comes from the moneyline, vig removed. It is not turned into a fake spread.

`spread` on a game row is the home spread (negative = home favored).
The prop join writes the player's-team perspective (negative = that team favored).
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

_scripts_dir = Path(__file__).resolve().parent
_REPO_ROOT = _scripts_dir.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from ensure_local_cache import ensure_local_cache

ensure_local_cache(str(_REPO_ROOT))

REPO_ROOT = _REPO_ROOT
DB_PATH = REPO_ROOT / "data" / "cache" / "game_lines.db"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }
)

# NBA: map ESPN abbrev → PrizePicks / pipeline abbrev (see NBA/scripts/step3_attach_defense.py)
NBA_ESPN_TO_SLATE: dict[str, str] = {
    "BKN": "BRK",
    "GSW": "GS",
    "NOP": "NO",
    "NYK": "NY",
    "SAS": "SA",
    "PHX": "PHO",
    "UTAH": "UTA",
}

# CBB: small alias table (CBB/scripts/pipeline/step1_pp_cbb_scraper.py) — ESPN usually matches PP
CBB_TEAM_ALIASES: dict[str, str] = {
    "GCU": "GC",
    "NEVADA": "NEV",
    "SDST": "SDSU",
    "MIZ": "MIZZ",
    "NCSU": "NCST",
    "GTECH": "GT",
}

# NHL: ESPN → PrizePicks (NHL/scripts/step3_attach_defense_nhl.py TEAM_ALIASES inverted)
NHL_ESPN_TO_SLATE: dict[str, str] = {
    "LAK": "LA",
    "NJD": "NJ",
    "SJS": "SJ",
    "TBL": "TB",
    "CBJ": "CLB",
    "UTA": "ARZ",
    "UTAH": "ARZ",
}

# MLB: ESPN official abbr → common slate / PrizePicks variants (all share one ESPN id).
MLB_ESPN_TO_SLATE: dict[str, str] = {
    "CHW": "CWS",
    "WSH": "WSN",
    "ARI": "AZ",
    "ATH": "OAK",
}

SCOREBOARD_URLS: dict[str, str] = {
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "CBB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    # Soccer uses SOCCER_LEAGUE_PATHS (multi-league). This URL is MLS-only fallback.
    "Soccer": "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard",
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
}

# Completed games often drop odds from the site scoreboard; core API still has closing lines.
CORE_ODDS_PREFIX: dict[str, str] = {
    "NBA": "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events",
    "CBB": "https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/events",
    "NHL": "https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events",
    "Soccer": "https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.1/events",
    "MLB": "https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events",
}

# Soccer scoreboards are per-league. Pooled battery eligibility needs all of these.
SOCCER_LEAGUE_PATHS: list[str] = [
    "soccer/usa.1",
    "soccer/usa.nwsl",
    "soccer/eng.1",
    "soccer/eng.2",
    "soccer/esp.1",
    "soccer/ger.1",
    "soccer/ita.1",
    "soccer/fra.1",
    "soccer/uefa.champions",
    "soccer/uefa.europa",
    "soccer/mex.1",
    "soccer/bra.1",
    "soccer/arg.1",
    "soccer/ksa.1",
    "soccer/fifa.world",
]

# Team directories for the id crosswalk. Game lines for MLB/WNBA/NFL/CFB/WCBB
# are not fetched here; the ids have to exist before that backfill.
TEAM_DIRECTORY_PATHS: dict[str, list[str]] = {
    "NBA": ["basketball/nba"],
    "CBB": ["basketball/mens-college-basketball"],
    "WCBB": ["basketball/womens-college-basketball"],
    "NHL": ["hockey/nhl"],
    "MLB": ["baseball/mlb"],
    "WNBA": ["basketball/wnba"],
    "NFL": ["football/nfl"],
    "CFB": ["football/college-football"],
    "Soccer": list(SOCCER_LEAGUE_PATHS),
}

# Official ESPN abbreviation -> slate abbreviation already stored on older rows.
SLATE_ABBR_ALIASES: dict[str, dict[str, str]] = {
    "NBA": NBA_ESPN_TO_SLATE,
    "CBB": CBB_TEAM_ALIASES,
    "NHL": NHL_ESPN_TO_SLATE,
    "MLB": MLB_ESPN_TO_SLATE,
}

_NEW_GAME_LINE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_id", "TEXT"),
    ("home_team_id", "TEXT"),
    ("away_team_id", "TEXT"),
    ("provider", "TEXT"),
    ("is_closing", "INTEGER"),
    ("captured_at", "TEXT"),
    ("draw_win_prob", "REAL"),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm_slate_team(sport: str, abbr: str) -> str:
    t = str(abbr or "").strip().upper()
    if not t:
        return ""
    if sport == "NBA":
        return NBA_ESPN_TO_SLATE.get(t, t)
    if sport == "CBB":
        return CBB_TEAM_ALIASES.get(t, t)
    if sport == "NHL":
        return NHL_ESPN_TO_SLATE.get(t, t)
    if sport == "MLB":
        return MLB_ESPN_TO_SLATE.get(t, t)
    return t


def _american_to_implied_prob(odds_str: Any) -> float | None:
    if odds_str is None:
        return None
    s = str(odds_str).replace("−", "-").strip().upper()
    if not s or s in ("EVEN", "PK", "PICK"):
        return None
    try:
        if s[0] in "+-":
            sign, rest = s[0], s[1:]
            n = float(rest)
        else:
            sign, n = "+", float(s)
    except ValueError:
        return None
    if sign == "-":
        return n / (n + 100.0)
    return 100.0 / (n + 100.0)


def _american_number(value: Any) -> Any:
    """ESPN moneyline is either a number or {american: '-184'}."""
    if isinstance(value, dict):
        for key in ("american", "alternateDisplayValue"):
            raw = value.get(key)
            if raw not in (None, ""):
                return raw
        return None
    return value


def line_from_core_item(item: dict[str, Any]) -> dict[str, Any]:
    """Spread, total, book, and vig-free win probs from one core odds item.

    `details` is ignored on purpose. A string like 'MAN -270' is a moneyline.

    Win probability is the team **winning outright**. When `drawOdds` is present
    (soccer three-way markets), implied probs are normalized by home+draw+away.
    Two-way sports (no draw) still normalize by home+away only.
    """
    spread = None
    total = None
    try:
        if item.get("spread") is not None:
            spread = float(item["spread"])
    except (TypeError, ValueError):
        spread = None
    try:
        if item.get("overUnder") is not None:
            total = float(item["overUnder"])
    except (TypeError, ValueError):
        total = None

    provider = item.get("provider") if isinstance(item.get("provider"), dict) else {}
    book = str(provider.get("name") or provider.get("displayName") or "").strip()

    ho = item.get("homeTeamOdds") if isinstance(item.get("homeTeamOdds"), dict) else {}
    ao = item.get("awayTeamOdds") if isinstance(item.get("awayTeamOdds"), dict) else {}
    draw_raw = item.get("drawOdds")
    if isinstance(draw_raw, dict):
        draw_ml = draw_raw.get("moneyLine")
    else:
        draw_ml = draw_raw

    ph = _american_to_implied_prob(_american_number(ho.get("moneyLine")))
    pa = _american_to_implied_prob(_american_number(ao.get("moneyLine")))
    pd = _american_to_implied_prob(_american_number(draw_ml))

    home_wp = away_wp = draw_wp = None
    if ph is not None and pa is not None and pd is not None and (ph + pa + pd) > 0:
        # Three-way (soccer): outright win, not "not losing".
        s = ph + pa + pd
        home_wp = ph / s
        away_wp = pa / s
        draw_wp = pd / s
    elif ph is not None and pa is not None and (ph + pa) > 0:
        home_wp = ph / (ph + pa)
        away_wp = pa / (ph + pa)
    elif ph is not None:
        home_wp = ph
        away_wp = 1.0 - ph
    elif pa is not None:
        away_wp = pa
        home_wp = 1.0 - pa

    return {
        "spread": spread,
        "total": total,
        "provider": book,
        "home_win_prob": home_wp,
        "away_win_prob": away_wp,
        "draw_win_prob": draw_wp,
    }


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS game_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT NOT NULL,
            game_date TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            spread REAL,
            total REAL,
            home_win_prob REAL,
            away_win_prob REAL,
            source TEXT,
            created_at TEXT,
            updated_at TEXT,
            event_id TEXT,
            home_team_id TEXT,
            away_team_id TEXT,
            provider TEXT,
            is_closing INTEGER,
            captured_at TEXT,
            UNIQUE(sport, game_date, home_team, away_team)
        )
        """
    )
    have = {str(row[1]) for row in conn.execute("PRAGMA table_info(game_lines)")}
    for name, decl in _NEW_GAME_LINE_COLUMNS:
        if name not in have:
            conn.execute(f"ALTER TABLE game_lines ADD COLUMN {name} {decl}")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS team_crosswalk (
            sport TEXT NOT NULL,
            abbr TEXT NOT NULL,
            espn_team_id TEXT NOT NULL,
            display_name TEXT,
            PRIMARY KEY (sport, abbr, espn_team_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_team_crosswalk_id ON team_crosswalk (sport, espn_team_id)"
    )
    conn.commit()


def purge_bad_soccer_spreads(conn: sqlite3.Connection) -> int:
    """Drop soccer rows whose spread is a moneyline, not a goal line.

    Goal lines are 0.5–3.5. American odds parsed out of `details` are ±100 or more.
    Those rows are deleted, not repaired.
    """
    cur = conn.execute(
        """
        DELETE FROM game_lines
        WHERE sport = 'Soccer'
          AND spread IS NOT NULL
          AND ABS(spread) > 4
        """
    )
    conn.commit()
    return int(cur.rowcount or 0)


def _get_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """site.api often 403s; site.web.api serves the same path."""
    urls = [url]
    if "site.api.espn.com" in url:
        urls.append(url.replace("site.api.espn.com", "site.web.api.espn.com", 1))
    for candidate in urls:
        try:
            resp = SESSION.get(candidate, params=params, timeout=45.0)
        except requests.RequestException:
            continue
        if resp.status_code == 403:
            continue
        if not resp.ok:
            return None
        try:
            data = resp.json()
        except ValueError:
            return None
        return data if isinstance(data, dict) else None
    return None


def _norm_team_name_key(name: str) -> str:
    """Uppercase ASCII key for club-name joins (graded soccer uses full names)."""
    s = unicodedata.normalize("NFKD", str(name or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.upper().replace("&", " AND ")
    out: list[str] = []
    for ch in s:
        if ch.isalnum() or ch.isspace():
            out.append(ch)
    return " ".join("".join(out).split())


def fetch_scoreboard_json(url: str, yyyymmdd: str) -> dict[str, Any] | None:
    return _get_json(url, {"dates": yyyymmdd})


def fetch_core_odds_item(
    sport: str,
    event_id: str,
    *,
    league_path: str | None = None,
) -> dict[str, Any] | None:
    if sport == "Soccer" and league_path:
        league = str(league_path).split("/")[-1]
        base = f"https://sports.core.api.espn.com/v2/sports/soccer/leagues/{league}/events"
    else:
        base = CORE_ODDS_PREFIX.get(sport)
    if not base:
        return None
    url = f"{base}/{event_id}/competitions/{event_id}/odds"
    try:
        r = SESSION.get(url, timeout=45.0)
        if not r.ok:
            return None
        data = r.json()
        items = data.get("items") or []
        return items[0] if items else None
    except (requests.RequestException, ValueError, TypeError, KeyError):
        return None


def _event_is_final(ev: dict[str, Any]) -> bool:
    status = ev.get("status") if isinstance(ev.get("status"), dict) else {}
    kind = status.get("type") if isinstance(status.get("type"), dict) else {}
    return str(kind.get("state") or "").strip().lower() == "post"


def _is_closing_from_feed(ev: dict[str, Any], core_item: dict[str, Any] | None) -> int:
    """Close flag from the feed/event, not from when we stored the row.

    Prefer an explicit close/open marker on the core odds item when present.
    Otherwise a final event's core odds number is the close (post-game fetches
    keep the closing spread/total after the scoreboard drops live odds).
    Pre-tip / in-progress captures are not close.
    """
    if isinstance(core_item, dict):
        for key in ("isClose", "is_close", "closed", "bettingClosed"):
            if key in core_item and core_item[key] is not None:
                return 1 if bool(core_item[key]) else 0
        status = str(core_item.get("status") or core_item.get("state") or "").strip().lower()
        if status in ("closed", "close", "final"):
            return 1
        if status in ("open", "pre", "preview"):
            return 0
    return 1 if _event_is_final(ev) else 0


def parse_games_for_sport(
    sport: str,
    payload: dict[str, Any],
    game_date: str,
    *,
    league_path: str | None = None,
) -> list[dict[str, Any]]:
    """One row per event. Lines are taken from core odds, never from details."""
    rows: list[dict[str, Any]] = []
    for ev in payload.get("events") or []:
        comps = (ev.get("competitions") or [None])[0]
        if not isinstance(comps, dict):
            continue
        home_abbr = away_abbr = ""
        home_id = away_id = ""
        for c in comps.get("competitors") or []:
            team = c.get("team") or {}
            ab = str(team.get("abbreviation") or "").strip().upper()
            tid = str(team.get("id") or "").strip()
            ha = str(c.get("homeAway") or "").lower()
            if ha == "home":
                home_abbr, home_id = ab, tid
            elif ha == "away":
                away_abbr, away_id = ab, tid
        if not home_abbr or not away_abbr:
            continue

        event_id = str(ev.get("id") or comps.get("id") or "").strip()
        home_team = _norm_slate_team(sport, home_abbr)
        away_team = _norm_slate_team(sport, away_abbr)
        core_item = (
            fetch_core_odds_item(sport, event_id, league_path=league_path)
            if event_id
            else None
        )
        if not core_item:
            continue
        parsed = line_from_core_item(core_item)
        rows.append(
            {
                "sport": sport,
                "game_date": game_date,
                "home_team": home_team,
                "away_team": away_team,
                "home_team_id": home_id,
                "away_team_id": away_id,
                "event_id": event_id,
                "spread": parsed["spread"],
                "total": parsed["total"],
                "home_win_prob": parsed["home_win_prob"],
                "away_win_prob": parsed["away_win_prob"],
                "draw_win_prob": parsed.get("draw_win_prob"),
                "provider": parsed["provider"],
                "is_closing": _is_closing_from_feed(ev, core_item),
                "source": "espn_core",
                "league_path": league_path or "",
            }
        )
    return rows


def recompute_soccer_threeway_wp(*, sleep_s: float = 0.05) -> dict[str, int]:
    """Re-fetch core odds and rewrite Soccer WP with home+draw+away normalization.

    Prior rows used two-way home/(home+away), which inflated outright win probs.
    """
    import time

    conn = sqlite3.connect(str(DB_PATH))
    try:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT id, event_id, home_win_prob, away_win_prob
            FROM game_lines
            WHERE sport='Soccer' AND event_id IS NOT NULL AND event_id != ''
            """
        ).fetchall()
        updated = skipped = failed = 0
        league_cache: dict[str, str] = {}
        for row_id, eid, old_h, old_a in rows:
            eid_s = str(eid)
            item = None
            path = league_cache.get(eid_s)
            paths = [path] + [p for p in SOCCER_LEAGUE_PATHS if p != path] if path else list(SOCCER_LEAGUE_PATHS)
            for p in paths:
                if not p:
                    continue
                item = fetch_core_odds_item("Soccer", eid_s, league_path=p)
                if item:
                    league_cache[eid_s] = p
                    break
            if not item:
                failed += 1
                continue
            parsed = line_from_core_item(item)
            if parsed.get("draw_win_prob") is None:
                # No draw on this item — leave two-way (rare for soccer).
                skipped += 1
                continue
            conn.execute(
                """
                UPDATE game_lines
                SET home_win_prob = ?, away_win_prob = ?, draw_win_prob = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    parsed["home_win_prob"],
                    parsed["away_win_prob"],
                    parsed["draw_win_prob"],
                    _now_iso(),
                    row_id,
                ),
            )
            updated += 1
            if sleep_s > 0:
                time.sleep(sleep_s)
        conn.commit()
    finally:
        conn.close()
    return {"updated": updated, "skipped_no_draw": skipped, "failed": failed}


def upsert_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    now = _now_iso()
    for r in rows:
        conn.execute(
            """
            INSERT INTO game_lines (
                sport, game_date, home_team, away_team, spread, total,
                home_win_prob, away_win_prob, source, created_at, updated_at,
                event_id, home_team_id, away_team_id, provider, is_closing, captured_at,
                draw_win_prob
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sport, game_date, home_team, away_team) DO UPDATE SET
                spread = excluded.spread,
                total = excluded.total,
                home_win_prob = excluded.home_win_prob,
                away_win_prob = excluded.away_win_prob,
                source = excluded.source,
                updated_at = excluded.updated_at,
                event_id = excluded.event_id,
                home_team_id = excluded.home_team_id,
                away_team_id = excluded.away_team_id,
                provider = excluded.provider,
                is_closing = excluded.is_closing,
                captured_at = excluded.captured_at,
                draw_win_prob = excluded.draw_win_prob
            """,
            (
                r["sport"],
                r["game_date"],
                r["home_team"],
                r["away_team"],
                r["spread"],
                r["total"],
                r["home_win_prob"],
                r["away_win_prob"],
                r["source"],
                now,
                now,
                r.get("event_id"),
                r.get("home_team_id"),
                r.get("away_team_id"),
                r.get("provider"),
                r.get("is_closing"),
                now,
                r.get("draw_win_prob"),
            ),
        )
    conn.commit()


def _teams_from_directory_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for sport in payload.get("sports") or []:
        if not isinstance(sport, dict):
            continue
        for league in sport.get("leagues") or []:
            if not isinstance(league, dict):
                continue
            for wrap in league.get("teams") or []:
                tm = wrap.get("team") if isinstance(wrap, dict) else None
                if isinstance(tm, dict) and tm.get("id"):
                    found.append(tm)
    return found


def fetch_team_directory(sport: str) -> list[dict[str, str]]:
    """ESPN team id + every abbreviation that should resolve to it."""
    out: list[dict[str, str]] = []
    aliases = SLATE_ABBR_ALIASES.get(sport) or {}
    for path in TEAM_DIRECTORY_PATHS.get(sport) or []:
        page = 1
        while page <= 20:
            url = f"https://site.api.espn.com/apis/site/v2/sports/{path}/teams"
            payload = _get_json(url, {"limit": 500, "page": page})
            if not payload:
                break
            teams = _teams_from_directory_payload(payload)
            if not teams:
                break
            for tm in teams:
                tid = str(tm.get("id") or "").strip()
                abbr = str(tm.get("abbreviation") or "").strip().upper()
                name = str(tm.get("displayName") or "").strip()
                short = str(tm.get("shortDisplayName") or tm.get("name") or "").strip()
                if not tid or not abbr:
                    continue
                abbrs = {abbr}
                slate = aliases.get(abbr)
                if slate:
                    abbrs.add(str(slate).strip().upper())
                for espn_ab, slate_ab in aliases.items():
                    if str(slate_ab).strip().upper() == abbr:
                        abbrs.add(str(espn_ab).strip().upper())
                # Soccer grades store club names (JUVENTUS), not ESPN abbrs.
                for label in (name, short):
                    key = _norm_team_name_key(label)
                    if key:
                        abbrs.add(key)
                for ab in abbrs:
                    if ab:
                        out.append(
                            {
                                "sport": sport,
                                "abbr": ab,
                                "espn_team_id": tid,
                                "display_name": name,
                            }
                        )
            if len(teams) < 500:
                break
            page += 1
    return out


def upsert_crosswalk(conn: sqlite3.Connection, rows: list[dict[str, str]]) -> int:
    for r in rows:
        conn.execute(
            """
            INSERT INTO team_crosswalk (sport, abbr, espn_team_id, display_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(sport, abbr, espn_team_id) DO UPDATE SET
                display_name = excluded.display_name
            """,
            (r["sport"], r["abbr"], r["espn_team_id"], r["display_name"]),
        )
    conn.commit()
    return len(rows)


def stamp_team_ids_from_crosswalk(conn: sqlite3.Connection) -> int:
    """Fill home/away ESPN ids on existing rows when the abbreviation maps to one id."""
    n = 0
    pending = conn.execute(
        """
        SELECT id, sport, home_team, away_team
        FROM game_lines
        WHERE home_team_id IS NULL OR home_team_id = ''
           OR away_team_id IS NULL OR away_team_id = ''
        """
    ).fetchall()
    for row_id, sport, home, away in pending:
        ids: dict[str, str | None] = {}
        for side, abbr in (("home", home), ("away", away)):
            found = conn.execute(
                """
                SELECT DISTINCT espn_team_id FROM team_crosswalk
                WHERE sport = ? AND abbr = ?
                """,
                (sport, str(abbr or "").strip().upper()),
            ).fetchall()
            ids[side] = str(found[0][0]) if len(found) == 1 else None
        if not ids["home"] and not ids["away"]:
            continue
        conn.execute(
            """
            UPDATE game_lines
            SET home_team_id = COALESCE(?, home_team_id),
                away_team_id = COALESCE(?, away_team_id)
            WHERE id = ?
            """,
            (ids["home"], ids["away"], row_id),
        )
        n += 1
    conn.commit()
    return n


def refresh_crosswalk(conn: sqlite3.Connection, sports: list[str] | None = None) -> dict[str, int]:
    init_db(conn)
    counts: dict[str, int] = {}
    for sport in sports or list(TEAM_DIRECTORY_PATHS):
        rows = fetch_team_directory(sport)
        counts[sport] = upsert_crosswalk(conn, rows)
    return counts


def has_data_for_date(conn: sqlite3.Connection, sport: str, game_date: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM game_lines WHERE sport = ? AND game_date = ? LIMIT 1",
        (sport, game_date),
    )
    return cur.fetchone() is not None


def run_fetch(
    game_date: str,
    sports: list[str],
    refresh: bool,
) -> dict[str, list[dict[str, Any]]]:
    yyyymmdd = game_date.replace("-", "")
    out: dict[str, list[dict[str, Any]]] = {s: [] for s in sports}
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    try:
        init_db(conn)
        purge_bad_soccer_spreads(conn)
        for sp in sports:
            if not refresh and has_data_for_date(conn, sp, game_date):
                cur = conn.execute(
                    "SELECT sport, game_date, home_team, away_team, spread, total, "
                    "home_win_prob, away_win_prob, source FROM game_lines "
                    "WHERE sport = ? AND game_date = ?",
                    (sp, game_date),
                )
                cols = [d[0] for d in cur.description]
                out[sp] = [dict(zip(cols, row)) for row in cur.fetchall()]
                continue
            if sp == "Soccer":
                rows: list[dict[str, Any]] = []
                for path in SOCCER_LEAGUE_PATHS:
                    url = f"https://site.api.espn.com/apis/site/v2/sports/{path}/scoreboard"
                    payload = fetch_scoreboard_json(url, yyyymmdd)
                    if not payload:
                        continue
                    rows.extend(
                        parse_games_for_sport(
                            sp, payload, game_date, league_path=path
                        )
                    )
                if rows:
                    upsert_rows(conn, rows)
                out[sp] = rows
                continue
            url = SCOREBOARD_URLS.get(sp)
            if not url:
                continue
            payload = fetch_scoreboard_json(url, yyyymmdd)
            if not payload:
                continue
            rows = parse_games_for_sport(sp, payload, game_date)
            if rows:
                upsert_rows(conn, rows)
            out[sp] = rows
    finally:
        conn.close()
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch game lines from ESPN into SQLite.")
    ap.add_argument("--sport", choices=("NBA", "CBB", "NHL", "Soccer", "MLB"), default=None)
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--refresh", action="store_true", help="Re-fetch even if DB has this date")
    ap.add_argument(
        "--crosswalk-only",
        action="store_true",
        help="Refresh ESPN team ids, stamp them onto existing rows, and drop bad soccer spreads",
    )
    args = ap.parse_args()

    if args.crosswalk_only:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH))
        try:
            init_db(conn)
            dropped = purge_bad_soccer_spreads(conn)
            counts = refresh_crosswalk(conn)
            stamped = stamp_team_ids_from_crosswalk(conn)
        finally:
            conn.close()
        for sport, n in counts.items():
            print(f"  crosswalk {sport}: {n} abbreviation rows")
        print(f"  soccer moneyline rows deleted: {dropped}")
        print(f"  game rows stamped with team id: {stamped}")
        print(f"  DB: {DB_PATH}")
        return

    if args.date.strip():
        game_date = args.date.strip()
    else:
        # US sports boards are Eastern calendar days — not UTC.
        # A 9PM ET run on Sep 24 must fetch 2026-09-24, not UTC Sep 25.
        from zoneinfo import ZoneInfo

        game_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    # Always pull slate day + next ET day (evening boards / UTC spill).
    dates = [game_date]
    try:
        base = datetime.strptime(game_date, "%Y-%m-%d").date()
        from datetime import timedelta

        dates.append((base + timedelta(days=1)).strftime("%Y-%m-%d"))
    except ValueError:
        pass

    # MLB was missing from the default list — run_daily --refresh never wrote it.
    sports = [args.sport] if args.sport else ["MLB", "NBA", "CBB", "NHL", "Soccer"]

    total_games = 0
    spread_ok = 0
    total_ok = 0
    lines: list[str] = []
    for d in dates:
        result = run_fetch(d, sports, args.refresh)
        for sp in sports:
            rows = result.get(sp) or []
            n = len(rows)
            total_games += n
            spread_ok += sum(1 for r in rows if r.get("spread") is not None)
            total_ok += sum(1 for r in rows if r.get("total") is not None)
            lines.append(f"  {sp}: {n} games fetched ({d})")

    print("\n".join(lines))
    print(f"  Spreads available: {spread_ok}/{total_games} games")
    print(f"  Totals available: {total_ok}/{total_games} games")
    print(f"  DB: {DB_PATH}")

    # Teeth: after fetch, report freshness for the ET slate day (not UTC).
    try:
        from utils.game_lines_freshness import warn_if_stale

        warn_if_stale(slate_date=game_date, label="game_lines fetch")
    except Exception as exc:
        print(f"  [game_lines] freshness check skipped: {exc}")


if __name__ == "__main__":
    main()
