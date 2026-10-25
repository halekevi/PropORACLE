"""Freshness gate for data/cache/game_lines.db.

Same soft-signal / hard-gate split as THIN_CLEAR_OVER and the anchor-cap:
game lines are environment only. Stale rows must not silently attach as if live.

Slate calendar is US/Eastern (not UTC). A 9PM ET fetch must target that ET day.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
_REPO = Path(__file__).resolve().parents[1]
DEFAULT_DB = _REPO / "data" / "cache" / "game_lines.db"

# Sports the fetcher can write. Season-gated for "expected tonight."
FETCH_SPORTS = ("MLB", "NBA", "NHL", "Soccer", "CBB")

NBA_SEASON_RESUME = "2026-10-01"
NHL_REGULAR_RESUME = "2026-10-08"  # preseason optional; regular season required
CBB_SEASON_RESUME = "2026-10-20"  # early DI / exhibition; was 2026-11-01
MLB_SEASON_END = "2026-11-05"


def et_today(now: datetime | None = None) -> str:
    dt = now or datetime.now(ET)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET).strftime("%Y-%m-%d")


def expected_sports_for_date(slate_date: str) -> list[str]:
    """Sports that should have lines for this ET slate (empty PP board != skip)."""
    d = str(slate_date or "")[:10]
    out: list[str] = []
    if d <= MLB_SEASON_END:
        out.append("MLB")
    out.append("Soccer")  # year-round boards
    if d >= NHL_REGULAR_RESUME:
        out.append("NHL")
    if d >= NBA_SEASON_RESUME:
        out.append("NBA")
    if d >= CBB_SEASON_RESUME:
        out.append("CBB")
    return out


def _conn(db_path: Path) -> sqlite3.Connection | None:
    if not db_path.is_file():
        return None
    return sqlite3.connect(str(db_path))


def sport_date_counts(
    db_path: Path | None = None,
    *,
    slate_date: str,
) -> dict[str, int]:
    path = db_path or DEFAULT_DB
    conn = _conn(path)
    if conn is None:
        return {}
    try:
        cur = conn.execute(
            "SELECT sport, COUNT(*) FROM game_lines WHERE game_date = ? GROUP BY sport",
            (str(slate_date)[:10],),
        )
        return {str(s): int(n) for s, n in cur.fetchall()}
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def max_game_dates(db_path: Path | None = None) -> dict[str, str]:
    path = db_path or DEFAULT_DB
    conn = _conn(path)
    if conn is None:
        return {}
    try:
        cur = conn.execute(
            "SELECT sport, MAX(game_date) FROM game_lines GROUP BY sport"
        )
        return {str(s): str(d or "") for s, d in cur.fetchall()}
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def game_lines_freshness_report(
    *,
    slate_date: str | None = None,
    db_path: Path | None = None,
    sports: list[str] | None = None,
    max_lag_days: int = 2,
) -> dict[str, Any]:
    """Per expected sport: rows for slate_date; stale if coverage lags the slate.

    Missing rows on the slate date is OK when the sport's newest stored game is
    within ``max_lag_days`` (bye / off day). A max_game_date weeks behind the
    slate is always stale — the failure mode of an early-Sep cache on a late-Sep
    board.
    """
    path = db_path or DEFAULT_DB
    day = str(slate_date or et_today())[:10]
    want = list(sports) if sports is not None else expected_sports_for_date(day)
    counts = sport_date_counts(path, slate_date=day)
    maxima = max_game_dates(path)
    mtime = None
    fetched_today = False
    if path.is_file():
        mt = datetime.fromtimestamp(path.stat().st_mtime, tz=ET)
        mtime = mt.isoformat()
        fetched_today = mt.strftime("%Y-%m-%d") == day

    slate_d = datetime.strptime(day, "%Y-%m-%d").date()
    sports_out: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    stale: list[str] = []
    for sp in want:
        n = int(counts.get(sp) or 0)
        mx = maxima.get(sp) or ""
        lag: int | None = None
        if mx:
            try:
                lag = (slate_d - datetime.strptime(mx[:10], "%Y-%m-%d").date()).days
            except ValueError:
                lag = None
        # Coverage is fresh when we have today's rows, or a nearby date within
        # max_lag_days (bye / off day / tomorrow already stored). Negative lag
        # means we already hold a future slate day — that counts as covered.
        covered = n > 0 or (lag is not None and lag <= int(max_lag_days))
        is_stale = not covered
        if not path.is_file() or not mx:
            if sp not in missing:
                missing.append(sp)
            is_stale = True
        elif is_stale and sp not in stale:
            stale.append(sp)
        sports_out[sp] = {
            "rows_for_slate": n,
            "max_game_date": mx,
            "lag_days": lag,
            "stale": is_stale,
        }

    # De-dupe: prefer stale over missing when both listed
    missing = [s for s in missing if s not in stale]

    return {
        "ok": not missing and not stale,
        "slate_date": day,
        "db_path": str(path),
        "db_mtime_et": mtime,
        "fetched_today_et": fetched_today,
        "max_lag_days": int(max_lag_days),
        "expected": want,
        "missing": missing,
        "stale": stale,
        "sports": sports_out,
        "hint": (
            "py -3.14 scripts/fetch_game_lines.py --date "
            f"{day} --refresh"
            if (missing or stale)
            else "game_lines fresh for slate"
        ),
    }


def warn_if_stale(
    *,
    slate_date: str | None = None,
    db_path: Path | None = None,
    label: str = "game_lines",
) -> dict[str, Any]:
    rep = game_lines_freshness_report(slate_date=slate_date, db_path=db_path)
    if rep["missing"] or rep["stale"]:
        print(
            f"[{label}] WARN stale/missing for {rep['slate_date']}: "
            f"missing={rep['missing']} stale={rep['stale']} — {rep['hint']}"
        )
    elif rep["ok"]:
        bits = [
            f"{sp}:{meta['rows_for_slate']}"
            for sp, meta in (rep.get("sports") or {}).items()
        ]
        print(f"[{label}] ok slate={rep['slate_date']} " + " ".join(bits))
    return rep


def assert_game_lines_fresh(
    *,
    slate_date: str | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Loud fail for daily C0 / before live attach."""
    rep = game_lines_freshness_report(slate_date=slate_date, db_path=db_path)
    if not rep["ok"]:
        raise RuntimeError(
            "GAME_LINES_STALE "
            f"slate={rep['slate_date']} missing={rep['missing']} "
            f"stale={rep['stale']} hint={rep['hint']}"
        )
    return rep


def attach_allowed_for_sport(
    sport: str,
    *,
    slate_date: str,
    db_path: Path | None = None,
) -> bool:
    """Hard block: do not attach environment columns from a stale sport-date."""
    from utils.pp_vegas_gap import parent_game_sport

    sp = parent_game_sport(str(sport or ""))
    if not sp:
        return False
    counts = sport_date_counts(db_path or DEFAULT_DB, slate_date=slate_date)
    return int(counts.get(sp) or 0) > 0


def fetch_dates_for_slate(slate_date: str | None = None) -> list[str]:
    """ET slate day plus next ET day (evening boards spill past midnight UTC)."""
    day = str(slate_date or et_today())[:10]
    base = datetime.strptime(day, "%Y-%m-%d").date()
    nxt = (base + timedelta(days=1)).strftime("%Y-%m-%d")
    return [day, nxt]
