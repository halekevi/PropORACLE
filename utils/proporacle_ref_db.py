"""Canonical proporacle_ref.db path, last-completed-slate freshness, SQLite locks.

While H:\\PropORACLE and H:\\PropORACLE_main_cp both exist, readers and
writers must open the same file. Resolution order:

1. ``PROPORACLE_REF_DB``
2. ``data/cache/proporacle_ref.path`` (local pin; data/cache is gitignored)
3. Newest ``nba.max(game_date)`` among this repo and git worktrees
   (tie-break: ``*_main_cp``)

If a sibling copy is newer than the chosen file, raise. Step4 / grader
log the path and max date on first open.

Freshness is the last ESPN date that actually had a final NBA game, not
calendar yesterday. Off nights (Thanksgiving eve, All-Star) must not fail.
A single team being off is not a DB-freshness check.
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

REF_DB_ENV = "PROPORACLE_REF_DB"
REF_DB_PATHFILE = "proporacle_ref.path"
SQLITE_TIMEOUT_SEC = 30.0
SQLITE_BUSY_MS = 30000
CONNECT_RETRIES = 4

_REPO = Path(__file__).resolve().parents[1]
NBA_SCOREBOARD = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
    "scoreboard?dates={date}&limit=100"
)
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def repo_root() -> Path:
    return _REPO


def local_ref_db(root: Path | None = None) -> Path:
    return (root or _REPO) / "data" / "cache" / "proporacle_ref.db"


def worktree_ref_dbs(root: Path | None = None) -> list[Path]:
    """Every worktree's ``data/cache/proporacle_ref.db`` that exists on disk."""
    root = (root or _REPO).resolve()
    found: list[Path] = []
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "worktree", "list", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
        for line in (proc.stdout or "").splitlines():
            if not line.startswith("worktree "):
                continue
            wt = Path(line.split(" ", 1)[1].strip())
            p = wt / "data" / "cache" / "proporacle_ref.db"
            if p.exists():
                found.append(p.resolve())
    except Exception:
        pass
    for extra in (Path(r"H:\PropORACLE"), Path(r"H:\PropORACLE_main_cp")):
        p = extra / "data" / "cache" / "proporacle_ref.db"
        if p.exists():
            found.append(p.resolve())
    # Preserve order, drop dups.
    return list(dict.fromkeys(found))


def peek_nba_max_date(path: Path) -> date | None:
    if not path.exists():
        return None
    con = None
    try:
        con = sqlite3.connect(str(path), timeout=SQLITE_TIMEOUT_SEC)
        con.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_MS}")
        row = con.execute("SELECT max(game_date) FROM nba").fetchone()
    except sqlite3.Error:
        return None
    finally:
        if con is not None:
            con.close()
    if not row or not row[0]:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def _prefer_live_score(path: Path) -> int:
    s = str(path).replace("/", "\\").lower()
    if "_main_cp" in s:
        return 2
    if s.endswith("\\proporacle\\data\\cache\\proporacle_ref.db") and "_wt_" not in s:
        return 0
    return 1


def _configured_path(root: Path | None = None) -> Path | None:
    root = root or _REPO
    env = (os.environ.get(REF_DB_ENV) or "").strip()
    if env:
        return Path(env).expanduser().resolve()
    pin = root / "data" / "cache" / REF_DB_PATHFILE
    if pin.exists():
        text = pin.read_text(encoding="utf-8").strip().splitlines()
        line = next((ln.strip() for ln in text if ln.strip() and not ln.startswith("#")), "")
        if line:
            return Path(line).expanduser().resolve()
    return None


def resolve_ref_db(*, root: Path | None = None, allow_missing: bool = False) -> Path:
    """Return the one box DB every reader/writer should open."""
    root = (root or _REPO).resolve()
    local = local_ref_db(root)
    siblings = worktree_ref_dbs(root)
    configured = _configured_path(root)

    if configured is not None:
        chosen = configured
    else:
        candidates = [local, *siblings]
        existing = list(
            dict.fromkeys(p.resolve() for p in candidates if p.exists())
        )
        if not existing:
            if allow_missing:
                return local
            raise FileNotFoundError(
                f"NBA box DB missing at {local}. "
                f"Set {REF_DB_ENV} or run build_boxscore_ref.py --backfill --days 7"
            )
        chosen = max(
            existing,
            key=lambda p: (
                peek_nba_max_date(p) or date.min,
                _prefer_live_score(p),
            ),
        )

    chosen_max = peek_nba_max_date(chosen) or date.min
    newer = []
    for other in siblings:
        if other.resolve() == chosen.resolve():
            continue
        other_max = peek_nba_max_date(other)
        if other_max and other_max > chosen_max:
            newer.append((other, other_max))
    if newer:
        other, other_max = newer[0]
        raise RuntimeError(
            f"NBA box DB stale vs sibling: using {chosen} max={chosen_max} "
            f"but {other} max={other_max}. "
            f"Set {REF_DB_ENV}={other}"
        )
    return chosen


def connect_ref_db(
    path: Path | None = None,
    *,
    write: bool = False,
    timeout: float = SQLITE_TIMEOUT_SEC,
) -> sqlite3.Connection:
    """Open the ref DB with busy-timeout + retries (mid-day backfill vs step4)."""
    db = Path(path) if path is not None else resolve_ref_db()
    last: Exception | None = None
    for attempt in range(CONNECT_RETRIES):
        try:
            con = sqlite3.connect(str(db), timeout=timeout)
            con.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_MS}")
            if write:
                con.execute("PRAGMA journal_mode=WAL;")
                con.execute("PRAGMA synchronous=NORMAL;")
            return con
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            last = exc
            time.sleep(0.4 * (attempt + 1))
    assert last is not None
    raise last


@lru_cache(maxsize=32)
def espn_date_has_final_nba(day: date) -> bool | None:
    """True if ESPN lists a completed NBA game that calendar day.

    ``None`` means the scoreboard could not be fetched (do not treat as off-night).
    """
    url = NBA_SCOREBOARD.format(date=day.strftime("%Y%m%d"))
    req = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            import json

            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return None
    events = data.get("events") or []
    if not events:
        return False
    for ev in events:
        st = (ev.get("status") or {}).get("type") or {}
        if st.get("completed") or str(st.get("state") or "").lower() == "post":
            return True
    return False


def last_completed_nba_slate(
    as_of: date,
    *,
    lookback_days: int = 21,
    has_final=None,
) -> date | None:
    """Most recent date *before* ``as_of`` that had a final NBA game.

    Tonight is excluded so 9PM day-ahead still requires last night, not
    in-progress tips. Off nights return an older date instead of yesterday.
    """
    checker = has_final or espn_date_has_final_nba
    saw_answer = False
    for i in range(1, int(lookback_days) + 1):
        day = as_of - timedelta(days=i)
        result = checker(day)
        if result is None:
            continue
        saw_answer = True
        if result:
            return day
    if not saw_answer:
        return None
    return None


def assert_ref_db_fresh(
    *,
    as_of: date | None = None,
    db_max: date | None = None,
    last_completed: date | None = None,
    has_final=None,
    path: Path | None = None,
) -> date:
    """Raise if the DB is behind the last completed NBA slate.

    Calendar lag is not the test. A two-day hole is fine when those nights
    had no NBA games. A player's own off day is not checked here.
    """
    as_of = as_of or date.today()
    path = Path(path) if path is not None else resolve_ref_db()
    mx = db_max if db_max is not None else peek_nba_max_date(path)
    if mx is None:
        raise RuntimeError(f"NBA box DB has no game_date rows (db={path} as_of={as_of})")
    required = last_completed
    if required is None:
        required = last_completed_nba_slate(as_of, has_final=has_final)
    if required is None:
        return mx
    if mx < required:
        raise RuntimeError(
            f"NBA box DB stale: max game_date={mx} last_completed_slate={required} "
            f"as_of={as_of} db={path}. "
            f"Refresh: py -3.14 Sports/NBA/scripts/build_boxscore_ref.py "
            f"--sports nba --date {required.isoformat()} --db {path}"
        )
    return mx


@lru_cache(maxsize=1)
def log_ref_db_once(path_str: str, max_s: str, extra: str = "") -> str:
    line = f"[NBA BOX] db={path_str} max={max_s}"
    if extra:
        line = f"{line} {extra}"
    print(line, flush=True)
    return line


def log_ref_db(path: Path | None = None, *, extra: str = "") -> str:
    db = Path(path) if path is not None else resolve_ref_db()
    mx = peek_nba_max_date(db)
    return log_ref_db_once(str(db), mx.isoformat() if mx else "none", extra)
