from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def find_db_path(start: Optional[Path] = None) -> Path:
    """
    Resolve central DB path using only relative discovery.
    Walk upward until we find data/cache/proporacle_ref.db; otherwise return default
    at <repo_root>/data/cache/proporacle_ref.db (created on first write).
    """
    here = (start or Path(__file__)).resolve()
    if here.is_file():
        here = here.parent

    for _ in range(8):
        candidate = here / "data" / "cache" / "proporacle_ref.db"
        if candidate.exists():
            return candidate
        here = here.parent

    return Path(__file__).resolve().parent.parent / "data" / "cache" / "proporacle_ref.db"


def open_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    p = db_path or find_db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(p)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


CREATE_WNBA = """
CREATE TABLE IF NOT EXISTS wnba (
    game_date        TEXT NOT NULL,
    event_id         TEXT NOT NULL,
    league           TEXT,
    home_team        TEXT,
    away_team        TEXT,
    player           TEXT NOT NULL,
    team             TEXT,
    position         TEXT,
    espn_athlete_id  TEXT,
    minutes          REAL,
    pts              REAL, reb   REAL, ast   REAL,
    stl              REAL, blk   REAL, tov   REAL,
    fgm              REAL, fga   REAL,
    fg3m             REAL, fg3a  REAL,
    fg2m             REAL, fg2a  REAL,
    ftm              REAL, fta   REAL,
    oreb             REAL, dreb  REAL,
    pf               REAL,
    pra              REAL, pr    REAL,
    pa               REAL, ra    REAL,
    bs               REAL, fantasy_score REAL,
    PRIMARY KEY (event_id, player, team)
);
"""


CREATE_MLB_GAMELOG = """
CREATE TABLE IF NOT EXISTS mlb_gamelog (
    mlb_player_id  TEXT NOT NULL,
    season         TEXT NOT NULL,
    game_date      TEXT NOT NULL,
    game_id        TEXT NOT NULL,
    player_type    TEXT,
    prop_norm      TEXT NOT NULL,
    stat_value     REAL,
    updated_at     TEXT,
    PRIMARY KEY (mlb_player_id, season, game_id, prop_norm)
);
"""


def ensure_mlb_schema(con: sqlite3.Connection) -> None:
    con.execute(CREATE_MLB_GAMELOG)
    con.execute("CREATE INDEX IF NOT EXISTS idx_mlb_player_date ON mlb_gamelog (mlb_player_id, game_date);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_mlb_prop ON mlb_gamelog (prop_norm, game_date);")
    con.commit()


def ensure_wnba_schema(con: sqlite3.Connection) -> None:
    con.execute(CREATE_WNBA)
    con.execute("CREATE INDEX IF NOT EXISTS idx_wnba_player ON wnba (player, game_date);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wnba_date   ON wnba (game_date);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wnba_espnid ON wnba (espn_athlete_id, game_date);")
    con.commit()


def upsert_rows(con: sqlite3.Connection, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    placeholders = ", ".join("?" * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
    data = [[r.get(c) for c in cols] for r in rows]
    with con:
        con.executemany(sql, data)
    return len(rows)


def log_pipeline_health(
    component: str,
    message: str,
    *,
    level: str = "ERROR",
    extra: Optional[dict[str, Any]] = None,
    start: Optional[Path] = None,
) -> None:
    """
    Append a single JSON line to logs/pipeline_health.log.
    Never raises (best-effort).
    """
    try:
        db_path = find_db_path(start=start or Path(__file__))
        repo_root = db_path.parent.parent  # <root>/data/cache/proporacle_ref.db
        log_dir = repo_root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "pipeline_health.log"

        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": str(level).upper(),
            "component": component,
            "message": message,
        }
        if extra:
            payload["extra"] = extra

        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass

