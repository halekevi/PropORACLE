"""Count NHL shots_on_goal zeros in the boxscore ref and graded actuals."""
from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "cache" / "proporacle_ref.db"


def main() -> None:
    print("db", DB, "exists", DB.is_file())
    if not DB.is_file():
        return
    con = sqlite3.connect(DB)
    cols = [r[1] for r in con.execute("PRAGMA table_info(nhl)").fetchall()]
    print("cols", cols)
    if "shots_on_goal" not in cols:
        return
    total, zeros, nulls, positive, d0, d1 = con.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN shots_on_goal = 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN shots_on_goal IS NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN shots_on_goal > 0 THEN 1 ELSE 0 END),
               MIN(game_date), MAX(game_date)
        FROM nhl
        WHERE position IS NULL OR position != 'G'
        """
    ).fetchone()
    print(f"skater rows {total} zero {zeros} null {nulls} positive {positive} {d0} -> {d1}")
    dates = con.execute(
        """
        SELECT COUNT(DISTINCT game_date) FROM nhl
        WHERE shots_on_goal = 0 OR shots_on_goal IS NULL
        """
    ).fetchone()[0]
    print("dates with a zero or null sog", dates)


if __name__ == "__main__":
    main()
