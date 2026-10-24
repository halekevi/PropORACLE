import sqlite3
from pathlib import Path

con = sqlite3.connect(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db"))
print(
    con.execute(
        """
        SELECT
          SUM(CASE WHEN shots_on_goal = 0 AND goals > 0 THEN 1 ELSE 0 END),
          SUM(CASE WHEN shots_on_goal = 0 AND (goals IS NULL OR goals = 0) THEN 1 ELSE 0 END),
          SUM(CASE WHEN shots_on_goal > 0 THEN 1 ELSE 0 END),
          COUNT(*)
        FROM nhl
        WHERE position IS NULL OR position != 'G'
        """
    ).fetchone()
)
