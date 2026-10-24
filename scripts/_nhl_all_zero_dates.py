import sqlite3
from pathlib import Path

con = sqlite3.connect(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db"))
rows = con.execute(
    """
    SELECT game_date,
           COUNT(*) AS n,
           SUM(CASE WHEN shots_on_goal = 0 THEN 1 ELSE 0 END) AS z,
           SUM(CASE WHEN shots_on_goal > 0 THEN 1 ELSE 0 END) AS pos
    FROM nhl
    WHERE position IS NULL OR position != 'G'
    GROUP BY game_date
    ORDER BY game_date
    """
).fetchall()
all_zero = [r for r in rows if r[2] == r[1] and r[1]]
print("dates", len(rows), "all-zero dates", len(all_zero))
if all_zero:
    print(" first", all_zero[0], "last", all_zero[-1])
print("sample recent", rows[-3:])
