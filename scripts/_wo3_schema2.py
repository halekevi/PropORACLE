import sqlite3
from pathlib import Path

c = sqlite3.connect(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db"))
print("mlb props", c.execute("SELECT prop_norm, COUNT(*) FROM mlb_gamelog GROUP BY 1 ORDER BY 2 DESC LIMIT 40").fetchall())
print("espn sports", c.execute("SELECT sport, COUNT(*) FROM espn_boxscore_rows GROUP BY 1").fetchall())
print("mlb types", c.execute("SELECT player_type, COUNT(*) FROM mlb_gamelog GROUP BY 1").fetchall())
