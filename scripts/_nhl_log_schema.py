import sqlite3
from pathlib import Path

path = Path(r"H:\PropORACLE\data\cache\historical_actuals.db")
con = sqlite3.connect(path)
cols = [r[1] for r in con.execute("PRAGMA table_info(player_game_logs)")]
print("cols", cols)
print(con.execute("SELECT sport, COUNT(*) FROM player_game_logs GROUP BY sport").fetchall())
