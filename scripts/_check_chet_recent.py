import sqlite3
from pathlib import Path

import pandas as pd

db = Path("Sports/NBA/data/cache/proporacle_ref.db")
con = sqlite3.connect(str(db))

print("nba max:", con.execute("select max(game_date), count(*) from nba").fetchone())
print(
    "Holmgren max:",
    con.execute(
        "select max(game_date), count(*) from nba where player like '%Holmgren%'"
    ).fetchone(),
)

oct = pd.read_sql_query(
    "select player, team, game_date, minutes, fg3a, fg3m, pts "
    "from nba where game_date >= '2026-10-01' order by game_date desc limit 30",
    con,
)
print("\nany 2026-10+ rows:", len(oct))
if len(oct):
    print(oct.to_string(index=False))

chet = pd.read_sql_query(
    "select game_date, team, minutes, fg3a, fg3m, fg2a, pts "
    "from nba where player = 'Chet Holmgren' order by game_date desc limit 8",
    con,
)
print("\nChet newest 8 in box store:")
print(chet.to_string(index=False))
