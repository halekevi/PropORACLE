import sqlite3
from pathlib import Path

import pandas as pd

con = sqlite3.connect(str(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db")))
df = pd.read_sql_query(
    "SELECT substr(game_date,1,7) AS m, COUNT(*) AS n "
    "FROM wnba WHERE lower(player) IN ('kaitlyn chen','veronica burton') "
    "GROUP BY 1 ORDER BY 1",
    con,
)
print(df.to_string(index=False))
print("global max", pd.read_sql_query("SELECT max(game_date), min(game_date) FROM wnba", con))
# compute pa for burton last 8
df = pd.read_sql_query(
    "SELECT game_date, minutes, pts, ast, (pts+ast) AS pa "
    "FROM wnba WHERE lower(player)='veronica burton' "
    "ORDER BY game_date DESC LIMIT 8",
    con,
)
print("\nBurton pts+ast:")
print(df.to_string(index=False))
df = pd.read_sql_query(
    "SELECT game_date, minutes, pts FROM wnba WHERE lower(player)='kaitlyn chen' "
    "ORDER BY game_date DESC LIMIT 8",
    con,
)
print("\nChen pts:")
print(df.to_string(index=False))
con.close()
