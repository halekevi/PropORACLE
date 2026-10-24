import sqlite3
from pathlib import Path

import pandas as pd

con = sqlite3.connect(str(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db")))
for name in ("Kaitlyn Chen", "Veronica Burton"):
    df = pd.read_sql_query(
        "SELECT game_date, team, minutes, pts, ast, pa, season "
        "FROM wnba WHERE lower(player)=? ORDER BY game_date DESC LIMIT 30",
        con,
        params=(name.lower(),),
    )
    print("===", name, "n=", len(df), "===")
    print(df.to_string(index=False))
    print()

# Find which games match step8 series
# Chen step8: 8,5,6,7,11
df = pd.read_sql_query(
    "SELECT game_date, player, team, minutes, pts FROM wnba "
    "WHERE lower(player)='kaitlyn chen' AND pts IN (8,5,6,7,11) "
    "ORDER BY game_date DESC LIMIT 20",
    con,
)
print("Chen pts in {8,5,6,7,11}:")
print(df.to_string(index=False))

# Burton step8 PA: 13,8,12,11,5
df = pd.read_sql_query(
    "SELECT game_date, player, team, minutes, pts, ast, pa FROM wnba "
    "WHERE lower(player)='veronica burton' AND pa IN (13,8,12,11,5) "
    "ORDER BY game_date DESC LIMIT 20",
    con,
)
print("\nBurton pa in {13,8,12,11,5}:")
print(df.to_string(index=False))
con.close()
