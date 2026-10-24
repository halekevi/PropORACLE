import sqlite3
from pathlib import Path

con = sqlite3.connect("data/cache/proporacle_ref.db")
cols = [r[1] for r in con.execute("PRAGMA table_info(nba)")]
print("cols", cols)
print(
    "oct1-19",
    con.execute(
        "select min(game_date), max(game_date), count(*) from nba "
        "where game_date between '2026-10-01' and '2026-10-19'"
    ).fetchone(),
)
print(
    "by date",
    con.execute(
        "select game_date, count(*) from nba "
        "where game_date between '2026-10-01' and '2026-10-19' "
        "group by 1 order by 1"
    ).fetchall(),
)
