import sqlite3
from pathlib import Path

for label, p in [
    ("Sports/NBA", Path("Sports/NBA/data/cache/proporacle_ref.db")),
    ("root", Path("data/cache/proporacle_ref.db")),
]:
    con = sqlite3.connect(str(p))
    mx = con.execute("select max(game_date), count(*) from nba").fetchone()
    oct = con.execute(
        "select count(*) from nba where game_date >= '2026-10-01'"
    ).fetchone()[0]
    chet = con.execute(
        "select game_date, minutes, fg3a, fg3m from nba "
        "where player='Chet Holmgren' and game_date >= '2026-10-01' "
        "order by game_date"
    ).fetchall()
    print(f"{label}: max={mx[0]} n={mx[1]} oct_rows={oct} chet_oct={chet}")
