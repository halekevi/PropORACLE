import sqlite3
from pathlib import Path

for p in [
    Path(r"H:/PropORACLE/data/cache/proporacle_ref.db"),
    Path(r"H:/PropORACLE/Sports/NBA/data/cache/proporacle_ref.db"),
]:
    if not p.exists():
        print("missing", p)
        continue
    con = sqlite3.connect(str(p))
    mx = con.execute(
        "select max(game_date), min(game_date), count(*) from nba"
    ).fetchone()
    print(p, "rows", mx)
    rows = con.execute(
        "select game_date, minutes, reb, ast, ra from nba "
        "where lower(player) like '%ajay mitchell%' "
        "order by game_date desc limit 12"
    ).fetchall()
    print("mitchell recent:", rows)
    print(
        "on/after Oct20",
        con.execute(
            "select count(*), max(game_date) from nba where game_date >= '2026-10-20'"
        ).fetchone(),
    )
    print(
        "May 2026",
        con.execute(
            "select count(*) from nba where game_date like '2026-05%'"
        ).fetchone(),
    )
    con.close()
