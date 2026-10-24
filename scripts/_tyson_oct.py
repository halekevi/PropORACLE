import sqlite3

con = sqlite3.connect("data/cache/proporacle_ref.db")
print(
    "Tyson Oct+",
    con.execute(
        "select game_date, minutes, fg2a, fg2m, pts, team from nba "
        "where player='Jaylon Tyson' and game_date>='2026-10-01' order by game_date"
    ).fetchall(),
)
print(
    "teams Oct20",
    con.execute(
        "select distinct team from nba where game_date='2026-10-20' order by 1"
    ).fetchall(),
)
print(
    "CLE any Oct",
    con.execute(
        "select distinct game_date, team from nba "
        "where team='CLE' and game_date>='2026-10-01' order by 1"
    ).fetchall(),
)
