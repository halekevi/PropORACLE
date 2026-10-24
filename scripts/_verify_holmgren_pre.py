import sqlite3

con = sqlite3.connect("data/cache/proporacle_ref.db")
print(
    "Holmgren Oct+",
    con.execute(
        "select game_date, minutes, pts, fg3a, starter, season_window "
        "from nba where player='Chet Holmgren' and game_date>='2026-10-01' "
        "order by game_date"
    ).fetchall(),
)
print("max", con.execute("select max(game_date), count(*) from nba").fetchone())
print(
    "pre tagged",
    con.execute(
        "select count(*) from nba where season_window='preseason'"
    ).fetchone(),
)
print(
    "starter filled Oct1-19",
    con.execute(
        "select count(*) from nba where starter is not null "
        "and game_date between '2026-10-01' and '2026-10-19'"
    ).fetchone(),
)
