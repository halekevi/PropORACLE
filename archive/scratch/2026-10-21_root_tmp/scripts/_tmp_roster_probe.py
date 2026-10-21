import sqlite3
from pathlib import Path

db = Path(r"H:\PropORACLE\Sports\NBA\data\cache\proporacle_ref.db")
con = sqlite3.connect(db)
for name in (
    "Jayson Tatum",
    "Jaylen Brown",
    "Mitchell Robinson",
    "Paul George",
    "Neemias Queta",
    "Karl-Anthony Towns",
):
    rows = con.execute(
        """
        SELECT game_date, team, pts, fga, minutes
        FROM nba WHERE player=? ORDER BY game_date DESC LIMIT 8
        """,
        (name,),
    ).fetchall()
    print(name, rows if rows else "NONE")
    if not rows:
        rows = con.execute(
            "SELECT DISTINCT player FROM nba WHERE player LIKE ? LIMIT 5",
            (f"%{name.split()[-1]}%",),
        ).fetchall()
        print("  similar", rows)

print("\nBOS top scorers Jan-Jun 2026:")
rows = con.execute(
    """
    SELECT player, SUM(pts) s, COUNT(*) n, MAX(game_date)
    FROM nba WHERE team='BOS' AND game_date>='2026-01-01' AND game_date<'2026-07-01'
    GROUP BY player ORDER BY s DESC LIMIT 12
    """
).fetchall()
for r in rows:
    print(r)

print("\nNYK Robinson last teams:")
rows = con.execute(
    """
    SELECT game_date, team, pts, reb, minutes
    FROM nba WHERE player='Mitchell Robinson' ORDER BY game_date DESC LIMIT 10
    """
).fetchall()
print(rows)
con.close()
