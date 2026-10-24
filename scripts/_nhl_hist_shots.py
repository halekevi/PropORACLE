import sqlite3
from pathlib import Path

con = sqlite3.connect(Path(r"H:\PropORACLE\data\cache\historical_actuals.db"))
print(
    "shots",
    con.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN shots = 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN shots IS NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN shots > 0 THEN 1 ELSE 0 END),
               MIN(game_date), MAX(game_date)
        FROM player_game_logs WHERE sport = 'NHL'
        """
    ).fetchone(),
)
print(
    "saves",
    con.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN saves = 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN saves IS NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN saves > 0 THEN 1 ELSE 0 END)
        FROM player_game_logs WHERE sport = 'NHL'
        """
    ).fetchone(),
)
print(
    "goals with zero shots",
    con.execute(
        """
        SELECT COUNT(*) FROM player_game_logs
        WHERE sport = 'NHL' AND shots = 0 AND goals > 0
        """
    ).fetchone()[0],
)
