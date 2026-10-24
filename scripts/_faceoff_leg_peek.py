import sqlite3
from pathlib import Path

db = Path(r"H:\PropORACLE\data\db\MyTicketPerformance.db")
conn = sqlite3.connect(db)
print(
    conn.execute(
        "SELECT entry_id, player_name, stat_type, line FROM entry_legs "
        "WHERE lower(COALESCE(stat_type, '')) LIKE '%faceoff%'"
    ).fetchall()
)
