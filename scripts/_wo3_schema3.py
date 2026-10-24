import json
import sqlite3
from pathlib import Path

c = sqlite3.connect(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db"))
row = c.execute(
    "SELECT sport, payload FROM espn_boxscore_rows WHERE sport='nfl' LIMIT 1"
).fetchone()
payload = json.loads(row[1]) if isinstance(row[1], str) else row[1]
print(type(payload), list(payload)[:40] if isinstance(payload, dict) else str(payload)[:400])
row2 = c.execute(
    "SELECT payload FROM espn_boxscore_rows WHERE sport='cfb' LIMIT 1"
).fetchone()
p2 = json.loads(row2[0]) if isinstance(row2[0], str) else row2[0]
print("cfb", list(p2)[:40] if isinstance(p2, dict) else str(p2)[:400])
