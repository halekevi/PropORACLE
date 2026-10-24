import json
import sqlite3
from collections import Counter
from pathlib import Path

c = sqlite3.connect(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db"))
mins = Counter()
for (payload,) in c.execute(
    "SELECT payload FROM espn_boxscore_rows WHERE sport='nfl' LIMIT 500"
):
    p = json.loads(payload)
    mins[str(p.get("MIN"))] += 1
print(mins.most_common(12))
print("sample", json.loads(c.execute("SELECT payload FROM espn_boxscore_rows WHERE sport='nfl' LIMIT 1").fetchone()[0]).get("player_norm"))
