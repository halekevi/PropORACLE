import sqlite3
from pathlib import Path

c = sqlite3.connect(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db"))
names = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print(names)
for t in names:
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})")]
    print(t, cols)
