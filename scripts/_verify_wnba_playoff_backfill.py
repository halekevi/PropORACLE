import sqlite3
from pathlib import Path

import pandas as pd

con = sqlite3.connect(str(Path(r"H:\PropORACLE\data\cache\proporacle_ref.db")))
print(
    "max date",
    con.execute("SELECT max(game_date), count(*) FROM wnba WHERE game_date>='2026-09-01'").fetchone(),
)
for name in ("Kaitlyn Chen", "Veronica Burton"):
    df = pd.read_sql_query(
        "SELECT game_date, team, minutes, pts, ast, pa "
        "FROM wnba WHERE lower(player)=? AND game_date>='2026-09-01' "
        "ORDER BY game_date DESC LIMIT 8",
        con,
        params=(name.lower(),),
    )
    print(f"\n=== {name} ===")
    print(df.to_string(index=False))
    if len(df) >= 5:
        top = df.head(5)
        if name.startswith("Kaitlyn"):
            vals = top["pts"].tolist()
            line, side = 4.5, "OVER"
            ok = all(v > line for v in vals)
        else:
            # prefer pa column; fall back to pts+ast
            if top["pa"].notna().all():
                vals = top["pa"].tolist()
            else:
                vals = (top["pts"] + top["ast"]).tolist()
            line, side = 13.5, "UNDER"
            ok = all(v < line for v in vals)
        print(f"L5 {side} {line}: {vals} → L5=5={ok}")
con.close()
