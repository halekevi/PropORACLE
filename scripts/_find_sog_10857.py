"""Locate the NHL shots store of about 10,857 zero rows."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

CANDIDATES = [
    Path(os.environ.get("LOCALAPPDATA", "")) / "PropORACLE" / "cache" / "historical_actuals.db",
    Path(r"H:\PropORACLE") / "data" / "cache" / "historical_actuals.db",
    Path(r"H:\PropORACLE") / "data" / "cache" / "proporacle_ref.db",
    Path(r"H:\PropORACLE_main_cp") / "data" / "cache" / "proporacle_ref.db",
]


def main() -> None:
    for path in CANDIDATES:
        print("FILE", path, path.is_file())
        if not path.is_file():
            continue
        con = sqlite3.connect(path)
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        print("  tables", tables)
        for table in tables:
            cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})")]
            sog_cols = [c for c in cols if "shot" in c.lower() or c.lower() == "sog"]
            if not sog_cols:
                continue
            print(" ", table, sog_cols)
            for col in sog_cols:
                total, zeros, positive = con.execute(
                    f"""
                    SELECT COUNT(*),
                           SUM(CASE WHEN {col} = 0 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN {col} > 0 THEN 1 ELSE 0 END)
                    FROM {table}
                    """
                ).fetchone()
                print(f"    {col} total {total} zero {zeros} positive {positive}")


if __name__ == "__main__":
    main()
