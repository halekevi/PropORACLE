#!/usr/bin/env python3
"""Print 'ok' if proporacle_ref.db has an nba1q table, else 'missing'."""
from __future__ import annotations

import os
import sqlite3
import sys


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else ""
    if not path or not os.path.exists(path):
        print("missing")
        return
    con = sqlite3.connect(path)
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='nba1q' LIMIT 1"
        ).fetchone()
    finally:
        con.close()
    print("ok" if row else "missing")


if __name__ == "__main__":
    main()
