"""Find where PrizePicks stored a leg result for soccer Saves/Shots/SOT."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path

DB = Path(r"H:\PropORACLE\data\db\MyTicketPerformance.db")


def walk(obj, prefix=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                yield path
                yield from walk(v, path)
            else:
                yield path
    elif isinstance(obj, list) and obj:
        yield from walk(obj[0], prefix + "[]")


def main() -> None:
    conn = sqlite3.connect(str(DB))
    print("status counts", conn.execute("SELECT status, COUNT(*) FROM user_entries GROUP BY 1").fetchall())
    print("results table", conn.execute("SELECT result, COUNT(*) FROM entry_leg_results GROUP BY 1").fetchall())
    row = conn.execute(
        """
        SELECT e.entry_id, e.status, e.raw_json, l.player_name, l.stat_type, l.description, l.league
        FROM entry_legs l
        JOIN user_entries e ON e.entry_id = l.entry_id
        WHERE lower(l.stat_type) LIKE '%save%'
           OR lower(l.stat_type) LIKE '%shot%'
        LIMIT 1
        """
    ).fetchone()
    if not row:
        print("no row")
        return
    eid, status, raw, player, stat, desc, league = row
    print("sample", player, stat, league, status, desc)
    payload = json.loads(raw) if raw else {}
    keys = Counter(walk(payload))
    print("top keys")
    for k, _ in keys.most_common(40):
        print(" ", k)
    # print a short slice of raw
    text = raw or ""
    print("raw length", len(text))
    print(text[:1500])


if __name__ == "__main__":
    main()
