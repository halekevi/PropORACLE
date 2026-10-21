#!/usr/bin/env python3
"""Merge CFB CSV boxscore cache into proporacle_ref.db (fills missing 2026 rows)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.espn_boxscore_cache import load_rows, open_db, ensure_schema, rowcount, upsert_rows

CSV = Path(r"H:/PropORACLE_main_cp/Sports/CFB/data/cache/cfb_boxscore_cache.csv")


def main() -> None:
    con = open_db()
    ensure_schema(con)
    before = rowcount(con, "cfb")
    db_rows = load_rows(con, "cfb")
    db_dates = {}
    for r in db_rows:
        d = str(r.get("game_date") or "").replace("-", "")[:8]
        db_dates[d] = db_dates.get(d, 0) + 1
    print("DB rows", before, "distinct dates", len(db_dates))
    print("DB newest dates", sorted(db_dates.keys(), reverse=True)[:12])

    df = pd.read_csv(CSV, dtype=str).fillna("")
    csv_rows = df.to_dict("records")
    csv_dates = {}
    for r in csv_rows:
        d = str(r.get("game_date") or "").replace("-", "")[:8]
        csv_dates[d] = csv_dates.get(d, 0) + 1
    print("CSV rows", len(csv_rows), "distinct dates", len(csv_dates))
    print("CSV newest dates", sorted(csv_dates.keys(), reverse=True)[:12])

    n = upsert_rows(con, "cfb", csv_rows)
    after = rowcount(con, "cfb")
    print(f"Upserted {n}; DB rows {before} -> {after}")

    # Jones check
    jones = [
        r
        for r in load_rows(con, "cfb")
        if "tavorus" in str(r.get("player_norm") or "").lower()
    ]
    jones_sorted = sorted(
        jones,
        key=lambda r: str(r.get("game_date") or "").replace("-", ""),
        reverse=True,
    )
    print("Jones after merge:")
    for r in jones_sorted[:8]:
        print(
            " ",
            r.get("game_date"),
            "rush",
            r.get("RUSH_YDS"),
            "aid",
            r.get("espn_athlete_id"),
            "team",
            r.get("team_id"),
        )


if __name__ == "__main__":
    main()
