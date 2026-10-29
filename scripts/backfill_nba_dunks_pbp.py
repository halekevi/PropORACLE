#!/usr/bin/python3
"""Stamp ``nba.dunks`` from ESPN play-by-play (made dunks only).

  py -3.14 scripts/backfill_nba_dunks_pbp.py --since 2025-10-01
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nba_pbp_dunks import dunks_by_athlete_id, espn_plays_for_event  # noqa: E402
from utils.proporacle_ref_db import resolve_ref_db  # noqa: E402


def _ensure_dunks_col(con: sqlite3.Connection) -> None:
    have = {r[1] for r in con.execute("PRAGMA table_info(nba)")}
    if "dunks" not in have:
        con.execute("ALTER TABLE nba ADD COLUMN dunks REAL")
        con.commit()
        print("  migrated: nba.dunks", flush=True)


def _events_needing(con: sqlite3.Connection, since: str, redo: bool) -> list[str]:
    if redo:
        rows = con.execute(
            "SELECT DISTINCT event_id FROM nba "
            "WHERE game_date>=? AND event_id IS NOT NULL AND event_id!=''",
            (since,),
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT event_id FROM nba
            WHERE game_date>=? AND event_id IS NOT NULL AND event_id!=''
            GROUP BY event_id
            HAVING SUM(CASE WHEN dunks IS NULL THEN 1 ELSE 0 END) > 0
            """,
            (since,),
        ).fetchall()
    return [str(r[0]) for r in rows]


def _one(event_id: str) -> tuple[str, dict[str, int], int]:
    plays = espn_plays_for_event(event_id)
    return event_id, dunks_by_athlete_id(plays), len(plays)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", default="2025-10-01")
    ap.add_argument("--db", default="")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--all", action="store_true", help="Recompute even when dunks is already set")
    args = ap.parse_args()
    db = Path(args.db) if args.db else resolve_ref_db()
    con = sqlite3.connect(str(db))
    _ensure_dunks_col(con)
    eids = _events_needing(con, args.since, args.all)
    print(f"events={len(eids)} db={db}", flush=True)
    ok = fail = dunk_sum = 0
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
        futs = {pool.submit(_one, eid): eid for eid in eids}
        for i, fut in enumerate(as_completed(futs), 1):
            eid = futs[fut]
            try:
                event_id, counts, n_plays = fut.result()
            except Exception as e:
                fail += 1
                print(f"  FAIL {eid}: {e}", flush=True)
                continue
            if n_plays <= 0:
                fail += 1
            else:
                ok += 1
            dunk_sum += sum(counts.values())
            if counts:
                con.executemany(
                    "UPDATE nba SET dunks=? WHERE event_id=? AND espn_athlete_id=?",
                    [(float(n), event_id, aid) for aid, n in counts.items()],
                )
            con.execute(
                "UPDATE nba SET dunks=0 WHERE event_id=? AND dunks IS NULL",
                (event_id,),
            )
            if i % 25 == 0 or i == len(eids):
                con.commit()
                print(
                    f"  {i}/{len(eids)} events  stamped={ok} empty={fail} dunks={dunk_sum}",
                    flush=True,
                )
    con.commit()
    filled = con.execute(
        "SELECT COUNT(*) FROM nba WHERE dunks IS NOT NULL AND game_date>=?",
        (args.since,),
    ).fetchone()[0]
    print(
        f"events_ok={ok} empty_pbp={fail} row_updates={dunk_sum} "
        f"made_dunks={dunk_sum} rows_with_dunks_col={filled}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
