#!/usr/bin/python3
"""Compare ESPN PBP made-dunk counts to Basketball-Reference PBP for Oct 28."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nba_pbp_dunks import dunks_by_athlete_id, espn_plays_for_event  # noqa: E402
from utils.proporacle_ref_db import resolve_ref_db  # noqa: E402

# BRef made-dunk tallies (misses/blocks excluded) from
# boxscores/pbp/202610280{UTA,CLE,MIL,CHI}.html pulled 2026-10-29.
BREF = {
    "401909890": {  # SA @ UTA
        "Victor Wembanyama": 6,
        "Stephon Castle": 1,
        "Devin Vassell": 1,
        "De'Aaron Fox": 1,
        "Mo Bamba": 1,
        "Darryn Peterson": 0,  # miss only
    },
    "401909882": {  # WAS @ CLE
        "Alex Sarr": 3,
        "Bilal Coulibaly": 6,
        "Kyshawn George": 1,
        "Evan Mobley": 3,
        "Anthony Davis": 1,
        "Jarrett Allen": 0,  # misses only
        "Deandre Ayton": 1,
    },
    "401909887": {  # HOU @ MIL
        "Amen Thompson": 3,
        "Clint Capela": 1,
        "Kevin Durant": 1,
        "Tyler Herro": 1,
    },
    "401909886": {  # NY @ CHI
        "Caleb Wilson": 2,
        "OG Anunoby": 2,
        "Karl-Anthony Towns": 1,
        "Matas Buzelis": 1,
    },
}


def main() -> int:
    db = resolve_ref_db()
    con = sqlite3.connect(str(db))
    mismatches = []
    compared = 0
    for eid, names in BREF.items():
        plays = espn_plays_for_event(eid)
        counts = dunks_by_athlete_id(plays)
        rows = con.execute(
            "SELECT player, espn_athlete_id FROM nba WHERE event_id=?",
            (eid,),
        ).fetchall()
        by_name = {str(p): str(a or "") for p, a in rows}
        print(f"\n{eid} espn_plays={len(plays)}")
        for name, want in names.items():
            aid = by_name.get(name, "")
            if not aid:
                last = name.split()[-1].lower()
                hits = [a for p, a in by_name.items() if p.lower().endswith(last)]
                aid = hits[0] if len(hits) == 1 else ""
            got = int(counts.get(aid, 0)) if aid else None
            compared += 1
            ok = got == want
            print(f"  {'OK' if ok else 'DIFF'} {name}: espn={got} bref={want} id={aid}")
            if not ok:
                mismatches.append((name, got, want))
    print(f"\ncompared={compared} mismatches={len(mismatches)}")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
