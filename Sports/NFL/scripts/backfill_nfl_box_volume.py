#!/usr/bin/env python3
"""Fill RUSH_ATT / PASS_ATT on the NFL boxscore cache from ESPN summaries."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nfl_box_volume import backfill_nfl_box_volume


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=0, help="NFL season (0 = every event in the cache)")
    ap.add_argument("--limit", type=int, default=0, help="Cap events (debug)")
    args = ap.parse_args()
    season = int(args.season) or None
    payload = backfill_nfl_box_volume(
        _REPO,
        season=season,
        limit=int(args.limit) or None,
    )
    if not payload.get("ok"):
        print(f"[NFL volume] {payload.get('reason')}")
        return 1
    print(
        f"[NFL volume] season={season or 'all'} events={payload.get('events')} "
        f"fetched={payload.get('fetched')} failed={payload.get('failed')} "
        f"rush_att_rows={payload.get('rush_att_nonzero')} "
        f"pass_att_rows={payload.get('pass_att_nonzero')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
