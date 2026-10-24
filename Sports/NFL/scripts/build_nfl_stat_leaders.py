#!/usr/bin/env python3
"""Rank NFL team / league leaders and bottoms from the boxscore cache."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nfl_stat_leaders import write_nfl_leader_artifacts


def main() -> int:
    ap = argparse.ArgumentParser(description="Build NFL stat leader/bottom ranks.")
    ap.add_argument("--season", type=int, default=0)
    args = ap.parse_args()
    season = int(args.season) or None
    payload = write_nfl_leader_artifacts(_REPO, season=season)
    print(
        f"[NFL leaders] season={payload.get('season')} "
        f"players={payload.get('player_count')} blocks={payload.get('block_count')} "
        f"-> {payload.get('csv_path')}"
    )
    boards = payload.get("league_boards") or {}
    rec = (boards.get("rec_yds") or {}).get("top") or []
    if rec:
        print("  league rec yds 1:", rec[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
