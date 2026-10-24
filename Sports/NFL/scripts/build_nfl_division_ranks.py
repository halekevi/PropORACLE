#!/usr/bin/env python3
"""Build NFL division offensive/defensive ranks and divisional tightness."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nfl_team_unit_ranks import write_nfl_unit_rank_artifacts


def main() -> int:
    ap = argparse.ArgumentParser(description="NFL division O/D ranks + rival tightness.")
    ap.add_argument("--season", type=int, default=0, help="Rank season (0 = last full 17-game year).")
    ap.add_argument("--refresh-games", action="store_true")
    args = ap.parse_args()
    season = int(args.season) or None
    payload = write_nfl_unit_rank_artifacts(
        _REPO, season=season, refresh_games=bool(args.refresh_games)
    )
    tight = payload.get("tightness") or {}
    delta = tight.get("delta") or {}
    print(
        f"[NFL division] rank_season={payload.get('season')} teams={payload.get('n_teams')} "
        f"games={payload.get('n_games')} -> {payload.get('ranks_path')}"
    )
    if delta:
        print(
            f"  tightness: mean margin div-nondiv={delta.get('mean_margin_div_minus_nondiv')} "
            f"one-score pp={delta.get('one_score_pp_div_minus_nondiv')} "
            f"tighter={delta.get('tighter_if_negative_margin')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
