#!/usr/bin/env python3
"""Build NFL scoring-vehicle table and game-script pass/rush/kick leans."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nfl_scoring_vehicles import write_scoring_vehicle_artifacts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2025)
    args = ap.parse_args()
    payload = write_scoring_vehicle_artifacts(_REPO, season=int(args.season) or None)
    teams = payload.get("teams")
    n = 0 if teams is None or getattr(teams, "empty", True) else len(teams)
    print(f"[NFL vehicles] season={payload.get('season')} teams={n} games={payload.get('game_n')} -> {payload.get('vehicle_path')}")
    script = payload.get("script") or {}
    for k in ("baseline", "by_script"):
        print(f"  {k}: {script.get(k)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
