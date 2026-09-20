#!/usr/bin/env python3
"""
NFL step5 — ESPN boxscore L5/L10 + rolling averages (delegates to football boxscore engine).

Run from NFL/ with NFL_PIPELINE_ACTIVE=1:
  py -3.14 scripts/step5_attach_boxscore_stats_nfl.py \\
      --input data/outputs/step3_nfl_with_defense.csv \\
      --output data/outputs/step5_nfl_with_stats.csv \\
      --date 2025-09-07
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from _nfl_pipeline_active import require_nfl_pipeline_active_or_exit

_ENGINE = _SCRIPT_DIR.parent.parent / "CFB" / "scripts" / "pipeline" / "step5b_attach_boxscore_stats.py"


def main() -> None:
    require_nfl_pipeline_active_or_exit()
    if not _ENGINE.is_file():
        raise SystemExit(f"Missing boxscore engine: {_ENGINE}")
    argv = ["step5b_attach_boxscore_stats.py", "--league", "nfl", *sys.argv[1:]]
    if "--cache" not in argv:
        argv.extend(["--cache", "data/cache/nfl_boxscore_cache.csv"])
    if "--days" not in argv:
        # Fetch window stays wide so the cache covers last season for NFLP
        # (Mar–Aug) and weeks 1–3 PRIOR_SEASON_FILL. Sep 1 boundary still
        # drives games_used / l5_sample_n / THIN_*; face L5 may mix prior.
        # NFLP and NFL share the same ESPN NFL game logs.
        argv.extend(["--days", "400"])
    sys.argv = argv
    runpy.run_path(str(_ENGINE), run_name="__main__")


if __name__ == "__main__":
    main()
