"""Refresh NFL and CFB ESPN box caches. No slate and no prop file required.

The evening grader calls this on days the board is empty so week-N boxes
are not stuck at the last pipeline run.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "Sports" / "CFB" / "scripts" / "pipeline" / "step5b_attach_boxscore_stats.py"


def _engine():
    spec = importlib.util.spec_from_file_location("cfb_step5b_refresh", ENGINE)
    if spec is None or spec.loader is None:
        raise SystemExit(f"missing box engine: {ENGINE}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def refresh(mod, league: str, days: int, cache: Path) -> None:
    mod.ESPN_LEAGUE = league
    print(f"-> refresh {league} last {days} days -> {cache}", flush=True)
    mod.build_player_histories(
        days=days,
        slate_ids=set(),
        workers=6,
        cache_path=str(cache),
        end_date=dt.date.today(),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--league", choices=("nfl", "college-football", "both"), default="both")
    args = ap.parse_args()
    mod = _engine()
    if args.league in {"nfl", "both"}:
        refresh(
            mod,
            "nfl",
            args.days,
            ROOT / "Sports" / "NFL" / "data" / "cache" / "nfl_boxscore_cache.csv",
        )
    if args.league in {"college-football", "both"}:
        refresh(
            mod,
            "college-football",
            args.days,
            ROOT / "Sports" / "CFB" / "data" / "cache" / "cfb_boxscore_cache.csv",
        )


if __name__ == "__main__":
    main()
