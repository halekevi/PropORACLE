"""Backfill as-of proj_edge snapshots for already-graded dates.

New slates freeze at build time (step8 / slate-minutes / T−30) while still
pre-tip. Do not use this script to recompute a live night after tip.

  py -3.14 scripts/freeze_nba_proj_edge.py --from 2026-02-19 --to 2026-10-20
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from score_nba_stat_projections import (  # noqa: E402
    _attach_gate,
    _load_graded,
    _maybe_fit_k,
    attach_nba_projections,
)
from utils.expanding_marginals import walk_forward_lookups_by_date  # noqa: E402
from utils.nba_player_projection import DEFAULT_DB, NbaBoxIndex  # noqa: E402
from utils.nba_proj_edge_snapshot import frozen_exists, write_frozen_day  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="date_from", default="2026-02-19")
    ap.add_argument("--to", dest="date_to", default="2026-10-20")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--templates", default=str(_REPO / "ui_runner" / "templates"))
    ap.add_argument("--replace", action="store_true")
    args = ap.parse_args()

    graded = _load_graded(Path(args.templates))
    sport = graded.get("sport", pd.Series("", index=graded.index)).astype(str).str.strip().str.upper()
    nba = graded.loc[sport.eq("NBA")].copy()
    fd = nba["file_date"].astype(str).str[:10]
    if args.date_from:
        nba = nba.loc[fd >= args.date_from[:10]]
        fd = nba["file_date"].astype(str).str[:10]
    if args.date_to:
        nba = nba.loc[fd <= args.date_to[:10]]
    dates = sorted({str(x)[:10] for x in nba["file_date"].astype(str) if str(x)[:10]})
    if not dates:
        print("[freeze] no NBA dates")
        return 1

    index = NbaBoxIndex(args.db)
    _maybe_fit_k(index, "2026-01-01")
    lookups = walk_forward_lookups_by_date(graded.loc[sport.eq("NBA")])

    wrote = skipped = 0
    for i, d in enumerate(dates):
        if frozen_exists(d) and not args.replace:
            skipped += 1
            continue
        day = nba.loc[nba["file_date"].astype(str).str[:10].eq(d)].copy()
        scored = attach_nba_projections(
            day, as_of=d, db_path=Path(args.db), index=index, lookup=lookups.get(d, {})
        )
        scored["_gate"] = _attach_gate(scored, index, d)
        n = write_frozen_day(scored, d, replace=args.replace)
        wrote += 1
        print(f"[freeze] {d} rows={n}  {i + 1}/{len(dates)}")
    print(f"[freeze] wrote {wrote} dates, skipped {skipped} already frozen")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
