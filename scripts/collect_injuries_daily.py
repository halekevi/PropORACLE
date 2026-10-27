#!/usr/bin/env python3
"""Collect multi-sport injury snapshots (Phase 1).

Writes data/injuries/<date>/<sport>_<HHMM>.json (never overwrite) and
data/injuries/injuries_latest.csv.

  py -3.14 scripts/collect_injuries_daily.py
  py -3.14 scripts/collect_injuries_daily.py --date 2026-10-27 --sports NBA,NFL
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from utils.injury_collect import (  # noqa: E402
    LIVE_SPORTS,
    captured_at_iso,
    collect_sport,
    hhmm_et,
    injuries_dir,
    merge_latest_csv,
    now_et,
    write_snapshot,
)

ET = ZoneInfo("America/New_York")


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily injury collect (timestamped snapshots)")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default: today ET)")
    ap.add_argument(
        "--sports",
        default="",
        help="Comma list (default: live sports set)",
    )
    ap.add_argument("--root", default="", help="Repo root override")
    args = ap.parse_args()

    root = Path(args.root).resolve() if str(args.root).strip() else _REPO
    date_str = (args.date or now_et().strftime("%Y-%m-%d")).strip()[:10]
    sports = [s.strip().upper() for s in (args.sports or ",".join(LIVE_SPORTS)).split(",") if s.strip()]
    captured = captured_at_iso()
    stamp = hhmm_et()
    raw_dir = injuries_dir(date_str, root=root) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    summary: dict = {"date": date_str, "captured_at": captured, "hhmm": stamp, "sports": {}}
    for sport in sports:
        rows, meta = collect_sport(sport, date_str, captured, raw_dir=raw_dir)
        path = write_snapshot(sport, date_str, rows, meta, root=root, hhmm=stamp)
        summary["sports"][sport] = {"n": len(rows), "path": str(path.relative_to(root)), "meta": meta}
        all_rows.extend(rows)
        print(f"[injuries] {sport}: {len(rows)} rows -> {path.name}")

    latest = merge_latest_csv(all_rows, root=root, replace_sports=set(sports))
    summary["latest_csv"] = str(latest.relative_to(root))
    summary["n_total"] = len(all_rows)
    summary_path = injuries_dir(date_str, root=root) / f"_collect_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[injuries] latest -> {latest} ({len(all_rows)} rows across {len(sports)} sports)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
