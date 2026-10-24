#!/usr/bin/env python3
"""Print JSON scrape plan for a refresh (changed props + missing live_cdp)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.payout_rescrape import plan_rescrape  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickets", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--since", default="")
    ap.add_argument("--db", default="")
    args = ap.parse_args()
    plan = plan_rescrape(
        Path(args.tickets),
        str(args.date)[:10],
        since=str(args.since or "").strip() or None,
        db_path=Path(args.db) if str(args.db or "").strip() else None,
    )
    print(json.dumps(plan, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
