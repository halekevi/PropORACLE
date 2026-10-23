#!/usr/bin/env python3
"""Warn-only ledger checks from fill-by-date + ticket-join views.

  py -3.14 scripts/check_leg_ledger.py
  py -3.14 scripts/check_leg_ledger.py --since 2026-10-01 --strict
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from proporacle.data.ledger_checks import format_report, run_ledger_checks  # noqa: E402
from proporacle.data.leg_ledger import (  # noqa: E402
    LEDGER_ROOT,
    classify_unmatched_ticket_legs,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Warn-only ledger fill / ticket-join checks")
    ap.add_argument("--since", default="", help="YYYY-MM-DD (default: live window from 2026-10-23)")
    ap.add_argument("--ledger-root", default="", help="Override data/ledger path")
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 when any warning fires (default: warn-only, exit 0)",
    )
    ap.add_argument("--unmatched-since", default="", help="Classify unmatched ticket legs from this date")
    ap.add_argument("--unmatched-until", default="", help="Classify unmatched ticket legs through this date")
    args = ap.parse_args()
    root = Path(args.ledger_root) if args.ledger_root else LEDGER_ROOT
    report = run_ledger_checks(root, since=args.since or None, repo=ROOT)
    print(format_report(report))
    if args.unmatched_since or args.unmatched_until:
        miss = classify_unmatched_ticket_legs(
            root,
            since=args.unmatched_since or None,
            until=args.unmatched_until or None,
        )
        print()
        lo = args.unmatched_since or "(open)"
        hi = args.unmatched_until or "(open)"
        print(f"unmatched ticket legs {lo} .. {hi}  n={len(miss)}")
        if miss.empty:
            print("  (none)")
        else:
            grp = (
                miss.groupby(
                    [miss["slate_date"].astype(str).str[:10], "unmatched_reason", "sport"],
                    dropna=False,
                )
                .size()
                .reset_index(name="n")
                .sort_values(["slate_date", "unmatched_reason", "sport"])
            )
            for row in grp.itertuples(index=False):
                print(f"  {row.slate_date}  {row.unmatched_reason:12}  {row.sport:8}  n={row.n}")
            print("  totals:")
            for reason, n in miss["unmatched_reason"].value_counts().items():
                print(f"    {reason}: {n}")
    if args.strict and report.warnings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
