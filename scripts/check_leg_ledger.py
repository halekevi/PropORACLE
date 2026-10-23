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
from proporacle.data.leg_ledger import LEDGER_ROOT  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Warn-only ledger fill / ticket-join checks")
    ap.add_argument("--since", default="", help="YYYY-MM-DD (default: last 14 days)")
    ap.add_argument("--ledger-root", default="", help="Override data/ledger path")
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 when any warning fires (default: warn-only, exit 0)",
    )
    args = ap.parse_args()
    root = Path(args.ledger_root) if args.ledger_root else LEDGER_ROOT
    report = run_ledger_checks(root, since=args.since or None, repo=ROOT)
    print(format_report(report))
    if args.strict and report.warnings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
