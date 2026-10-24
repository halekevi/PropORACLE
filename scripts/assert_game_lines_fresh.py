#!/usr/bin/env python3
"""CLI: assert game_lines.db is fresh for the ET slate day.

Exit 0 = ok, exit 2 = stale/missing expected sports, exit 1 = tool error.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.game_lines_freshness import (  # noqa: E402
    assert_game_lines_fresh,
    et_today,
    game_lines_freshness_report,
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="", help="ET slate YYYY-MM-DD (default: today ET)")
    ap.add_argument("--json-out", default="", help="Optional path for report JSON")
    ap.add_argument("--dry-run", action="store_true", help="Print report; never raise")
    args = ap.parse_args()
    day = (args.date or et_today()).strip()[:10]
    rep = game_lines_freshness_report(slate_date=day)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(json.dumps(rep, indent=2))
    if args.dry_run:
        return 0
    if not rep["ok"]:
        try:
            assert_game_lines_fresh(slate_date=day)
        except RuntimeError as exc:
            print(f"FAILED: {exc}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
