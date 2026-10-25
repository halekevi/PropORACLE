"""Send the per-run pipeline WARN digest (one ntfy message) and clear the spool.

Called at the end of run_daily.ps1 / run_refresh_with_log.ps1 / run_daily_day_ahead.ps1 with the
window label, and hourly by the freshness task with --stale-minutes to drain a run that died
before its own flush.

    py -3.14 scripts/flush_pipeline_alerts.py --label 1AM
    py -3.14 scripts/flush_pipeline_alerts.py --stale-minutes 90
    py -3.14 scripts/flush_pipeline_alerts.py --list
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.pipeline_alerts import flush_pipeline_alert_digest, pending_alerts  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", default="", help="run / window label for the digest title")
    ap.add_argument("--stale-minutes", type=float, default=0.0, help="only flush if the oldest pending line is this old")
    ap.add_argument("--list", action="store_true", help="print pending lines; do not send")
    a = ap.parse_args()
    if a.list:
        for r in pending_alerts():
            print(f"{r.get('ts')} [{r.get('run') or '-'}] {r.get('stage')}: {r.get('reason')}")
        return 0
    print(flush_pipeline_alert_digest(a.label, min_age_sec=a.stale_minutes * 60.0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
