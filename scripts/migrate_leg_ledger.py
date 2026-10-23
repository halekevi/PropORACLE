#!/usr/bin/env python3
"""Load graded_props_*.json and ticket JSON into the local Parquet/DuckDB ledger.

  py -3.14 scripts/migrate_leg_ledger.py --from-graded --date 2026-10-20
  py -3.14 scripts/migrate_leg_ledger.py --from-graded --from-tickets --summary
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from proporacle.data.leg_ledger import (  # noqa: E402
    LEDGER_ROOT,
    connect,
    fill_by_date,
    fill_summary,
    ingest_graded,
    ingest_ticket_runs,
    ticket_join_summary,
    unmatched_ticket_legs,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate graded JSON / tickets into data/ledger/")
    ap.add_argument("--date", default="", help="YYYY-MM-DD only (default: all files)")
    ap.add_argument("--from-graded", action="store_true", help="Load ui_runner/templates/graded_props_*.json")
    ap.add_argument("--from-tickets", action="store_true", help="Load ticket_runs + combined_slate_tickets_*")
    ap.add_argument("--summary", action="store_true", help="Print fill rates and ticket-join rates")
    ap.add_argument("--wipe", action="store_true", help="Delete data/ledger before ingest")
    ap.add_argument("--ledger-root", default="", help="Override data/ledger path")
    args = ap.parse_args()
    date = (args.date or "").strip()[:10] or None
    root = Path(args.ledger_root) if args.ledger_root else LEDGER_ROOT
    if not args.from_graded and not args.from_tickets and not args.summary:
        args.from_graded = True
        args.summary = True
    if args.wipe and root.exists():
        import shutil

        shutil.rmtree(root)
        print(f"wiped {root}")

    if args.from_graded:
        out = ingest_graded(ROOT, date=date, root=root)
        print(
            f"graded files={out['files']} skipped={out.get('skipped', 0)} "
            f"legs={out['legs']} features={out['features']} -> {root}"
        )
    if args.from_tickets:
        out = ingest_ticket_runs(ROOT, date=date, root=root)
        print(
            f"tickets={out['tickets']} ticket_legs={out['ticket_legs']} "
            f"skipped={out.get('skipped', 0)} -> {root}"
        )
    if args.summary:
        fill = fill_summary(root, date=date)
        if fill.empty:
            print("ledger empty (no legs parquet yet)")
        else:
            print("\n=== fill by sport (weighted) ===")
            print(fill.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
            by_date = fill_by_date(root, date=date)
            print("\n=== fill by date × sport ===")
            print(by_date.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
        joins = ticket_join_summary(root, date=date)
        if not joins.empty:
            print("\n=== ticket_legs matched to graded legs (all dates) ===")
            print(joins.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
            print(
                f"overall match_rate="
                f"{joins['matched'].sum() / max(int(joins['ticket_leg_rows'].sum()), 1):.3f} "
                f"unmatched={int(joins['unmatched'].sum())}"
            )
            miss = unmatched_ticket_legs(root, date=date)
            if not miss.empty:
                cols = [c for c in ("slate_date", "sport", "player", "prop", "line", "direction", "pick_type") if c in miss.columns]
                print("\n=== unmatched ticket legs (sample 20) ===")
                print(miss[cols].head(20).to_string(index=False))
        con = connect(root)
        if con is not None:
            try:
                n = con.execute("SELECT COUNT(*) FROM legs").fetchone()[0]
                print(f"\nDuckDB legs view: {n} rows")
            except Exception as exc:
                print(f"DuckDB view not ready: {exc}")
            finally:
                con.close()
        else:
            print("\nDuckDB not installed; Parquet is readable via pandas (pip install duckdb).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
