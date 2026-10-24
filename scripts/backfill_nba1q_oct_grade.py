#!/usr/bin/env python3
"""Grade NBA1Q tickets for 2026-10-01..10-22 with period actuals.

Those slips went out before the NBA1Q hold. Fetch 1Q boxes for each date,
build a ticket-face slate when dated step8 is missing, and write
outputs/<date>/graded_nba1q_<date>.xlsx so the legs are auditable.

  py -3.14 scripts/backfill_nba1q_oct_grade.py
  py -3.14 scripts/backfill_nba1q_oct_grade.py --from 2026-10-01 --to 2026-10-22
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from ticket_leg_index import iter_ticket_legs  # noqa: E402


def _daterange(lo: str, hi: str) -> list[str]:
    cur = date.fromisoformat(lo[:10])
    end = date.fromisoformat(hi[:10])
    out: list[str] = []
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _ticket_slate_csv(ds: str, dest: Path) -> int:
    legs = iter_ticket_legs(ds, sport="NBA1Q", repo=ROOT)
    if not legs:
        return 0
    rows = []
    for rec in legs:
        rows.append(
            {
                "Player": rec.get("player") or rec.get("Player") or "",
                "Prop": rec.get("prop") or rec.get("prop_type") or rec.get("Prop") or "",
                "prop_type": rec.get("prop") or rec.get("prop_type") or rec.get("Prop") or "",
                "Tier": rec.get("tier") or rec.get("Tier") or "",
                "Line": rec.get("line") if rec.get("line") is not None else rec.get("Line"),
                "Direction": rec.get("direction") or rec.get("dir") or rec.get("Direction") or "OVER",
                "Pick Type": rec.get("pick_type") or rec.get("Pick Type") or "standard",
                "Team": rec.get("team") or rec.get("Team") or "",
                "Opp": rec.get("opp_team") or rec.get("Opp") or "",
                "Game Date": ds,
                "Sport": "NBA1Q",
            }
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(dest, index=False)
    return len(rows)


def _run(cmd: list[str], *, cwd: Path) -> int:
    print("  CMD:", " ".join(cmd))
    env = dict(**{k: v for k, v in __import__("os").environ.items()})
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    p = subprocess.run(cmd, cwd=str(cwd), env=env)
    return int(p.returncode or 0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="date_from", default="2026-10-20")
    ap.add_argument("--to", dest="date_to", default="2026-10-22")
    ap.add_argument("--output-root", default="", help="outputs tree (default: this repo)")
    ap.add_argument("--skip-existing", action="store_true", help="Skip dates that already have graded_nba1q xlsx")
    ap.add_argument(
        "--espn-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip NBA.com; ESPN play-by-play only (default on so backfill does not contend with live stats.nba.com).",
    )
    ap.add_argument(
        "--include-stale-board",
        action="store_true",
        help="Also grade 2026-10-01..10-19 phantom OKC@SAS tickets (invalid/stale_board; not leak cost).",
    )
    args = ap.parse_args()
    out_root = Path(args.output_root) if args.output_root else ROOT / "outputs"
    fetch = ROOT / "scripts" / "fetch_nba_period_actuals.py"
    grader = ROOT / "scripts" / "grading" / "slate_grader.py"
    py = [sys.executable]
    rc = 0
    date_from = args.date_from
    if not args.include_stale_board and date_from < "2026-10-20":
        print(
            f"  skipping stale_board dates {date_from}..<2026-10-20 "
            "(pass --include-stale-board to grade phantom OKC@SAS slips)"
        )
        date_from = "2026-10-20"
    for ds in _daterange(date_from, args.date_to):
        dated = out_root / ds
        dated.mkdir(parents=True, exist_ok=True)
        graded = dated / f"graded_nba1q_{ds}.xlsx"
        if args.skip_existing and graded.is_file():
            print(f"[skip] {ds} {graded.name} exists")
            continue
        actuals = dated / f"actuals_nba1q_{ds}.csv"
        print(f"=== NBA1Q {ds} ===")
        fetch_cmd = py + [str(fetch), "--date", ds, "--segment", "1Q", "--output", str(actuals)]
        if args.espn_only:
            fetch_cmd.append("--espn-only")
        if _run(fetch_cmd, cwd=ROOT):
            print(f"  WARN: 1Q actuals fetch failed for {ds}")
            rc = 1
            continue
        if not actuals.is_file():
            print(f"  WARN: no actuals file {actuals}")
            rc = 1
            continue
        # Always grade the ticketed 1Q faces. Dated step8 under outputs/<date>/nba1q
        # can be a leftover May Finals board (game_date 2026-05-30) that date-filters
        # to empty and hides the tickets.
        slate = dated / f"nba1q_ticket_faces_{ds}.csv"
        n = _ticket_slate_csv(ds, slate)
        if n == 0:
            print(f"  no NBA1Q tickets — skip grade")
            continue
        print(f"  ticket-face slate {n} rows -> {slate}")
        grade_rc = _run(
            py
            + [
                str(grader),
                "--sport",
                "NBA",
                "--ticket-sport",
                "NBA1Q",
                "--slate",
                str(slate),
                "--actuals",
                str(actuals),
                "--output",
                str(graded),
                "--date",
                ds,
            ],
            cwd=ROOT,
        )
        if grade_rc:
            print(f"  WARN: grader exit {grade_rc} for {ds}")
            rc = 1
        elif graded.is_file():
            print(f"  wrote {graded}")
            from utils.stale_board_tickets import stamp_graded_xlsx

            n_tag = stamp_graded_xlsx(graded, slate_date=ds, sport="NBA1Q")
            if n_tag:
                print(f"  tagged {n_tag} Box Raw row(s) invalid_reason=stale_board")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
