#!/usr/bin/env python3
"""Rebuild NFL actuals + graded workbooks and export prop×gate ledger.

Rates default to **unique player-games** (ladder dedup). See
``utils.nfl_prop_gate_ledger``.

Usage:
  py -3.14 Sports/NFL/scripts/backfill_nfl_week1_prop_gates.py
  py -3.14 Sports/NFL/scripts/backfill_nfl_week1_prop_gates.py --skip-fetch
  py -3.14 Sports/NFL/scripts/backfill_nfl_week1_prop_gates.py --skip-fetch --dates 2026-09-18,2026-09-21
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[3]
_MAIN = Path(r"H:\PropORACLE_main_cp")
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts.fetch_football_actuals import fetch_football_actuals
from scripts.grading import slate_grader as sg
from utils.nfl_prop_gate_ledger import (
    WATCH_FANTASY_SKILL_UNDER_N,
    WATCH_SACKS_TAKEN_UNDER_N,
    build_prop_gate_ledger,
    goblin_diagnostic_report,
    ledger_methodology_note,
    print_goblin_diagnostics,
)

DATES = ["2026-09-09", "2026-09-10", "2026-09-13", "2026-09-14"]

LEDGER_JSON = _REPO / "Sports/NFL/data/nfl_prop_gate_ledger_week1.json"
LEDGER_CSV = _REPO / "Sports/NFL/data/nfl_prop_gate_ledger_week1.csv"


def _grade_one(date: str, *, skip_fetch: bool) -> Path | None:
    root = _MAIN if (_MAIN / "outputs" / date / "nfl").is_dir() else _REPO
    date_dir = root / "outputs" / date
    slate = date_dir / "nfl" / "step8_nfl_direction_clean.xlsx"
    if not slate.is_file():
        print(f"[{date}] no step8 — skip")
        return None
    act_path = date_dir / f"actuals_nfl_{date}.csv"
    if not skip_fetch:
        df = fetch_football_actuals("nfl", date)
        act_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(act_path, index=False)
        print(f"[{date}] actuals {len(df)} rows -> {act_path}")
    elif not act_path.is_file():
        print(f"[{date}] missing actuals and --skip-fetch — skip")
        return None
    out = date_dir / f"graded_nfl_{date}.xlsx"
    old = sys.argv[:]
    try:
        sys.argv = [
            "slate_grader.py",
            "--sport",
            "NFL",
            "--slate",
            str(slate),
            "--actuals",
            str(act_path),
            "--output",
            str(out),
            "--date",
            date,
        ]
        sg.main()
    except SystemExit as exc:
        if int(getattr(exc, "code", 0) or 0) not in (0, None):
            print(f"[{date}] grade exit {exc.code}")
            return None
    except Exception as exc:
        print(f"[{date}] grade failed: {exc}")
        return None
    finally:
        sys.argv = old
    print(f"[{date}] graded -> {out}")
    return out if out.is_file() else None


def _load_box(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Box Raw", header=0)
    if df.empty or "result" not in df.columns:
        print(f"[{path.parent.name}] skip empty Box Raw ({path.name})")
        return pd.DataFrame()
    decided = df["result"].astype(str).str.upper().isin(["HIT", "MISS", "PUSH"])
    if int(decided.sum()) == 0:
        print(
            f"[{path.parent.name}] skip stub grade "
            f"(0 decided / {len(df)} rows — empty-folder placeholder)"
        )
        return pd.DataFrame()
    df = df.copy()
    df["slate"] = path.parent.name
    return df


def _print_watch(ledger: pd.DataFrame) -> None:
    print("\nWatch (unique-game primary):")
    for prop, side, need in (
        ("sacks_taken", "UNDER", WATCH_SACKS_TAKEN_UNDER_N),
        ("fantasy_score", "UNDER", WATCH_FANTASY_SKILL_UNDER_N),
    ):
        sub = ledger[
            (ledger["prop"] == prop)
            & (ledger["side"] == side)
            & (ledger["pick_type"].astype(str).str.lower() == "standard")
        ]
        if sub.empty:
            print(f"  {prop} {side}: no rows yet (need unique n>={need})")
            continue
        r = sub.iloc[0]
        print(
            f"  {prop} {side}: unique n={int(r['n'])} hr={r['hr']} "
            f"gate70_n={int(r['gate70_n'])} gate70_hr={r['gate70_hr']} "
            f"ladder_n={int(r['ladder_n'])} (allowlist at n>={need})"
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-fetch", action="store_true")
    ap.add_argument("--skip-grade", action="store_true", help="Rebuild ledger from existing graded xlsx only")
    ap.add_argument("--dates", default=",".join(DATES))
    ap.add_argument(
        "--ledger-tag",
        default="week1",
        help="Output suffix: nfl_prop_gate_ledger_{tag}.{csv,json} (e.g. week2)",
    )
    ap.add_argument(
        "--require-dates",
        default="",
        help=(
            "Comma dates that must contribute ≥1 decided row "
            "(e.g. 2026-09-20,2026-09-21). Empty stubs fail the run."
        ),
    )
    args = ap.parse_args()
    dates = [d.strip() for d in str(args.dates).split(",") if d.strip()]
    require = [d.strip() for d in str(args.require_dates).split(",") if d.strip()]
    tag = str(args.ledger_tag or "week1").strip().lower() or "week1"
    ledger_csv = _REPO / "Sports" / "NFL" / "data" / f"nfl_prop_gate_ledger_{tag}.csv"
    ledger_json = _REPO / "Sports" / "NFL" / "data" / f"nfl_prop_gate_ledger_{tag}.json"

    graded_paths: list[Path] = []
    for d in dates:
        if not args.skip_grade:
            p = _grade_one(d, skip_fetch=bool(args.skip_fetch))
            if p and p.is_file():
                graded_paths.append(p)
                continue
        for root in (_MAIN, _REPO):
            cand = root / "outputs" / d / f"graded_nfl_{d}.xlsx"
            if cand.is_file():
                graded_paths.append(cand)
                break

    boxes = [_load_box(p) for p in graded_paths]
    boxes = [b for b in boxes if not b.empty]
    present = sorted({str(b["slate"].iloc[0]) for b in boxes if "slate" in b.columns})
    if require:
        missing = [d for d in require if d not in present]
        if missing:
            print(
                f"FATAL: required dates missing decided grades: {missing} "
                f"(present={present}). Empty stub folders do not count."
            )
            return 2
    ledger = build_prop_gate_ledger(boxes)
    goblin_report = goblin_diagnostic_report(boxes)
    ledger_json.parent.mkdir(parents=True, exist_ok=True)
    if ledger.empty:
        print("No ledger rows")
        return 1
    ledger.to_csv(ledger_csv, index=False, encoding="utf-8-sig")
    payload = {
        "window": dates,
        "present_slates": present,
        "source": "NFL graded board (Standard + Goblin when present)",
        "methodology": ledger_methodology_note(),
        "note": (
            f"{tag}: unique-game n/hr are canonical; "
            "ladder_* is transparency only. Goblin rows use the same dedup. "
            "Empty stub folders (0 decided) are skipped and must not mark the week complete."
        ),
        "goblin_diagnostics": goblin_report,
        "rows": ledger.to_dict(orient="records"),
    }
    ledger_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {ledger_csv} ({len(ledger)} rows)")
    print(f"Wrote {ledger_json}")
    print(f"Present slates with decided rows: {present}")
    keep = ledger[(ledger["side"] == "UNDER") & (ledger["gate70_n"] >= 5)]
    print("\nGate70 UNDER (unique n, gate70_n>=5):")
    if keep.empty:
        print("  (none)")
    else:
        print(
            keep[["prop", "pick_type", "hr", "n", "ladder_n", "gate70_hr", "gate70_n"]].to_string(
                index=False
            )
        )
    _print_watch(ledger)
    print_goblin_diagnostics(goblin_report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
