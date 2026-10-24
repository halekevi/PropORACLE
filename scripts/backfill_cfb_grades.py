"""Grade CFB dates that have step8 but no graded_cfb workbook / JSON rows.

Fetches ESPN actuals for date-1..date+1 (Thursday leftovers / Sunday games),
runs slate_grader, then writes or merges CFB into graded_props JSON without
wiping other sports already in that file.

Permanent hole: 2026-09-19 (Week 3 Saturday). No step8, line_history last
CFB archive is 2026-09-11, PrizePicks lines are gone. Do not add that date
to --dates expecting a reconstruct.

  py -3.14 scripts/backfill_cfb_grades.py
  py -3.14 scripts/backfill_cfb_grades.py --dates 2026-09-12 2026-08-29
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / "scripts" / "grading"))

from build_grades_html import load_graded  # noqa: E402
from fetch_football_actuals import fetch_football_actuals  # noqa: E402

PY = sys.executable
SLATE_GRADER = REPO / "scripts" / "grading" / "slate_grader.py"


def _iso_shift(date_str: str, days: int) -> str:
    return (date.fromisoformat(date_str) + timedelta(days=days)).strftime("%Y-%m-%d")


def _step8(date_str: str) -> Path | None:
    d = REPO / "outputs" / date_str
    for p in (
        d / "cfb" / "step8_cfb_direction_clean.xlsx",
        d / "cfb" / f"step8_cfb_direction_clean_{date_str}.xlsx",
        d / f"step8_cfb_direction_clean_{date_str}.xlsx",
    ):
        if p.is_file():
            return p
    return None


def _discover() -> list[str]:
    out: list[str] = []
    root = REPO / "outputs"
    if not root.is_dir():
        return out
    for d in sorted(p.name for p in root.iterdir() if p.is_dir()):
        try:
            date.fromisoformat(d)
        except ValueError:
            continue
        if _step8(d) is None:
            continue
        if _json_has_cfb(d):
            continue
        graded = root / d / f"graded_cfb_{d}.xlsx"
        if not graded.is_file():
            out.append(d)
    return out


def _concat_actuals(dates: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for d in dates:
        print(f"  fetch ESPN CFB {d}")
        df = fetch_football_actuals("cfb", d)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["player", "team", "prop_type", "actual"])
    out = pd.concat(frames, ignore_index=True)
    out["actual"] = pd.to_numeric(out["actual"], errors="coerce")
    out = out.dropna(subset=["actual"])
    return out.drop_duplicates(subset=["player", "team", "prop_type"], keep="last")


def _json_has_cfb(date_str: str) -> bool:
    p = REPO / "ui_runner" / "templates" / f"graded_props_{date_str}.json"
    if not p.is_file():
        return False
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return any(str(r.get("sport") or "").upper() == "CFB" for r in (raw.get("props") or []))


def _merge_cfb_json(date_str: str, graded_xlsx: Path) -> None:
    """Keep non-CFB rows from an existing JSON; replace CFB via prop_row_for_api."""
    from build_grades_html import export_graded_props_json, prop_row_for_api

    templates = REPO / "ui_runner" / "templates"
    existing_path = templates / f"graded_props_{date_str}.json"
    cfb_raw = load_graded(graded_xlsx, "cfb")
    cfb_rows = []
    for row in cfb_raw:
        p = prop_row_for_api(row, "CFB", slate_date=date_str)
        if p:
            cfb_rows.append(p)
    if existing_path.is_file():
        raw = json.loads(existing_path.read_text(encoding="utf-8"))
        keep = [r for r in (raw.get("props") or []) if str(r.get("sport") or "").upper() != "CFB"]
        raw["props"] = keep + cfb_rows
        raw["count"] = len(raw["props"])
        raw["date"] = date_str
        existing_path.write_text(
            json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"  merged CFB into {existing_path.name} ({len(cfb_rows)} CFB, {len(raw['props'])} total)")
        return
    export_graded_props_json(date_str, templates, [("CFB", cfb_raw)])


def grade_one(date_str: str, *, force_fetch: bool) -> bool:
    slate = _step8(date_str)
    if slate is None:
        print(f"[{date_str}] no CFB step8 — skip")
        return False
    out_dir = REPO / "outputs" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    actuals_path = out_dir / f"actuals_cfb_{date_str}.csv"
    need_fetch = force_fetch or not actuals_path.is_file()
    if actuals_path.is_file() and not force_fetch:
        try:
            existing = pd.read_csv(actuals_path)
            if existing.empty:
                need_fetch = True
        except Exception:
            need_fetch = True
    if need_fetch:
        window = [_iso_shift(date_str, i) for i in (-1, 0, 1)]
        df = _concat_actuals(window)
        df.to_csv(actuals_path, index=False)
        print(f"  wrote {actuals_path} ({len(df)} rows)")
    else:
        print(f"  reuse {actuals_path}")
    if not actuals_path.is_file() or actuals_path.stat().st_size < 20:
        print(f"[{date_str}] empty actuals — skip grade")
        return False
    graded = out_dir / f"graded_cfb_{date_str}.xlsx"
    import subprocess

    cmd = [
        PY,
        str(SLATE_GRADER),
        "--sport",
        "CFB",
        "--slate",
        str(slate),
        "--actuals",
        str(actuals_path),
        "--output",
        str(graded),
        "--date",
        date_str,
    ]
    print(f"  grade {' '.join(cmd[-10:])}")
    proc = subprocess.run(cmd, cwd=str(REPO))
    if proc.returncode != 0 or not graded.is_file():
        print(f"[{date_str}] slate_grader failed rc={proc.returncode}")
        return False
    # Preview boards (e.g. 8/18 before kickoff) produce 0 decided — do not
    # emit a JSON date that looks like a graded slate.
    raw_rows = load_graded(graded, "cfb")
    decided = sum(
        1
        for r in raw_rows
        if str(r.get("Result") or r.get("result") or "").strip().upper()
        in {"HIT", "MISS", "WIN", "LOSS"}
    )
    if decided == 0:
        print(f"  {decided} decided CFB rows — skip graded_props JSON")
        return True
    if _json_has_cfb(date_str):
        print(f"  JSON already has CFB — leave graded_props_{date_str}.json in place")
        return True
    _merge_cfb_json(date_str, graded)
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", nargs="*", help="YYYY-MM-DD (default: step8 and no graded_cfb)")
    ap.add_argument("--force-fetch", action="store_true")
    ap.add_argument("--json-only", action="store_true", help="Re-merge CFB JSON from existing xlsx")
    args = ap.parse_args()
    dates = args.dates or _discover()
    if not dates:
        print("No CFB dates to backfill.")
        return 0
    print("CFB grade backfill:", ", ".join(dates))
    ok = True
    for d in dates:
        print(f"\n=== {d} ===")
        if args.json_only:
            graded = REPO / "outputs" / d / f"graded_cfb_{d}.xlsx"
            if not graded.is_file():
                print(f"[{d}] missing {graded.name}")
                ok = False
                continue
            _merge_cfb_json(d, graded)
            continue
        if not grade_one(d, force_fetch=args.force_fetch):
            ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
