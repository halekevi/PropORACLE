#!/usr/bin/env python3
"""Rebuild per-date Vegas lines from on-disk step8 slates (open_line / line_vegas).

Odds API day caches are almost empty. Step8 MLB/WNBA/NBA/NHL/Soccer files already
store the snapshot as open_line (PP fallback copied the PrizePicks number — those
are dropped unless implied_prob or a different open_line proves a book match).

  py -3.14 scripts/rebuild_vegas_from_slates.py
  py -3.14 scripts/rebuild_vegas_from_slates.py --from 2026-07-01 --to 2026-09-02
  py -3.14 scripts/rebuild_vegas_from_slates.py --write-grades
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))

from pickem_step1_schema import VEGAS_OUTPUT_COLUMNS  # noqa: E402
from utils.pp_vegas_gap import (  # noqa: E402
    slate_rows_to_vegas,
    sport_from_step8_name,
)

from attach_market_lines_to_grades import enrich_file, graded_candidates, load_game_rows  # noqa: E402
from utils.pp_vegas_gap import build_game_index, build_vegas_index  # noqa: E402

_USECOLS = {
    "player",
    "Player",
    "prop",
    "Prop",
    "prop_type",
    "Prop Type",
    "line",
    "Line",
    "open_line",
    "Open Line",
    "line_vegas",
    "Line (LV)",
    "implied_prob",
    "implied_prob_book",
    "line_movement",
}


def _date_dirs(*roots: Path) -> dict[str, list[Path]]:
    out: dict[str, list[Path]] = {}
    for root in roots:
        base = root / "outputs"
        if not base.is_dir():
            continue
        for d in base.iterdir():
            if d.is_dir() and len(d.name) == 10 and d.name[4] == "-":
                out.setdefault(d.name, []).append(d)
    return out


def _read_step8(path: Path) -> list[dict[str, Any]]:
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as exc:
        print(f"  skip {path.name}: {exc}")
        return []
    want = {x.lower() for x in _USECOLS}
    keep = [c for c in df.columns if str(c).strip().lower() in want]
    if not keep:
        return []
    return df[keep].to_dict(orient="records")


NBA_RESUME = "2026-10-01"
NHL_RESUME = "2026-09-01"


def rebuild_date(date_str: str, dirs: list[Path], dest_repo: Path) -> Path | None:
    rows: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for d in dirs:
        for path in sorted(d.glob("step8*.xlsx")):
            rp = path.resolve()
            if rp in seen:
                continue
            seen.add(rp)
            sport = sport_from_step8_name(path.name)
            if not sport:
                continue
            if sport == "NBA" and date_str < NBA_RESUME:
                continue
            if sport == "NHL" and date_str < NHL_RESUME:
                continue
            part = slate_rows_to_vegas(_read_step8(path), board_sport=sport)
            if part:
                print(f"  {path.name}: {len(part)} book lines")
            rows.extend(part)
    if not rows:
        stale = dest_repo / "data" / "cache" / "vegas_props" / f"{date_str}.csv"
        if stale.is_file():
            stale.unlink()
        return None
    dest = dest_repo / "data" / "cache" / "vegas_props" / f"{date_str}.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=VEGAS_OUTPUT_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in VEGAS_OUTPUT_COLUMNS})
    print(f"[ok] {date_str} {len(rows)} rows -> {dest}")
    return dest


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill Vegas CSVs from step8 slates")
    ap.add_argument("--from", dest="date_from", default="", help="YYYY-MM-DD inclusive")
    ap.add_argument("--to", dest="date_to", default="", help="YYYY-MM-DD inclusive")
    ap.add_argument("--write-grades", action="store_true", help="Patch graded_props JSON in place")
    args = ap.parse_args()

    by_date = _date_dirs(_REPO, MAIN)
    dates = sorted(by_date)
    if args.date_from:
        dates = [d for d in dates if d >= args.date_from[:10]]
    if args.date_to:
        dates = [d for d in dates if d <= args.date_to[:10]]
    if not dates:
        print("no outputs/<date> folders")
        return

    n_ok = 0
    for date_str in dates:
        print(f"[{date_str}]")
        path = rebuild_date(date_str, by_date[date_str], _REPO)
        if not path:
            print("  no book-matched Vegas rows")
            continue
        n_ok += 1
        if args.write_grades:
            idx = build_vegas_index(list(csv.DictReader(path.open(encoding="utf-8-sig"))))
            games = build_game_index(
                load_game_rows(_REPO / "data" / "cache" / "game_lines.db", date_str)
            )
            for gpath in graded_candidates(date_str):
                n, total = enrich_file(gpath, idx, games, write=True)
                print(f"  grades {gpath.name}: Vegas on {n}/{total}")
    print(f"done dates_with_vegas={n_ok}/{len(dates)}")


if __name__ == "__main__":
    main()
