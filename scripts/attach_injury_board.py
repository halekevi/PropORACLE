#!/usr/bin/env python3
"""Attach inj_* columns onto today's step8 boards + write board sidecar CSV.

Does not rewrite frozen track files. Writes:
  data/injuries/<date>/board_injury_<date>.csv
  data/injuries/<date>/unmatched_names.csv
  data/reports/injury_board_attach_<date>.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from utils.injury_board import attach_injury_columns, write_unmatched_log  # noqa: E402
from utils.injury_collect import injuries_dir, now_et  # noqa: E402


SPORT_PATTERNS: dict[str, tuple[str, ...]] = {
    # Prefer *_ALL.parquet — xlsx default sheet is often 1-row _META.
    "NBA": (
        "step8_nba_direction_clean*_ALL.parquet",
        "nba/step8_nba_direction_clean*.parquet",
        "nba/step8_nba_direction_clean*.xlsx",
        "step8_nba_direction_clean*.xlsx",
    ),
    "NFL": (
        "nfl/step8_nfl_direction_clean*.parquet",
        "nfl/step8_nfl_direction_clean*.xlsx",
        "step8_nfl_direction_clean*.xlsx",
    ),
    "NHL": (
        "nhl/step8_nhl_direction_clean*.parquet",
        "nhl/step8_nhl_direction_clean*.xlsx",
        "step8_nhl_direction_clean*.xlsx",
    ),
    "CFB": ("cfb/step8_cfb_direction_clean*.xlsx",),
    "CBB": ("cbb/step8_cbb_direction_clean*.xlsx",),
    "SOCCER": ("soccer/step8_soccer_direction_clean*.xlsx",),
    "TENNIS": ("tennis/step8_tennis_direction_clean*.xlsx", "step8_tennis_direction_clean*.xlsx"),
    "WNBA": ("wnba/step8_wnba_direction_clean*.xlsx", "step8_wnba_direction_clean*.xlsx"),
    "MLB": ("mlb/step8_mlb_direction_clean*.xlsx", "step8_mlb_direction_clean*.xlsx"),
}


def _find_step8(root: Path, date: str, patterns: tuple[str, ...]) -> Path | None:
    base = root / "outputs" / date
    cands: list[Path] = []
    for pattern in patterns:
        cands.extend(base.glob(pattern))
    if not cands:
        return None
    cands.sort(
        key=lambda p: (
            0 if p.suffix.lower() == ".parquet" else 1,
            0 if "_ALL" in p.name.upper() else 1,
            -p.stat().st_mtime,
        )
    )
    return cands[0]


def _load_board(path: Path, sport: str) -> list[dict]:
    try:
        if path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
        elif path.suffix.lower() in {".xlsx", ".xls"}:
            xl = pd.ExcelFile(path)
            sheet = "ALL" if "ALL" in xl.sheet_names else xl.sheet_names[0]
            df = pd.read_excel(path, sheet_name=sheet)
        else:
            df = pd.read_csv(path, low_memory=False)
    except Exception as exc:
        print(f"[attach] WARN skip {path.name}: {exc}")
        return []
    if len(df) <= 2 and "banner" in df.columns and "player" not in df.columns and "Player" not in df.columns:
        print(f"[attach] WARN {path.name} looks like meta-only ({len(df)} rows) — skip")
        return []
    rows = df.where(pd.notnull(df), None).to_dict(orient="records")
    for r in rows:
        r["sport"] = sport
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Attach injury columns to step8 boards")
    ap.add_argument("--date", default="")
    ap.add_argument("--root", default="")
    args = ap.parse_args()
    root = Path(args.root).resolve() if str(args.root).strip() else _REPO
    date = (args.date or now_et().strftime("%Y-%m-%d")).strip()[:10]

    board: list[dict] = []
    sources: dict[str, str] = {}
    for sport, patterns in SPORT_PATTERNS.items():
        path = _find_step8(root, date, patterns)
        if path is None:
            continue
        part = _load_board(path, sport)
        if not part:
            continue
        board.extend(part)
        sources[sport] = str(path.relative_to(root))
        print(f"[attach] {sport}: {len(part)} rows from {path.name}")

    enriched, stats = attach_injury_columns(board, root=root)
    stats["sources"] = sources
    stats["date"] = date

    out_dir = injuries_dir(date, root=root)
    out_dir.mkdir(parents=True, exist_ok=True)
    board_csv = out_dir / f"board_injury_{date}.csv"
    if enriched:
        pd.DataFrame(enriched).to_csv(board_csv, index=False)
    else:
        board_csv.write_text("player,team,sport,inj_status\n", encoding="utf-8")
    write_unmatched_log(stats, out_dir / "unmatched_names.csv")

    report = root / "data" / "reports" / f"injury_board_attach_{date}.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    print(
        f"[attach] matched={stats['n_matched']} out={stats['n_out']} "
        f"soft={stats['n_soft_flag']} stale={stats['n_stale']} -> {board_csv.name}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
