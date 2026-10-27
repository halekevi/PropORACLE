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


SPORT_GLOBS = (
    ("NBA", "nba/step8_nba_direction_clean*.xlsx", "step8_nba_direction_clean*.xlsx"),
    ("NFL", "nfl/step8_nfl_direction_clean*.xlsx", "step8_nfl_direction_clean*.xlsx"),
    ("NHL", "nhl/step8_nhl_direction_clean*.xlsx", "step8_nhl_direction_clean*.xlsx"),
    ("CFB", "cfb/step8_cfb_direction_clean*.xlsx", None),
    ("CBB", "cbb/step8_cbb_direction_clean*.xlsx", None),
    ("SOCCER", "soccer/step8_soccer_direction_clean*.xlsx", None),
    ("TENNIS", "tennis/step8_tennis_direction_clean*.xlsx", None),
    ("WNBA", "wnba/step8_wnba_direction_clean*.xlsx", "step8_wnba_direction_clean*.xlsx"),
    ("MLB", "mlb/step8_mlb_direction_clean*.xlsx", "step8_mlb_direction_clean*.xlsx"),
)


def _find_step8(root: Path, date: str, sport: str, nested: str, flat: str | None) -> Path | None:
    base = root / "outputs" / date
    cands: list[Path] = []
    for pattern in (nested, flat):
        if not pattern:
            continue
        cands.extend(sorted(base.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True))
    return cands[0] if cands else None


def _load_board(path: Path, sport: str) -> list[dict]:
    try:
        df = pd.read_excel(path) if path.suffix.lower() in {".xlsx", ".xls"} else pd.read_csv(path)
    except Exception as exc:
        print(f"[attach] WARN skip {path.name}: {exc}")
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
    for sport, nested, flat in SPORT_GLOBS:
        path = _find_step8(root, date, sport, nested, flat)
        if path is None:
            continue
        part = _load_board(path, sport)
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
