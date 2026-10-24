#!/usr/bin/env python3
"""Fetch CFBD advanced season stats and write OL/DL/secondary z-score indices.

Observe-only. Equal-weight composites are placeholders.

  py -3.14 Sports/CFB/scripts/pipeline/build_cfb_line_secondary.py
  py -3.14 Sports/CFB/scripts/pipeline/build_cfb_line_secondary.py --year 2025
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_SCRIPT = Path(__file__).resolve()
_REPO = _SCRIPT.parents[4]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.cfb_line_secondary import (  # noqa: E402
    _RANK_CSV,
    build_team_lookup,
    index_rows,
    write_outputs,
)
from utils.cfbd_client import cfbd_get, infer_default_year, resolve_cfbd_key  # noqa: E402

_ADVANCED = "/stats/season/advanced"


def _games_by_abbr(path: Path) -> dict[str, float]:
    if not path.is_file():
        return {}
    rk = pd.read_csv(path, dtype=str)
    out: dict[str, float] = {}
    if "team_abbr" not in rk.columns:
        return out
    for _, r in rk.iterrows():
        abbr = str(r.get("team_abbr") or "").strip().upper()
        try:
            g = float(r.get("games") or "")
        except (TypeError, ValueError):
            continue
        if abbr and g == g:
            out[abbr] = g
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--year", type=int, default=0)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    year = int(args.year) if args.year else infer_default_year()
    key = resolve_cfbd_key()
    if not key:
        print(
            "[CFB line] CANNOT-OBTAIN: no CFBD_API_KEY / CFBD_BEARER / "
            "COLLEGEFOOTBALLDATA_API_KEY in env or .env."
        )
        print("[CFB line] Indices stay empty; step8 attach is a no-op until a key is set.")
        return 2

    print(f"[CFB line] Fetching CFBD {_ADVANCED} year={year}")
    payload = cfbd_get(_ADVANCED, {"year": year}, key)
    if not isinstance(payload, list):
        print("[CFB line] unexpected payload")
        return 1
    rk = pd.read_csv(_RANK_CSV) if _RANK_CSV.is_file() else pd.DataFrame()
    lookup = build_team_lookup(rk)
    games = _games_by_abbr(_RANK_CSV)
    df, meta = index_rows(payload, lookup=lookup, games_by_abbr=games, season=year)
    n_early = int(df["early_season"].fillna(False).astype(bool).sum()) if "early_season" in df.columns else 0
    print(
        f"[CFB line] teams={len(df)} early_season(games<={meta.get('early_season_games')})={n_early} "
        f"weighting={meta.get('weighting')}"
    )
    out = Path(args.out) if args.out else None
    write_outputs(df, meta, csv_path=out)
    print(f"[CFB line] Wrote reference CSV ({len(df)} rows). Observe-only — not a ticket gate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
