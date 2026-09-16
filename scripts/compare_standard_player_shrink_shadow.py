#!/usr/bin/env python3
"""Summarize standard_player_shrink_shadow_*.json across days.

Usage:
  py -3.14 scripts/compare_standard_player_shrink_shadow.py
  py -3.14 scripts/compare_standard_player_shrink_shadow.py --start 2026-09-10 --end 2026-09-16
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
_DATA = _REPO / "ui_runner" / "data"
_REPORTS = _REPO / "data" / "reports"


def _load_reports(start: str | None, end: str | None) -> pd.DataFrame:
    rows = []
    paths = sorted(_DATA.glob("standard_player_shrink_shadow_*.json"))
    paths += sorted(_REPORTS.glob("standard_player_shrink_shadow_*.json"))
    seen: set[str] = set()
    for p in paths:
        if "latest" in p.name:
            continue
        key = p.name
        if key in seen:
            continue
        seen.add(key)
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        d = str(data.get("date") or "")[:10]
        if start and d and d < start:
            continue
        if end and d and d > end:
            continue
        rows.append(
            {
                "date": d,
                "n_standard": data.get("n_standard"),
                "jaccard_top": data.get("jaccard_top"),
                "mean_pri_raw_top": data.get("mean_pri_raw_top"),
                "mean_pri_shrunk_top": data.get("mean_pri_shrunk_top"),
                "mlb_share_raw_top": data.get("mlb_share_raw_top"),
                "mlb_share_shrunk_top": data.get("mlb_share_shrunk_top"),
                "mlb_share_pool": data.get("mlb_share_pool"),
                "mlb_mean_prior_n": data.get("mlb_mean_prior_n"),
                "mean_prior_n": data.get("mean_prior_n"),
                "rank_on": data.get("production_rank_shrink"),
                "ev_on": data.get("production_ev_shrink"),
            }
        )
    return pd.DataFrame(rows).drop_duplicates(subset=["date"]).sort_values("date")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    args = ap.parse_args()
    df = _load_reports(args.start, args.end)
    if df.empty:
        print("No standard_player_shrink_shadow_*.json found yet.")
        return
    print(df.to_string(index=False))
    print()
    print(
        f"days={len(df)}  mean_jaccard={df['jaccard_top'].mean():.3f}  "
        f"mlb_raw→shrunk "
        f"{df['mlb_share_raw_top'].mean():.3f}→{df['mlb_share_shrunk_top'].mean():.3f}"
    )


if __name__ == "__main__":
    main()
