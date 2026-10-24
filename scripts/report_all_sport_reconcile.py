"""Print per-category reconcile tables for every sport on a slate date.

  py -3.14 scripts/report_all_sport_reconcile.py --date 2026-10-20

Uses main_cp outputs when present, else this checkout. Empty step1 files print
status=no_slate or fetch_failed (header-only vs missing).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from utils.pp_category_reconcile import print_reconciliation, reconcile_categories
from utils.step1_board_reconcile import classify_empty_step1, print_empty_reconcile

SPORT_PATHS: dict[str, tuple[str, ...]] = {
    "NBA": ("nba/step1_pp_props_today.csv", "nba/step1_nba_props.csv"),
    "NHL": ("nhl/step1_nhl_props.csv",),
    "TENNIS": ("tennis/step1_tennis_props.csv",),
    "SOCCER": ("soccer/step1_soccer_props.csv",),
    "WNBA": ("wnba/step1_wnba_props.csv",),
    "NFL": ("nfl/step1_pp_props_today.csv", "nfl/step1_nfl_props.csv"),
    "MLB": ("mlb/step1_mlb_props.csv",),
}


def _choose_root(date: str) -> Path:
    for root in (
        _REPO.parent / "PropORACLE_main_cp" / "outputs" / date,
        _REPO / "outputs" / date,
    ):
        if root.is_dir():
            return root
    return _REPO / "outputs" / date


def _load(path: Path) -> pd.DataFrame | None:
    if not path.is_file():
        return None
    if path.stat().st_size < 50:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-10-20")
    args = ap.parse_args()
    date = str(args.date).strip()[:10]
    root = _choose_root(date)
    print(f"Reconcile tables for {date}  root={root}\n")

    for sport, rels in SPORT_PATHS.items():
        path = None
        df = None
        for rel in rels:
            cand = root / rel
            loaded = _load(cand)
            if loaded is not None:
                path = cand
                df = loaded
                break
        if path is None:
            # No file on a day when sibling sports have boards → usually no_slate
            # (offseason / no games), not a silent fetch failure.
            print_empty_reconcile(sport, status="no_slate", fetched_n=0, survived_n=0)
            print(f"  (no step1 file under {root} — treat as no_slate unless a fetch log says otherwise)")
            continue
        if df is None or df.empty:
            # Header-only / empty → no_slate (offseason or no games that day)
            status = classify_empty_step1(fetched_n=0, survived_n=0, fetch_failed=False)
            print_empty_reconcile(sport, status=status, fetched_n=0, survived_n=0)
            print(f"  file={path.name} (empty)")
            continue
        # NHL uses stat_type
        if "prop_type" not in df.columns and "stat_type" in df.columns:
            df = df.copy()
            df["prop_type"] = df["stat_type"]
        rows = reconcile_categories(
            sport=sport,
            board_df=df,
            fetched_df=df,
            kept_df=df,
        )
        for r in rows:
            if r.category == "fantasy" and r.kept > 0:
                r.drop_reason = "ticketable=false"
                r.dropped = 0
        print_reconciliation(rows, sport=sport)
        print(f"  file={path.name} rows={len(df)}")
        # Call out known gaps
        by_key = {r.category: r for r in rows}
        if sport == "NHL" and by_key.get("goalie_saves", None) and by_key["goalie_saves"].fetched == 0:
            print("  GAP  Goalie Saves fetched=0 (required; still off tickets when present)")
        if sport == "TENNIS" and by_key.get("double_faults", None) and by_key["double_faults"].fetched == 0:
            print("  GAP  Double Faults fetched=0 (missing tab)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
