#!/usr/bin/env python3
"""Grader sanity checks → logs/pipeline_alerts.log + optional ntfy.

Flags (per sport, for one grade date):
  - games/actuals present but 0 graded rows
  - VOID share of graded rows > 50%
  - Goblin OVER decided hit rate < 50% (n >= 20)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from utils.pipeline_alerts import emit_pipeline_alert  # noqa: E402

SPORT_FILES = {
    "NBA": "graded_nba_{d}.xlsx",
    "NBA1H": "graded_nba1h_{d}.xlsx",
    "NBA1Q": "graded_nba1q_{d}.xlsx",
    "NHL": "graded_nhl_{d}.xlsx",
    "CFB": "graded_cfb_{d}.xlsx",
    "NFL": "graded_nfl_{d}.xlsx",
    "Soccer": "graded_soccer_{d}.xlsx",
    "Tennis": "graded_tennis_{d}.xlsx",
    "MLB": "graded_mlb_{d}.xlsx",
    "WNBA": "graded_wnba_{d}.xlsx",
}

ACTUALS_FILES = {
    "NBA": "actuals_nba_{d}.csv",
    "NBA1H": "actuals_nba1h_{d}.csv",
    "NBA1Q": "actuals_nba1q_{d}.csv",
    "NHL": "actuals_nhl_{d}.csv",
    "CFB": "actuals_cfb_{d}.csv",
    "NFL": "actuals_nfl_{d}.csv",
    "Soccer": "actuals_soccer_{d}.csv",
    "Tennis": "actuals_tennis_{d}.csv",
    "MLB": "actuals_mlb_{d}.csv",
    "WNBA": "actuals_wnba_{d}.csv",
}


def _read_graded(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
    except Exception:
        return pd.DataFrame()
    preferred = ["Box Raw", "graded", "ALL", "Sheet1"]
    sheet = next((s for s in preferred if s in xls.sheet_names), xls.sheet_names[0])
    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _col(df: pd.DataFrame, *names: str) -> pd.Series:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return df[lower[n.lower()]]
    return pd.Series([np.nan] * len(df))


def _actuals_nonempty(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 32:
        return False
    try:
        df = pd.read_csv(path, nrows=5)
        return len(df) > 0
    except Exception:
        return False


def check_sport(date_str: str, sport: str, out_dir: Path) -> list[str]:
    alerts: list[str] = []
    gpath = out_dir / SPORT_FILES[sport].format(d=date_str)
    apath = out_dir / ACTUALS_FILES[sport].format(d=date_str)
    has_actuals = _actuals_nonempty(apath)
    df = _read_graded(gpath)

    if has_actuals and (df is None or len(df) == 0):
        alerts.append(
            f"{sport} {date_str}: actuals on disk but 0 graded rows "
            f"({gpath.name} missing/empty)"
        )
        return alerts

    if df is None or len(df) == 0:
        return alerts

    result = _col(df, "result", "Result").astype(str).str.upper().str.strip()
    n = len(df)
    n_void = int(result.isin(["VOID", "PENDING"]).sum())
    void_share = n_void / n if n else 0.0
    if void_share > 0.50 and n >= 20:
        alerts.append(
            f"{sport} {date_str}: VOID/PENDING share {void_share:.0%} "
            f"({n_void}/{n}) exceeds 50%"
        )

    pick = _col(df, "pick_type", "Pick Type").astype(str)
    direction = _col(df, "direction", "bet_direction", "Direction").astype(str).str.upper()
    gob = pick.str.lower().str.contains("goblin", na=False) & direction.eq("OVER")
    decided = result.isin(["HIT", "MISS"])
    sub = df.loc[gob & decided]
    if len(sub) >= 20:
        hits = int((_col(sub, "result").astype(str).str.upper() == "HIT").sum())
        hr = hits / len(sub)
        if hr < 0.50:
            alerts.append(
                f"{sport} {date_str}: Goblin OVER decided HR {hr:.1%} "
                f"({hits}/{len(sub)}) under 50%"
            )
    return alerts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--root", default=str(ROOT))
    ap.add_argument("--notify", action="store_true", default=True)
    ap.add_argument("--no-notify", action="store_true")
    args = ap.parse_args()
    notify = bool(args.notify) and not bool(args.no_notify)
    date_str = str(args.date).strip()[:10]
    out_dir = Path(args.root) / "outputs" / date_str
    all_alerts: list[str] = []
    for sport in SPORT_FILES:
        all_alerts.extend(check_sport(date_str, sport, out_dir))

    if not all_alerts:
        print(f"[grader-sanity] {date_str}: OK")
        return 0

    for msg in all_alerts:
        print(f"[grader-sanity] WARN: {msg}")
        emit_pipeline_alert("grader-sanity", msg, notify=notify)

    summary = {
        "date": date_str,
        "alerts": all_alerts,
        "n_alerts": len(all_alerts),
    }
    rep = Path(args.root) / "data" / "reports" / "grader_sanity"
    rep.mkdir(parents=True, exist_ok=True)
    (rep / f"grader_sanity_{date_str}.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
