#!/usr/bin/env python3
"""Print the modal Game Date from an NBA step8 workbook (YYYY-MM-DD)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", required=True)
    args = ap.parse_args()
    path = Path(args.xlsx)
    if not path.is_file():
        return 0
    try:
        df = pd.read_excel(path, sheet_name="ALL", header=0)
    except Exception:
        return 0
    col = "Game Date" if "Game Date" in df.columns else (
        "game_date" if "game_date" in df.columns else None
    )
    if col is None:
        return 0
    series = df[col].dropna().astype(str).str[:10]
    series = series[series.str.match(r"\d{4}-\d{2}-\d{2}", na=False)]
    if series.empty:
        return 0
    print(series.mode().iloc[0])
    return 0


if __name__ == "__main__":
    sys.exit(main())
