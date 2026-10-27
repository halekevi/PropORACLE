#!/usr/bin/env python3
"""Exit 0 when earliest slate tip is 50–70 minutes away (T-60 collect window).

  py -3.14 scripts/injury_t60_gate.py --date 2026-10-27
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")
LO_MIN = 50
HI_MIN = 70


def _parse_tip(raw: object, date: str) -> datetime | None:
    s = str(raw or "").strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ET)
        return dt.astimezone(ET)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %I:%M %p", "%H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%H:%M":
                y, m, d = (int(x) for x in date.split("-"))
                dt = dt.replace(year=y, month=m, day=d)
            return dt.replace(tzinfo=ET)
        except Exception:
            continue
    return None


def earliest_tip(root: Path, date: str) -> datetime | None:
    base = root / "outputs" / date
    if not base.is_dir():
        return None
    tips: list[datetime] = []
    for path in base.rglob("step8_*direction_clean*.xlsx"):
        try:
            df = pd.read_excel(path, nrows=500)
        except Exception:
            continue
        for col in ("game_time", "Game Time", "start_time", "Start Time", "commence_time", "tip"):
            if col not in df.columns:
                continue
            for v in df[col].tolist():
                dt = _parse_tip(v, date)
                if dt is not None:
                    tips.append(dt)
            break
    return min(tips) if tips else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--root", default="")
    ap.add_argument("--now", default="", help="ISO override for tests")
    args = ap.parse_args()
    root = Path(args.root).resolve() if str(args.root).strip() else _REPO
    date = (args.date or datetime.now(ET).strftime("%Y-%m-%d")).strip()[:10]
    now = (
        datetime.fromisoformat(args.now.replace("Z", "+00:00")).astimezone(ET)
        if str(args.now).strip()
        else datetime.now(ET)
    )
    tip = earliest_tip(root, date)
    if tip is None:
        print("no_tip")
        return 2
    delta = (tip - now).total_seconds() / 60.0
    msg = f"tip={tip.isoformat()} delta_min={delta:.1f}"
    print(msg)
    if LO_MIN <= delta <= HI_MIN:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
