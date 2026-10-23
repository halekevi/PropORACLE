#!/usr/bin/env python3
"""Append one line to logs/late_fetch_sport_rebuilds.jsonl for this late-fetch window.

Reads the stamp ``data/cache/last_line_window.json`` (written by stamp_fetch_window.py) for the
new-row counts, so the PowerShell caller only passes what it actually did.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.bet_windows import LINE_WINDOW_CACHE, append_sport_rebuild_log  # noqa: E402


def _csv(raw: str) -> list[str]:
    return [s.strip().upper() for s in str(raw or "").split(",") if s.strip()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--window", default="")
    ap.add_argument("--mode", required=True, choices=["full", "sport", "none"])
    ap.add_argument("--rebuilt", default="", help="comma list of sports rebuilt")
    ap.add_argument("--outcome", default="")
    a = ap.parse_args()

    stamp: dict = {}
    try:
        stamp = json.loads(Path(LINE_WINDOW_CACHE).read_text(encoding="utf-8"))
    except Exception:
        pass
    path = append_sport_rebuild_log(
        date=a.date,
        window=a.window or str(stamp.get("window") or ""),
        mode=a.mode,
        triggered=list(stamp.get("sport_rebuild_triggered") or []),
        rebuilt=_csv(a.rebuilt),
        new_by_sport=dict(stamp.get("new_by_sport") or {}),
        outcome=a.outcome,
    )
    print(f"[sport-rebuild-log] mode={a.mode} rebuilt={a.rebuilt or '-'} -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
