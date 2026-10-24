#!/usr/bin/env python3
"""
Export Vegas / sharp consensus player-prop lines from The Odds API.

Uses the same snapshot cache as utils.line_movement (Pinnacle → Circa → DraftKings).
Does not burn extra quota when step5 already fetched today's cache.

Examples:
  py -3.14 scripts/fetch_vegas_player_props.py --output outputs/2026-08-30/vegas_props.csv
  py -3.14 scripts/fetch_vegas_player_props.py --sports MLB,WNBA --date 2026-08-30 -o vegas.csv

Tennis and golf have no Odds API player props and are skipped.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pickem_step1_schema import VEGAS_OUTPUT_COLUMNS  # noqa: E402
from utils.line_movement import (  # noqa: E402
    SPORT_LINE_MOVEMENT_PRESETS,
    fetch_line_snapshot,
    flatten_vegas_lines,
    markets_for_sport_key,
)
from utils.pp_vegas_gap import archive_vegas_csv  # noqa: E402

_SPORT_ALIASES: dict[str, str] = {
    "nba": "NBA",
    "nhl": "NHL",
    "mlb": "MLB",
    "wnba": "WNBA",
    "soccer": "Soccer",
    "epl": "Soccer",
    "nfl": "NFL",
    "cfb": "CFB",
    "ncaaf": "CFB",
}


def _expand_sports(raw: str) -> list[str]:
    s = (raw or "ALL").strip()
    if not s or s.upper() == "ALL":
        return list(SPORT_LINE_MOVEMENT_PRESETS.keys())
    out: list[str] = []
    seen: set[str] = set()
    for part in s.split(","):
        key = _SPORT_ALIASES.get(part.strip().lower(), part.strip())
        if key in SPORT_LINE_MOVEMENT_PRESETS and key not in seen:
            out.append(key)
            seen.add(key)
        elif key and key not in SPORT_LINE_MOVEMENT_PRESETS:
            print(f"[vegas] skip unknown sport {part!r} (no Odds API player-prop preset)")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Vegas / Odds API player props → CSV")
    ap.add_argument("--sports", default="ALL", help="Comma list or ALL (every Odds API preset: MLB,WNBA,NBA,NHL,Soccer,NFL,CFB)")
    ap.add_argument("--date", default="", help="Cache date YYYY-MM-DD (default: today)")
    ap.add_argument("-o", "--output", default="vegas_props.csv")
    ap.add_argument("--force-refresh", action="store_true", help="Bypass Odds API snapshot cache")
    args = ap.parse_args()

    sports = _expand_sports(args.sports)
    if not sports:
        print("[vegas] no sports selected")
        pd.DataFrame(columns=VEGAS_OUTPUT_COLUMNS).to_csv(args.output, index=False, encoding="utf-8-sig")
        sys.exit(1)

    cache_date = (args.date or "").strip()[:10] or None
    rows: list[dict] = []
    for label in sports:
        preset = SPORT_LINE_MOVEMENT_PRESETS[label]
        sport_key = str(preset.get("sport_key") or "")
        markets = list(preset.get("markets") or markets_for_sport_key(sport_key))
        extra = [str(k) for k in (preset.get("extra_sport_keys") or []) if str(k).strip()]
        keys = [sport_key] + [k for k in extra if k and k != sport_key]
        for sk in keys:
            print(f"[vegas] {label} ({sk}) markets={len(markets)}")
            snap = fetch_line_snapshot(
                sk,
                markets,
                force_refresh=bool(args.force_refresh),
                cache_date=cache_date,
                allow_stale=False,
            )
            part = flatten_vegas_lines(snap, board_sport=label)
            print(f"[vegas] {label}/{sk}: {len(part)} preferred-book lines")
            rows.extend(part)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        df = pd.DataFrame(columns=VEGAS_OUTPUT_COLUMNS)
    else:
        df = pd.DataFrame(rows)
        for c in VEGAS_OUTPUT_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        df = df[VEGAS_OUTPUT_COLUMNS]
        df["line"] = pd.to_numeric(df["line"], errors="coerce")
        df = df[df["line"].notna()].drop_duplicates(
            subset=["board_sport", "player", "odds_market", "line"], keep="first"
        )
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[ok] {len(df)} rows -> {out_path}")
    date_key = cache_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if len(df) > 0:
        archived = archive_vegas_csv(out_path, date_key, _REPO_ROOT)
        if archived:
            print(f"[ok] archived {archived}")
    else:
        print("[vegas] skip archive (0 rows) — keep any prior snapshot for this date")


if __name__ == "__main__":
    main()
