#!/usr/bin/env python3
"""Backfill start_time onto archived Goblin-70 legs from that day's step1 CSV.

PrizePicks often stamps T23:00:00 when the real tip is unknown. We still copy
whatever step1 has onto the archive leg so grading can see placeholder vs real
starts without re-joining the board later.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from report_gated_construction_efficiency import discover_g70_archives  # noqa: E402
from utils.ticket_70_pool import norm_sport  # noqa: E402


def _starts(day: str, sport: str) -> dict[str, str]:
    by: dict[str, Counter] = defaultdict(Counter)
    roots = [MAIN / "outputs" / day, ROOT / "outputs" / day]
    patterns = {
        "tennis": ["tennis/step1_tennis_props.csv", "step1_tennis_props.csv"],
        "soccer": ["soccer/step1_soccer_props.csv", "step1_soccer_props.csv"],
        "mlb": ["mlb/step1_mlb_props.csv", "step1_mlb_props.csv"],
        "wnba": ["wnba/step1_wnba_props.csv", "step1_wnba_props.csv"],
        "cfb": ["cfb/step1_cfb_props.csv", "step1_cfb_props.csv"],
        "nfl": ["nfl/step1_nfl_props.csv", "step1_nfl_props.csv"],
    }
    for root in roots:
        for rel in patterns.get(sport, []):
            path = root / rel
            if not path.is_file():
                continue
            try:
                df = pd.read_csv(path, dtype=str, encoding_errors="replace")
            except Exception:
                continue
            cols = {str(c).lower(): c for c in df.columns}
            pc = cols.get("player") or cols.get("player_name")
            sc = cols.get("start_time") or cols.get("game_time")
            if not (pc and sc):
                continue
            for player, start in zip(df[pc], df[sc], strict=False):
                if not player or not start:
                    continue
                name = str(player).strip().lower()
                stamp = str(start).strip()
                if name and stamp and stamp.lower() not in {"nan", "none", "nat"}:
                    by[name][stamp] += 1
    return {n: c.most_common(1)[0][0] for n, c in by.items() if c}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="Write patched archives")
    ap.add_argument("--sport", default="tennis", help="Sport folder for step1 lookup")
    args = ap.parse_args()

    filled = already = missing = 0
    for date_s, path in discover_g70_archives(start=None, end=None):
        raw = json.loads(path.read_text(encoding="utf-8"))
        starts = _starts(date_s, args.sport.lower())
        changed = False
        tickets = list(raw.get("tickets") or [])
        groups = list(raw.get("groups") or [])
        if not tickets and groups:
            for g in groups:
                tickets.extend(t for t in (g.get("tickets") or []) if isinstance(t, dict))

        def _patch(leg: dict) -> None:
            nonlocal filled, already, missing, changed
            if not isinstance(leg, dict):
                return
            if norm_sport(str(leg.get("sport") or "")).lower() != args.sport.lower():
                return
            if str(leg.get("start_time") or "").strip():
                already += 1
                return
            player = str(leg.get("player") or "").strip().lower()
            st = starts.get(player, "")
            if st:
                leg["start_time"] = st
                filled += 1
                changed = True
            else:
                missing += 1

        for t in raw.get("tickets") or []:
            if isinstance(t, dict):
                for leg in t.get("legs") or []:
                    _patch(leg)
        for g in raw.get("groups") or []:
            if not isinstance(g, dict):
                continue
            for t in g.get("tickets") or []:
                if isinstance(t, dict):
                    for leg in t.get("legs") or []:
                        _patch(leg)
        if changed and args.write:
            path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"wrote {path.name}")
    print(f"filled={filled} already={already} missing_step1={missing} write={args.write}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
