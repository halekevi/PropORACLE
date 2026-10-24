#!/usr/bin/env python3
"""
Backfill college football (CFB) player boxscores from ESPN into:
  Sports/CFB/data/cache/cfb_boxscore_cache.csv

Use this so step5b can attach L5/L10 early in the season (needs prior-year games in cache).

Examples:
  python scripts/backfill_cfb_espn_range.py --preset full-2025 --season 2025
  python scripts/backfill_cfb_espn_range.py --from 2024-08-24 --to 2025-01-20 --season 2024
  python scripts/backfill_cfb_espn_range.py --preset bowls-2025 --season 2025

Presets (pair with --season):
  full-2024      Aug 2024 – Jan 2025 (prior season + bowls)
  full-2025      Aug 2025 – Jan 2026
  regular-2025   Aug – early Dec 2025
  bowls-2025     Dec 2025 – Jan 2026
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Tuple

import pandas as pd

REPO = Path(__file__).resolve().parent.parent

PRESETS: dict[str, Tuple[str, str]] = {
    "full-2024": ("2024-08-24", "2025-01-20"),
    "full-2025": ("2025-08-23", "2026-01-19"),
    "regular-2025": ("2025-08-23", "2025-12-07"),
    "bowls-2025": ("2025-12-14", "2026-01-19"),
}

NFL_PRESETS: dict[str, Tuple[str, str]] = {
    "full-2025": ("2025-09-04", "2026-02-09"),
    "regular-2025": ("2025-09-04", "2026-01-05"),
}

DEFAULT_CACHE = REPO / "Sports" / "CFB" / "data" / "cache" / "cfb_boxscore_cache.csv"
DEFAULT_NFL_CACHE = REPO / "Sports" / "NFL" / "data" / "cache" / "nfl_boxscore_cache.csv"


def _load_cfb_step5b():
    path = REPO / "Sports" / "CFB" / "scripts" / "pipeline" / "step5b_attach_boxscore_stats.py"
    spec = importlib.util.spec_from_file_location("cfb_step5b", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _expand_dates(d0: date, d1: date) -> list[date]:
    out: list[date] = []
    cur = d0
    while cur <= d1:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _fmt_game_date(ds: str) -> str:
    s = str(ds or "").strip().replace("-", "")[:8]
    if len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return str(ds)[:10]


def _attach_opponent(rows: list[dict], t1: str, t2: str) -> None:
    for row in rows:
        tid = str(row.get("team_id", "")).strip()
        if tid and t1 and t2:
            row["opp_team_id"] = t2 if tid == t1 else (t1 if tid == t2 else "")


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill CFB/NFL ESPN games into boxscore cache CSV")
    ap.add_argument("--league", choices=("cfb", "nfl"), default="cfb")
    ap.add_argument("--preset", default="")
    ap.add_argument("--from", dest="date_from", default="")
    ap.add_argument("--to", dest="date_to", default="")
    ap.add_argument("--season", required=True, help="SEASON label stored on cache rows (e.g. 2025)")
    ap.add_argument("--cache", default="")
    ap.add_argument("--sleep", type=float, default=0.12)
    ap.add_argument("--workers", type=int, default=6, help="Parallel summary fetches for pending events")
    ap.add_argument(
        "--refresh-missing",
        action="store_true",
        help="Re-fetch cached events that still lack LONG / ATT / FG columns so week-1 L5 can use 2025 tape.",
    )
    args = ap.parse_args()

    league = str(args.league).strip().lower()
    tag = f"backfill-{league}"
    preset_map = NFL_PRESETS if league == "nfl" else PRESETS
    if args.preset:
        if args.preset not in preset_map:
            raise SystemExit(f"Unknown preset {args.preset!r} for {league}. Choose: {', '.join(sorted(preset_map))}")
        d0s, d1s = preset_map[args.preset]
        d0 = date.fromisoformat(d0s)
        d1 = date.fromisoformat(d1s)
        print(f"[{tag}] preset {args.preset}: {d0} .. {d1}")
    else:
        if not str(args.date_from).strip() or not str(args.date_to).strip():
            raise SystemExit("Provide --preset or both --from and --to (YYYY-MM-DD).")
        d0 = date.fromisoformat(args.date_from.strip())
        d1 = date.fromisoformat(args.date_to.strip())
    if d1 < d0:
        raise SystemExit("--to must be on or after --from")

    mod = _load_cfb_step5b()
    mod.ESPN_LEAGUE = "nfl" if league == "nfl" else "college-football"

    cache_path = Path(args.cache) if str(args.cache).strip() else (
        DEFAULT_NFL_CACHE if league == "nfl" else DEFAULT_CACHE
    )
    if cache_path.exists():
        cache = pd.read_csv(cache_path, dtype=str, encoding="utf-8-sig").fillna("")
    else:
        cache = pd.DataFrame()

    existing_events: set[str] = set()
    missing_stat_events: set[str] = set()
    _NEED_COLS = ("REC_LONG", "RUSH_LONG", "PASS_LONG", "RUSH_ATT", "PASS_ATT", "KICK_FG")
    if not cache.empty and "event_id" in cache.columns:
        existing_events = set(cache["event_id"].astype(str).unique())
        if args.refresh_missing:
            have = {c: c in cache.columns for c in _NEED_COLS}
            for eid, grp in cache.groupby("event_id"):
                eid_s = str(eid)
                ok = False
                for col, present in have.items():
                    if not present:
                        continue
                    vals = grp[col].astype(str).str.strip()
                    if vals.isin(["", "nan", "None"]).all():
                        continue
                    if (pd.to_numeric(vals, errors="coerce").fillna(0) > 0).any():
                        ok = True
                        break
                if not ok:
                    missing_stat_events.add(eid_s)
            existing_events -= missing_stat_events
            print(
                f"[{tag}] --refresh-missing: {len(missing_stat_events)} cached events "
                "will be re-fetched for LONG/ATT/FG"
            )

    # Collect pending events across calendar (no slate team filter)
    pending: list[tuple[str, str, str, str]] = []
    seen: set[str] = set()
    dates = _expand_dates(d0, d1)
    print(f"[{tag}] Scanning {len(dates)} calendar days...")
    for i, d in enumerate(dates):
        yyyymmdd = d.strftime("%Y%m%d")
        sb = mod.pull_scoreboard(yyyymmdd)
        for eid, t1, t2, ds in mod.extract_events(sb):
            if eid in seen:
                continue
            seen.add(eid)
            if eid not in existing_events:
                pending.append((eid, t1, t2, ds))
        if (i + 1) % 30 == 0:
            print(f"  ... {i + 1}/{len(dates)} days, {len(pending)} events pending")

    print(f"[{tag}] {len(seen)} events in range | {len(existing_events)} already cached | {len(pending)} to fetch")

    new_rows: list[dict] = []
    if pending:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _one(item: tuple[str, str, str, str]) -> list[dict]:
            eid, t1, t2, ds = item
            time.sleep(args.sleep)
            summ = mod.pull_summary(eid)
            rows = mod.parse_players(summ, game_date=_fmt_game_date(ds), event_id=eid)
            _attach_opponent(rows, t1, t2)
            for r in rows:
                r["SEASON"] = str(args.season)
            return rows

        with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
            futures = {pool.submit(_one, item): item[0] for item in pending}
            done = 0
            for fut in as_completed(futures):
                done += 1
                try:
                    rows = fut.result()
                    if rows:
                        new_rows.extend(rows)
                        existing_events.add(futures[fut])
                except Exception as exc:
                    print(f"  [WARN] event failed: {exc}")
                if done % 50 == 0:
                    print(f"  ... fetched {done}/{len(pending)} events ({len(new_rows)} player-rows)")

    print(f"[{tag}] Fetched {len(new_rows)} new player-rows from {len(pending)} events")

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        if missing_stat_events:
            cache = cache[~cache["event_id"].astype(str).isin(missing_stat_events)].copy() if not cache.empty else cache
        cache = pd.concat([cache, new_df], ignore_index=True) if not cache.empty else new_df
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache.to_csv(cache_path, index=False, encoding="utf-8-sig")
        print(f"Cache updated → {cache_path}  ({len(cache)} rows)")
        if "SEASON" in cache.columns:
            print(cache["SEASON"].value_counts().head(10).to_string())
    else:
        print("No new rows (range already in cache or no games on ESPN for these dates).")


if __name__ == "__main__":
    main()
