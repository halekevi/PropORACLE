#!/usr/bin/env python3
"""Day-ahead PrizePicks Standard line snapshot (overs + unders).

Captures tonight's Standard numbers for Eastern tomorrow so gameday can
diff cuts. Key: sport|player|prop|pick_type|game_date.

Typical run (after step1 with --include-tomorrow):

  py -3.14 scripts/snapshot_day_ahead_lines.py --date 2026-08-17

Writes:
  data/reports/day_ahead_standard_lines_<board_date>.json
  data/reports/day_ahead_standard_lines_latest.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO / "scripts"
for _p in (str(_REPO), str(_SCRIPTS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from scripts.day_ahead_standard_under_watchlist import (  # noqa: E402
    SPORT_L5,
    SPORT_STEP1,
    _ET,
    _norm_pick,
    _player_key,
    _prop_key,
    _read_csv,
    _row_game_date,
    _sport_l5_map,
    _write_json,
    collect_watchlist,
)
from utils.step1_slate_date_filter import add_calendar_days, eastern_today_ymd  # noqa: E402


def snapshot_key(rec: dict) -> str:
    return "|".join(
        [
            str(rec.get("sport") or "").upper(),
            _player_key(rec.get("player")),
            _prop_key(rec.get("prop")),
            str(rec.get("pick_type") or "Standard"),
            str(rec.get("game_date") or "")[:10],
        ]
    )


def collect_standard_snapshot(
    *,
    repo: Path,
    board_date: str,
    min_l5: int = 4,
    sports: tuple[str, ...] = ("WNBA", "MLB", "SOCCER", "TENNIS"),
) -> dict:
    out_dir = repo / "outputs" / board_date
    tomorrow = add_calendar_days(board_date, 1)
    want = {s.upper() for s in sports}
    l5_under = {
        sport: _sport_l5_map(out_dir, folder, files, side="under")
        for sport, folder, files in SPORT_L5
        if sport in want
    }
    l5_over = {
        sport: _sport_l5_map(out_dir, folder, files, side="over")
        for sport, folder, files in SPORT_L5
        if sport in want
    }

    rows: list[dict] = []
    sources: list[str] = []
    by_sport: dict[str, int] = {}

    for sport, folder, step1_name in SPORT_STEP1:
        if sport not in want:
            continue
        path = out_dir / folder / step1_name
        df = _read_csv(path)
        if df.empty:
            continue
        sources.append(str(path.relative_to(repo)).replace("\\", "/"))
        n_sport = 0
        under_map = l5_under.get(sport, {})
        over_map = l5_over.get(sport, {})
        for _, row in df.iterrows():
            pick = _norm_pick(row.get("pick_type"))
            if pick != "Standard":
                continue
            game_date = _row_game_date(row, tomorrow)
            if game_date != tomorrow:
                continue
            player = str(row.get("player") or row.get("Player") or row.get("player_name") or "").strip()
            prop = str(row.get("prop_type") or row.get("Prop") or "").strip()
            if not player or not prop:
                continue
            line = pd.to_numeric(pd.Series([row.get("line")]), errors="coerce").iloc[0]
            key = (_player_key(player), _prop_key(prop))
            rec = {
                "sport": sport,
                "player": player,
                "prop": prop,
                "line": None if pd.isna(line) else float(line),
                "pick_type": "Standard",
                "dir": "two-way",
                "l5_under": under_map.get(key),
                "l5_over": over_map.get(key),
                "game_date": game_date,
                "board_date": str(row.get("board_date") or board_date)[:10],
                "line_asof": str(row.get("line_asof") or row.get("board_date") or board_date)[:10],
                "team": str(row.get("team") or "").strip(),
                "opp_team": str(row.get("opp_team") or "").strip(),
            }
            rec["key"] = snapshot_key(rec)
            rows.append(rec)
            n_sport += 1
        by_sport[sport] = n_sport

    def _over_sort(rec: dict) -> tuple:
        l5 = rec.get("l5_over")
        return (
            -(int(l5) if l5 is not None else -1),
            str(rec.get("sport") or ""),
            str(rec.get("player") or ""),
            str(rec.get("prop") or ""),
        )

    def _under_sort(rec: dict) -> tuple:
        l5 = rec.get("l5_under")
        return (
            -(int(l5) if l5 is not None else -1),
            str(rec.get("sport") or ""),
            str(rec.get("player") or ""),
            str(rec.get("prop") or ""),
        )

    rows.sort(key=_under_sort)
    notable_overs = [r for r in rows if r.get("l5_over") is not None and int(r["l5_over"]) >= int(min_l5)]
    notable_overs.sort(key=_over_sort)
    notable_unders = [r for r in rows if r.get("l5_under") is not None and int(r["l5_under"]) >= int(min_l5)]
    notable_unders.sort(key=_under_sort)
    by_key = {r["key"]: r for r in rows}

    return {
        "generated_at": datetime.now(_ET).isoformat(timespec="seconds"),
        "board_date": board_date,
        "game_date": tomorrow,
        "line_asof": board_date,
        "min_l5": int(min_l5),
        "note": (
            "Full Standard board (two-way overs + unders). Day-ahead numbers are often "
            "cut on gameday; unders should be captured tonight, overs usually improve "
            "after the cut. Tuesday morning: re-fetch with line_asof=game_date and diff "
            f"against this snapshot (line_asof={board_date})."
        ),
        "sources": sources,
        "counts": {
            "all_standards": len(rows),
            "by_sport": by_sport,
            "notable_unders_l5": len(notable_unders),
            "notable_overs_l5": len(notable_overs),
        },
        "by_key": by_key,
        "all_standards": rows,
        "standard_overs": notable_overs,
        "standard_unders": notable_unders,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Day-ahead Standard line snapshot (over+under)")
    ap.add_argument("--date", default="", help="Board/fetch date YYYY-MM-DD (default: Eastern today)")
    ap.add_argument("--min-l5", type=int, default=4)
    ap.add_argument(
        "--sports",
        default="WNBA,MLB,SOCCER,TENNIS",
        help="Comma sports to scan",
    )
    ap.add_argument("--skip-watchlist", action="store_true", help="Do not refresh the UNDER watchlist JSON")
    args = ap.parse_args()
    board_date = str(args.date or "").strip()[:10] or eastern_today_ymd()
    sports = tuple(s.strip().upper() for s in str(args.sports).split(",") if s.strip())
    payload = collect_standard_snapshot(
        repo=_REPO,
        board_date=board_date,
        min_l5=int(args.min_l5),
        sports=sports,
    )
    dated = _REPO / "data" / "reports" / f"day_ahead_standard_lines_{board_date}.json"
    latest = _REPO / "data" / "reports" / "day_ahead_standard_lines_latest.json"
    ui_latest = _REPO / "ui_runner" / "data" / "day_ahead_standard_lines_latest.json"
    _write_json(dated, payload)
    _write_json(latest, payload)
    try:
        _write_json(ui_latest, payload)
    except OSError:
        pass

    counts = payload["counts"]
    print(
        f"[day-ahead lines] board={board_date} game={payload['game_date']} "
        f"standards={counts['all_standards']} "
        f"unders_L5>={args.min_l5}={counts['notable_unders_l5']} "
        f"overs_L5>={args.min_l5}={counts['notable_overs_l5']}"
    )
    print(f"  sports: {counts['by_sport']}")
    print(f"  -> {dated}")
    print("  Top Standard unders:")
    for rec in payload["standard_unders"][:12]:
        l5 = rec.get("l5_under")
        l5s = f"{l5}/5 U" if l5 is not None else "L5 n/a"
        print(
            f"    {rec['sport']:<7} {rec['player']} {rec['prop']} "
            f"{rec['line']} ({l5s})"
        )
    print("  Notable Standard overs (usually wait for gameday cut):")
    for rec in payload["standard_overs"][:8]:
        l5 = rec.get("l5_over")
        l5s = f"{l5}/5 O" if l5 is not None else "L5 n/a"
        print(
            f"    {rec['sport']:<7} {rec['player']} {rec['prop']} "
            f"{rec['line']} ({l5s})"
        )
    if not payload["sources"]:
        print("  (no step1 CSVs found — run sport step1 with --include-tomorrow first)")
        return 1

    if not args.skip_watchlist:
        watch = collect_watchlist(
            repo=_REPO,
            board_date=board_date,
            min_l5_under=int(args.min_l5),
            sports=sports,
        )
        watch["standard_overs"] = payload["standard_overs"]
        watch["all_standards_n"] = counts["all_standards"]
        w_dated = _REPO / "data" / "reports" / f"day_ahead_standard_unders_{board_date}.json"
        w_latest = _REPO / "data" / "reports" / "day_ahead_standard_unders_latest.json"
        w_ui = _REPO / "ui_runner" / "data" / "day_ahead_standard_unders_latest.json"
        _write_json(w_dated, watch)
        _write_json(w_latest, watch)
        try:
            _write_json(w_ui, watch)
        except OSError:
            pass
        print(
            f"[day-ahead unders] watchlist={watch['watchlist_n']} also_ran={watch['also_ran_n']} "
            f"-> {w_dated}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
