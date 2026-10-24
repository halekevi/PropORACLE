#!/usr/bin/env python3
"""Day-ahead PrizePicks Standard UNDER watchlist.

PrizePicks often posts a higher Standard number the night before, then cuts it
on gameday and parks the old number as a Demon Over. This report lists Standard
(not Demon/Goblin) unders for Eastern tomorrow so those lines can be used before
the cut.

Typical run (after WNBA step1 with --include-tomorrow):

  py -3.14 scripts/day_ahead_standard_under_watchlist.py --date 2026-08-16

Writes:
  data/reports/day_ahead_standard_unders_<board_date>.json
  data/reports/day_ahead_standard_unders_latest.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.step1_slate_date_filter import add_calendar_days, eastern_today_ymd

_ET = ZoneInfo("America/New_York")

SPORT_STEP1 = (
    ("WNBA", "wnba", "step1_wnba_props.csv"),
    ("MLB", "mlb", "step1_mlb_props.csv"),
    ("SOCCER", "soccer", "step1_soccer_props.csv"),
    ("TENNIS", "tennis", "step1_tennis_props.csv"),
)
SPORT_L5 = (
    ("WNBA", "wnba", ("step4_wnba_stats.csv", "step8_wnba_direction.csv", "step4_wnba_stats_day_ahead.csv")),
    ("MLB", "mlb", ("step4_mlb_with_stats.csv", "step8_mlb_direction.csv")),
    ("SOCCER", "soccer", ("step4_soccer_with_stats.csv", "step8_soccer_direction.csv", "step4_soccer_stats_day_ahead.csv")),
    ("TENNIS", "tennis", ("step4_tennis_with_stats.csv", "step8_tennis_direction.csv", "step4_tennis_stats_day_ahead.csv")),
)


def _norm_pick(val: object) -> str:
    s = str(val or "").strip().lower()
    if "dem" in s:
        return "Demon"
    if "gob" in s:
        return "Goblin"
    if "std" in s or s == "standard":
        return "Standard"
    return str(val or "").strip() or "Unknown"


def _player_key(val: object) -> str:
    return " ".join(str(val or "").strip().lower().split())


def _prop_key(val: object) -> str:
    return " ".join(str(val or "").strip().lower().replace("+", " ").split())


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


def _l5_lookup(df: pd.DataFrame, *, side: str = "under") -> dict[tuple[str, str], int]:
    if df is None or df.empty:
        return {}
    player_col = next((c for c in ("player", "Player", "player_name") if c in df.columns), None)
    prop_col = next((c for c in ("prop_type", "Prop", "prop") if c in df.columns), None)
    if str(side).strip().lower() == "over":
        l5_names = ("last5_over", "l5_over", "L5 Over")
    else:
        l5_names = ("last5_under", "l5_under", "L5 Under")
    l5_col = next((c for c in l5_names if c in df.columns), None)
    if not player_col or not prop_col or not l5_col:
        return {}
    out: dict[tuple[str, str], int] = {}
    l5 = pd.to_numeric(df[l5_col], errors="coerce")
    for idx, row in df.iterrows():
        key = (_player_key(row.get(player_col)), _prop_key(row.get(prop_col)))
        val = l5.loc[idx]
        if key[0] and key[1] and pd.notna(val):
            out[key] = int(val)
    return out


def _sport_l5_map(
    out_dir: Path,
    sport_dir: str,
    filenames: tuple[str, ...],
    *,
    side: str = "under",
) -> dict[tuple[str, str], int]:
    merged: dict[tuple[str, str], int] = {}
    for name in filenames:
        merged.update(_l5_lookup(_read_csv(out_dir / sport_dir / name), side=side))
    return merged


def _row_game_date(row: pd.Series, fallback: str) -> str:
    for col in ("game_date", "Game Date"):
        raw = str(row.get(col, "") or "").strip()[:10]
        if len(raw) == 10 and raw[4] == "-":
            return raw
    st = str(row.get("start_time", "") or "").strip()
    ts = pd.to_datetime(st, errors="coerce", utc=True)
    if pd.notna(ts):
        return ts.tz_convert(_ET).strftime("%Y-%m-%d")
    return fallback


def collect_watchlist(
    *,
    repo: Path,
    board_date: str,
    min_l5_under: int = 4,
    sports: tuple[str, ...] = ("WNBA", "MLB", "SOCCER", "TENNIS"),
    game_date: str = "",
) -> dict:
    out_dir = repo / "outputs" / board_date
    tomorrow = str(game_date or "").strip()[:10] or add_calendar_days(board_date, 1)
    want = {s.upper() for s in sports}
    l5_by_sport = {
        sport: _sport_l5_map(out_dir, folder, files)
        for sport, folder, files in SPORT_L5
        if sport in want
    }

    watchlist: list[dict] = []
    also_ran: list[dict] = []
    sources: list[str] = []

    for sport, folder, step1_name in SPORT_STEP1:
        if sport not in want:
            continue
        path = out_dir / folder / step1_name
        df = _read_csv(path)
        if df.empty:
            continue
        sources.append(str(path.relative_to(repo)).replace("\\", "/"))
        l5_map = l5_by_sport.get(sport, {})
        for _, row in df.iterrows():
            pick = _norm_pick(row.get("pick_type"))
            if pick != "Standard":
                continue
            game_date = _row_game_date(row, tomorrow)
            if game_date != tomorrow:
                continue
            player = str(row.get("player") or row.get("Player") or "").strip()
            prop = str(row.get("prop_type") or row.get("Prop") or "").strip()
            if not player or not prop:
                continue
            line = pd.to_numeric(pd.Series([row.get("line")]), errors="coerce").iloc[0]
            l5 = l5_map.get((_player_key(player), _prop_key(prop)))
            rec = {
                "sport": sport,
                "player": player,
                "prop": prop,
                "line": None if pd.isna(line) else float(line),
                "pick_type": "Standard",
                "side": "UNDER",
                "l5_under": l5,
                "game_date": game_date,
                "board_date": str(row.get("board_date") or board_date)[:10],
                "line_asof": str(row.get("line_asof") or row.get("board_date") or board_date)[:10],
                "team": str(row.get("team") or "").strip(),
                "opp_team": str(row.get("opp_team") or "").strip(),
            }
            if l5 is not None and l5 >= int(min_l5_under):
                watchlist.append(rec)
            else:
                also_ran.append(rec)

    def _sort_key(rec: dict) -> tuple:
        l5 = rec.get("l5_under")
        return (
            -(int(l5) if l5 is not None else -1),
            str(rec.get("sport") or ""),
            str(rec.get("player") or ""),
            str(rec.get("prop") or ""),
        )

    watchlist.sort(key=_sort_key)
    also_ran.sort(key=_sort_key)
    return {
        "generated_at": datetime.now(_ET).isoformat(timespec="seconds"),
        "board_date": board_date,
        "game_date": tomorrow,
        "min_l5_under": int(min_l5_under),
        "note": (
            "Standard UNDER only (Demons excluded). Day-ahead numbers are often cut "
            "on gameday and parked as Demon Overs."
        ),
        "sources": sources,
        "watchlist": watchlist,
        "also_ran": also_ran,
        "watchlist_n": len(watchlist),
        "also_ran_n": len(also_ran),
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Day-ahead Standard UNDER watchlist")
    ap.add_argument("--date", default="", help="Board/fetch date YYYY-MM-DD (default: Eastern today)")
    ap.add_argument(
        "--game-date",
        default="",
        help="Match day to keep (default: board date + 1). Same as --date for a 9PM initial fetch already dated as tomorrow.",
    )
    ap.add_argument("--min-l5-under", type=int, default=4)
    ap.add_argument(
        "--sports",
        default="WNBA,MLB,SOCCER,TENNIS",
        help="Comma sports to scan (default WNBA,MLB,SOCCER,TENNIS)",
    )
    ap.add_argument(
        "--output",
        default="",
        help="Optional JSON path (default data/reports/day_ahead_standard_unders_<date>.json)",
    )
    args = ap.parse_args()
    board_date = str(args.date or "").strip()[:10] or eastern_today_ymd()
    sports = tuple(s.strip().upper() for s in str(args.sports).split(",") if s.strip())
    payload = collect_watchlist(
        repo=_REPO,
        board_date=board_date,
        min_l5_under=int(args.min_l5_under),
        sports=sports,
        game_date=str(getattr(args, "game_date", "") or "").strip()[:10],
    )
    dated = _REPO / "data" / "reports" / f"day_ahead_standard_unders_{board_date}.json"
    latest = _REPO / "data" / "reports" / "day_ahead_standard_unders_latest.json"
    ui_latest = _REPO / "ui_runner" / "data" / "day_ahead_standard_unders_latest.json"
    out = Path(args.output) if str(args.output or "").strip() else dated
    if not out.is_absolute():
        out = _REPO / out
    _write_json(out, payload)
    _write_json(latest, payload)
    try:
        _write_json(ui_latest, payload)
    except OSError:
        pass
    print(
        f"[day-ahead unders] board={board_date} game={payload['game_date']} "
        f"watchlist={payload['watchlist_n']} also_ran={payload['also_ran_n']}"
    )
    print(f"  -> {out}")
    for rec in payload["watchlist"][:12]:
        l5 = rec.get("l5_under")
        l5s = f"{l5}/5" if l5 is not None else "L5 n/a"
        print(
            f"  {rec['sport']:<6} {rec['player']} {rec['prop']} "
            f"{rec['line']} Standard UNDER ({l5s}) game={rec['game_date']}"
        )
    if not payload["sources"]:
        print("  (no step1 CSVs found — run WNBA/MLB/soccer step1 with --include-tomorrow first)")
        return 1
    if payload["watchlist_n"] == 0 and payload["also_ran_n"] == 0:
        print("  (no Standard rows for Eastern tomorrow on those boards)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
