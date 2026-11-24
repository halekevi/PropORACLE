#!/usr/bin/env python3
"""Collect multi-sport injury snapshots (Phase 1).

Writes data/injuries/<date>/<sport>_<HHMM>.json (never overwrite) and
data/injuries/injuries_latest.csv.

  py -3.14 scripts/collect_injuries_daily.py
  py -3.14 scripts/collect_injuries_daily.py --date 2026-10-27 --sports NBA,NFL
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from utils.injury_collect import (  # noqa: E402
    LIVE_SPORTS,
    captured_at_iso,
    collect_sport,
    hhmm_et,
    injuries_dir,
    merge_latest_csv,
    now_et,
    write_snapshot,
)

ET = ZoneInfo("America/New_York")


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily injury collect (timestamped snapshots)")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default: today ET)")
    ap.add_argument(
        "--sports",
        default="",
        help="Comma list (default: live sports set)",
    )
    ap.add_argument("--root", default="", help="Repo root override")
    ap.add_argument(
        "--no-sidecars",
        action="store_true",
        help="Skip outputs/<date>/injuries_<sport>_<date>.csv",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve() if str(args.root).strip() else _REPO
    date_str = (args.date or now_et().strftime("%Y-%m-%d")).strip()[:10]
    sports = [s.strip().upper() for s in (args.sports or ",".join(LIVE_SPORTS)).split(",") if s.strip()]
    captured = captured_at_iso()
    stamp = hhmm_et()
    raw_dir = injuries_dir(date_str, root=root) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    summary: dict = {"date": date_str, "captured_at": captured, "hhmm": stamp, "sports": {}}
    for sport in sports:
        rows, meta = collect_sport(sport, date_str, captured, raw_dir=raw_dir)
        path = write_snapshot(sport, date_str, rows, meta, root=root, hhmm=stamp)
        summary["sports"][sport] = {"n": len(rows), "path": str(path.relative_to(root)), "meta": meta}
        all_rows.extend(rows)
        print(f"[injuries] {sport}: {len(rows)} rows -> {path.name}")

    latest = merge_latest_csv(all_rows, root=root, replace_sports=set(sports))
    summary["latest_csv"] = str(latest.relative_to(root))
    summary["n_total"] = len(all_rows)
    summary_path = injuries_dir(date_str, root=root) / f"_collect_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[injuries] latest -> {latest} ({len(all_rows)} rows across {len(sports)} sports)")
    if args.no_sidecars:
        return 0
    missing = write_slate_sidecars(date_str, root, sports)
    missing += write_football_sidecars(date_str, root, sports, all_rows)
    return 2 if missing else 0


# outputs/<date>/injuries_<sport>_<date>.csv is what step7 rankers, combined, and usage
# redistribution read for the slate. Without it injury flags stay empty all day.
_SIDECAR_SPORTS = ("NBA", "NHL", "CBB", "WCBB", "WNBA", "MLB")
# Months a sport's ESPN injury list should never be empty when it has a slate.
_REQUIRED_MONTHS = {
    "NBA": {10, 11, 12, 1, 2, 3, 4, 5, 6},
    "NHL": {10, 11, 12, 1, 2, 3, 4, 5, 6},
}


def write_slate_sidecars(date_str: str, root: Path, sports: list[str]) -> list[str]:
    from espn_injuries import write_injuries_for_date

    want = [s for s in _SIDECAR_SPORTS if s in sports or (s == "WCBB" and "CBB" in sports)]
    month = int(date_str[5:7])
    out_dir = root / "outputs" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    missing: list[str] = []
    for sport in want:
        dest = out_dir / f"injuries_{sport.lower()}_{date_str}.csv"
        tmp = dest.with_suffix(".csv.tmp")
        n = -1
        try:
            n = write_injuries_for_date(sport, date_str, tmp)
        except Exception as exc:  # noqa: BLE001
            print(f"[injuries] {sport} sidecar fetch failed: {exc}")
        required = month in _REQUIRED_MONTHS.get(sport, set())
        if n > 0 or (n == 0 and not required):
            tmp.replace(dest)
            print(f"[injuries] {sport} sidecar -> {dest.name} ({n} rows)")
            continue
        tmp.unlink(missing_ok=True)
        kept = dest.is_file()
        why = "fetch failed" if n < 0 else "0 rows in season"
        print(f"[injuries] {sport} sidecar {why}; {'kept previous file' if kept else 'NO FILE'}")
        if not kept or n == 0:
            missing.append(sport)
    if missing:
        try:
            from utils.pipeline_alerts import emit_pipeline_alert

            emit_pipeline_alert(
                "injuries-missing",
                f"{date_str}: injury sidecar missing or empty for {', '.join(missing)} "
                f"(outputs/{date_str}/injuries_<sport>_{date_str}.csv).",
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[injuries] alert failed: {exc}")
    return missing


# Football boards span the week, so the sidecar is the league-wide list collected above
# (ESPN + nflverse for NFL), not a per-date scoreboard pull.
_FOOTBALL_REQUIRED_MONTHS = {"NFL": {9, 10, 11, 12, 1}}


def write_football_sidecars(date_str: str, root: Path, sports: list[str], rows: list[dict]) -> list[str]:
    import pandas as pd

    month = int(date_str[5:7])
    missing: list[str] = []
    for sport in ("NFL", "CFB"):
        if sport not in sports:
            continue
        sub = [r for r in rows if str(r.get("sport", "")).upper() == sport]
        required = month in _FOOTBALL_REQUIRED_MONTHS.get(sport, set())
        if not sub and required:
            print(f"[injuries] {sport} sidecar 0 rows in season; kept previous file")
            missing.append(sport)
            continue
        df = pd.DataFrame(
            {
                "date": date_str,
                "sport": sport,
                "team": [r.get("team", "") for r in sub],
                "player": [r.get("player", "") for r in sub],
                "injury_status": [r.get("status", "") for r in sub],
                "injury_type": [r.get("injury", "") for r in sub],
                "injury_type_desc": [r.get("injury", "") for r in sub],
                "source": [r.get("source", "") for r in sub],
            },
            columns=["date", "sport", "team", "player", "injury_status", "injury_type", "injury_type_desc", "source"],
        ).drop_duplicates(["team", "player"], keep="first")
        tag = sport.lower()
        for dest in (
            root / "outputs" / date_str / f"injuries_{tag}_{date_str}.csv",
            root / "outputs" / date_str / tag / f"injuries_{tag}_{date_str}.csv",
        ):
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(".csv.tmp")
            df.to_csv(tmp, index=False)
            tmp.replace(dest)
        print(f"[injuries] {sport} sidecar -> injuries_{tag}_{date_str}.csv ({len(df)} rows)")
    if missing:
        try:
            from utils.pipeline_alerts import emit_pipeline_alert

            emit_pipeline_alert(
                "injuries-missing",
                f"{date_str}: football injury sidecar empty in season for {', '.join(missing)}.",
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[injuries] alert failed: {exc}")
    return missing


if __name__ == "__main__":
    raise SystemExit(main())
