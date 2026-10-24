"""Stamp season_l5_flag / l5_sample_n onto archived goblin70 CFB/NFL legs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
REPORTS = ROOT / "data" / "reports"


def _flags_for(day: str, sport: str) -> dict[tuple[str, str], tuple[str, object]]:
    root = MAIN / "outputs" / day
    files = list(root.glob(f"**/*{sport}*step8*.parquet")) + list(
        root.glob(f"**/*step8*{sport}*.parquet")
    )
    out: dict[tuple[str, str], tuple[str, object]] = {}
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        player_c = cols.get("player")
        prop_c = cols.get("prop_type") or cols.get("prop")
        flag_c = cols.get("season l5 flag") or cols.get("season_l5_flag")
        sample_c = cols.get("l5 sample n") or cols.get("l5_sample_n")
        if not (player_c and prop_c and flag_c):
            continue
        for _, row in df.iterrows():
            key = (
                str(row[player_c] or "").strip().lower(),
                str(row[prop_c] or "").strip().lower(),
            )
            flag = str(row[flag_c] or "").strip()
            if flag.lower() in {"", "nan", "none", "null"}:
                flag = ""
            sample = row.get(sample_c) if sample_c else None
            if sample is not None and str(sample).lower() in {"nan", "none", "null"}:
                sample = None
            out[key] = (flag, sample)
    return out


def backfill(path: Path, day: str) -> int:
    raw = json.loads(path.read_text(encoding="utf-8"))
    flags = {
        "cfb": _flags_for(day, "cfb"),
        "nfl": _flags_for(day, "nfl"),
    }
    n = 0

    def stamp(leg: dict) -> None:
        nonlocal n
        sport = str(leg.get("sport") or "").strip().lower()
        if sport not in flags:
            return
        key = (
            str(leg.get("player") or "").strip().lower(),
            str(leg.get("prop") or leg.get("prop_type") or "").strip().lower(),
        )
        hit = flags[sport].get(key)
        if not hit:
            return
        flag, sample = hit
        cur = str(leg.get("season_l5_flag") or "").strip()
        if cur.lower() in {"nan", "none", "null"}:
            cur = ""
        if flag != cur:
            leg["season_l5_flag"] = flag
            n += 1
        if sample is not None and leg.get("l5_sample_n") != sample:
            leg["l5_sample_n"] = sample
            n += 1
        elif sample is None and str(leg.get("l5_sample_n") or "").lower() in {
            "nan",
            "none",
            "null",
        }:
            leg["l5_sample_n"] = None
            n += 1

    for leg in raw.get("goblin_pool") or []:
        if isinstance(leg, dict):
            stamp(leg)
    for ticket in raw.get("tickets") or []:
        if not isinstance(ticket, dict):
            continue
        for leg in ticket.get("legs") or []:
            if isinstance(leg, dict):
                stamp(leg)
    if n:
        path.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    args = ap.parse_args()
    paths = sorted(REPORTS.glob("goblin70_tickets_*.json"))
    for path in paths:
        if path.name.endswith("_latest.json"):
            continue
        day = ""
        for part in path.stem.split("_"):
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                day = part
                break
        if not day:
            continue
        if args.date and day != args.date:
            continue
        n = backfill(path, day)
        print(f"{path.name}: stamped {n} legs")


if __name__ == "__main__":
    main()
