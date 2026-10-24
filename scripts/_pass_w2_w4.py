"""W2 cell loss, W3 period L5 ratios, W4 unlock counts, W5 backfill."""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from build_goblin70_tickets import load_today_board  # noqa: E402
from capture_entries import backfill_settlement_store  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    goblin_70_eligible,
    standard_ticket_eligible,
)

MAIN = ROOT.parent / "PropORACLE_main_cp"
UNRESOLVED = ROOT / "data" / "reports" / "soccer_unresolved_multi_event.csv"
CELL_OUT = ROOT / "data" / "reports" / "soccer_notinbox_cell_loss.json"
RATIO_OUT = ROOT / "data" / "reports" / "nba_period_l5_ratio.json"
UNLOCK_OUT = ROOT / "data" / "reports" / "unlock_current_season.json"
DATE = "2026-10-08"

SOCCER_CELL = {
    "shots": "shots",
    "sog": "sog",
    "saves": "saves",
}


def _num(value: object) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def _fold(name: object) -> str:
    return "".join(ch for ch in str(name or "").lower() if ch.isalnum())


def graded_files() -> dict[str, Path]:
    by: dict[str, Path] = {}
    for folder in (
        MAIN / "ui_runner" / "templates",
        ROOT / "ui_runner" / "templates",
    ):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            date = path.stem.replace("graded_props_", "")[:10]
            by.setdefault(date, path)
    return by


def soccer_cell_loss() -> dict:
    keys: set[tuple[str, str, str]] = set()
    with UNRESOLVED.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("bucket") != "player_not_in_box":
                continue
            prop = row.get("prop") or ""
            if prop not in SOCCER_CELL:
                continue
            keys.add((row.get("day") or "", _fold(row.get("player_raw")), prop))
    decided = {p: 0 for p in SOCCER_CELL}
    lost = {p: 0 for p in SOCCER_CELL}
    for date, path in graded_files().items():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if T.norm_sport(str(row.get("sport") or "")) != "SOCCER":
                continue
            prop = T.canon_prop("SOCCER", str(row.get("prop") or ""))
            if prop not in SOCCER_CELL:
                continue
            if str(row.get("result") or "").upper() not in {"HIT", "MISS"}:
                continue
            decided[prop] += 1
            game_date = str(row.get("game_date") or row.get("date") or date)[:10]
            if (game_date, _fold(row.get("player")), prop) in keys:
                lost[prop] += 1
    report = {
        "not_in_box_keys": len(keys),
        "decided": decided,
        "lost_from_cell": lost,
        "remaining": {p: decided[p] - lost[p] for p in SOCCER_CELL},
    }
    CELL_OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("CELL", json.dumps(report))
    return report


def _named_col(columns: list[str], names: tuple[str, ...]) -> str | None:
    want = {name.lower() for name in names}
    for col in columns:
        if str(col).strip().lower() in want:
            return col
    return None


def _player_col(columns: list[str]) -> str | None:
    for col in columns:
        if str(col).strip().lower() in {"player", "player name", "name"}:
            return col
    return None


def _prop_col(columns: list[str]) -> str | None:
    for col in columns:
        if str(col).strip().lower() in {"prop", "prop type", "stat", "stat type"}:
            return col
    return None


def _load_step(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def _ratio_for(period: str, full: str) -> dict:
    ratios: list[float] = []
    l10_ratios: list[float] = []
    equal = 0
    dates = 0
    out_root = MAIN / "outputs"
    if not out_root.is_dir():
        return {"n": 0}
    for day_dir in sorted(out_root.iterdir()):
        if not day_dir.is_dir():
            continue
        period_paths = list(day_dir.glob(f"step8_{period}_direction_clean*.csv"))
        period_paths += list(day_dir.glob(f"step8_{period}_direction_clean*.xlsx"))
        period_paths += list((day_dir / period).glob("step8_*direction_clean*.csv"))
        period_paths += list((day_dir / period).glob("step8_*direction_clean*.xlsx"))
        full_paths = list(day_dir.glob("step8_nba_direction_clean*.csv"))
        full_paths += list(day_dir.glob("step8_nba_direction_clean*.xlsx"))
        full_paths += list((day_dir / "nba").glob("step8_*direction_clean*.csv"))
        full_paths += list((day_dir / "nba").glob("step8_*direction_clean*.xlsx"))
        if not period_paths or not full_paths:
            continue
        try:
            left = _load_step(period_paths[0])
            right = _load_step(full_paths[0])
        except Exception:
            continue
        pc, fc = list(left.columns), list(right.columns)
        p_player, f_player = _player_col(pc), _player_col(fc)
        p_prop, f_prop = _prop_col(pc), _prop_col(fc)
        p_l5 = _named_col(pc, ("last 5 avg",))
        f_l5 = _named_col(fc, ("last 5 avg",))
        p_l10 = _named_col(pc, ("last 10 avg",))
        f_l10 = _named_col(fc, ("last 10 avg",))
        if not all([p_player, f_player, p_prop, f_prop, p_l5, f_l5]):
            continue
        dates += 1
        full_idx: dict[tuple[str, str], tuple[float | None, float | None]] = {}
        for _, row in right.iterrows():
            key = (_fold(row[f_player]), _fold(row[f_prop]))
            full_idx[key] = (_num(row[f_l5]), _num(row[f_l10]) if f_l10 else None)
        for _, row in left.iterrows():
            key = (_fold(row[p_player]), _fold(row[p_prop]))
            if key not in full_idx:
                continue
            pl5 = _num(row[p_l5])
            fl5 = full_idx[key][0]
            if pl5 is None or fl5 is None or fl5 <= 0:
                continue
            ratios.append(pl5 / fl5)
            if abs(pl5 - fl5) < 1e-9:
                equal += 1
            if p_l10 and full_idx[key][1] not in (None, 0):
                pl10 = _num(row[p_l10])
                fl10 = full_idx[key][1]
                if pl10 is not None and fl10:
                    l10_ratios.append(pl10 / fl10)
    ratios.sort()
    l10_ratios.sort()

    def _median(vals: list[float]) -> float | None:
        if not vals:
            return None
        mid = len(vals) // 2
        if len(vals) % 2:
            return vals[mid]
        return (vals[mid - 1] + vals[mid]) / 2

    return {
        "dates": dates,
        "n": len(ratios),
        "median_l5_ratio": _median(ratios),
        "median_l10_ratio": _median(l10_ratios),
        "l10_n": len(l10_ratios),
        "equal_l5": equal,
    }


def period_ratios() -> dict:
    nba1q = _ratio_for("nba1q", "nba")
    nba1h = _ratio_for("nba1h", "nba")
    # Last 5 Avg / Last 10 Avg, not the 0-5 hit count.
    # 1Q sane band 0.25-0.40. 1H sane band 0.45-0.60.
    # n under 30 is reported and does not change the gate.
    block_q = (
        nba1q["n"] >= 30
        and nba1q["median_l5_ratio"] is not None
        and not (0.25 <= nba1q["median_l5_ratio"] <= 0.40)
    )
    block_h = (
        nba1h["n"] >= 30
        and nba1h["median_l5_ratio"] is not None
        and not (0.45 <= nba1h["median_l5_ratio"] <= 0.60)
    )
    report = {
        "nba1q": nba1q,
        "nba1h": nba1h,
        "block": bool(block_q or block_h),
        "sane": not (block_q or block_h) and nba1q["n"] >= 30,
    }
    RATIO_OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("RATIO", json.dumps(report))
    return report


def _sample_n(row: dict) -> float | None:
    for key in ("l5_sample_n", "L5 Sample N", "games_used"):
        val = _num(row.get(key))
        if val is not None:
            return val
    return None


def unlock() -> dict:
    rows = load_today_board(DATE)
    by: dict[str, dict] = defaultdict(
        lambda: {
            "rows": 0,
            "sample_known": 0,
            "current_n_ge5": 0,
            "goblin": 0,
            "standard": 0,
        }
    )
    for row in rows:
        sport = T.norm_sport(str(row.get("sport") or "")) or "UNKNOWN"
        bucket = by[sport]
        bucket["rows"] += 1
        sample = _sample_n(row)
        if sample is not None:
            bucket["sample_known"] += 1
            if sample >= 5:
                bucket["current_n_ge5"] += 1
        if goblin_70_eligible(row):
            bucket["goblin"] += 1
        if standard_ticket_eligible(row):
            bucket["standard"] += 1
    report = {"date": DATE, "sports": {k: dict(v) for k, v in sorted(by.items())}}
    UNLOCK_OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("UNLOCK", json.dumps(report))
    return report


def main() -> None:
    soccer_cell_loss()
    period_ratios()
    unlock()
    filled = backfill_settlement_store()
    print("BACKFILL", json.dumps(filled))
    (ROOT / "data" / "reports" / "settlement_backfill.json").write_text(
        json.dumps(filled, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
