#!/usr/bin/env python3
"""Validate step8 boards for a slate date; emit pass/fail alerts per sport.

Checks:
  - file exists (or expected-in-season missing)
  - required columns (Title Case or snake_case aliases)
  - minimum fill rates
  - no constant columns (when n>=20)
  - Game Date matches slate date
  - ML Prob not saturated at 0/1
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.pipeline_alerts import emit_pipeline_alert  # noqa: E402
from utils.step8_contracts import Step8Contract, default_contracts  # noqa: E402


def _outputs_root(slate_date: str) -> Path:
    return _REPO / "outputs" / slate_date


def _find_step8(sport_dir: Path, folder: str, slate_date: str) -> Path | None:
    # Prefer parquet/csv (full schema) over clean xlsx (often drops game_date / snake_case).
    candidates = [
        sport_dir / f"step8_{folder}_direction_clean.parquet",
        sport_dir / f"step8_{folder}_direction_clean.csv",
        sport_dir / f"step8_{folder}_direction.parquet",
        sport_dir / f"step8_{folder}_direction.csv",
        sport_dir / f"step8_{folder}_direction_clean.xlsx",
        sport_dir / f"step8_{folder}_direction_clean_{slate_date}.xlsx",
        sport_dir / f"step8_all_direction_clean.xlsx",  # NBA naming
        sport_dir / f"step8_all_direction_clean_{slate_date}.xlsx",
    ]
    for p in candidates:
        if p.is_file():
            return p
    if sport_dir.is_dir():
        hits = sorted(
            sport_dir.glob("step8_*direction*"),
            key=lambda h: {".parquet": 0, ".csv": 1, ".xlsx": 2}.get(h.suffix.lower(), 9),
        )
        for h in hits:
            if h.suffix.lower() in {".xlsx", ".csv", ".parquet"} and h.is_file():
                return h
    return None


def _pick_col(df: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    cols = {str(c): c for c in df.columns}
    lower = {str(c).strip().lower(): c for c in df.columns}
    for a in aliases:
        if a in cols:
            return cols[a]
        if a.lower() in lower:
            return lower[a.lower()]
    return None


def _fill_rate(series: pd.Series) -> float:
    n = len(series)
    if n <= 0:
        return 0.0
    return float(series.notna().sum()) / float(n)


def _game_dates_ok(
    df: pd.DataFrame, col: str, slate_date: str, *, window_days: int = 0
) -> tuple[bool, str]:
    raw = df[col].astype(str)
    # Clock-only "Game Time" (7:00 PM) is presence-only — skip date match.
    if col.lower().replace(" ", "_") in {"game_time"} and not raw.str.match(
        r"^\d{4}-\d{2}-\d{2}", na=False
    ).any():
        nonempty = int((raw.str.strip().ne("") & raw.str.lower().ne("nan")).sum())
        if nonempty <= 0:
            return False, "Game Time empty"
        return True, f"Game Time present on {nonempty} rows (no calendar date to match)"

    parsed = pd.to_datetime(raw, errors="coerce", utc=True)
    dates = parsed.dt.tz_convert(None).dt.strftime("%Y-%m-%d")
    fallback = raw.str.slice(0, 10)
    dates = dates.where(dates.notna() & (dates.str.len() == 10), fallback)
    valid = dates[dates.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)]
    if valid.empty:
        return False, "no parseable Game Date values"
    allowed = {slate_date}
    if window_days > 0:
        base = pd.Timestamp(slate_date)
        for i in range(0, window_days + 1):
            allowed.add((base + pd.Timedelta(days=i)).strftime("%Y-%m-%d"))
    match_n = int(valid.isin(allowed).sum())
    share = match_n / max(len(valid), 1)
    if share < 0.80:
        top = valid.value_counts().head(3).to_dict()
        win = f"{slate_date}..+{window_days}d" if window_days else slate_date
        return False, f"only {share:.0%} rows on {win} (top={top})"
    return True, f"{share:.0%} rows on slate window"


def _ml_saturated(df: pd.DataFrame, col: str, max_share: float) -> tuple[bool, str]:
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(s) < 20:
        return False, f"n={len(s)} (skip)"
    extreme = ((s <= 0.01) | (s >= 0.99)).sum()
    share = float(extreme) / float(len(s))
    bad = share > max_share
    return bad, f"{share:.0%} at 0/1 (max {max_share:.0%})"


def validate_one(
    contract: Step8Contract,
    slate_date: str,
    *,
    expect_present: bool,
    alert: bool = True,
) -> dict[str, Any]:
    sport_dir = _outputs_root(slate_date) / contract.folder
    path = _find_step8(sport_dir, contract.folder, slate_date)
    result: dict[str, Any] = {
        "sport": contract.sport,
        "folder": contract.folder,
        "path": str(path) if path else None,
        "ok": True,
        "failures": [],
        "notes": [],
    }
    if path is None:
        if expect_present:
            result["ok"] = False
            result["failures"].append("missing step8 board")
        else:
            result["notes"].append("no step8 (optional / off-season)")
        return result

    try:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path, low_memory=False)
        elif path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
        else:
            df = pd.read_excel(path)
    except Exception as exc:
        result["ok"] = False
        result["failures"].append(f"read failed: {exc}")
        return result

    n = len(df)
    result["rows"] = n
    result["cols"] = len(df.columns)
    if n < contract.min_rows:
        result["ok"] = False
        result["failures"].append(f"rows={n} < min_rows={contract.min_rows}")

    for aliases in contract.required_any:
        hit = _pick_col(df, aliases)
        if hit is None:
            result["ok"] = False
            result["failures"].append(f"missing column any-of {aliases}")

    checked_fill: set[str] = set()
    for aliases_key, min_rate in contract.min_fill.items():
        col = _pick_col(df, (aliases_key,))
        if col is None or col in checked_fill:
            continue
        checked_fill.add(col)
        rate = _fill_rate(df[col])
        # Treat sentinel -1 on Days Rest as missing
        if "rest" in col.lower():
            s = pd.to_numeric(df[col], errors="coerce")
            rate = float(((s.notna()) & (s >= 0)).sum()) / max(n, 1)
        if rate + 1e-9 < min_rate:
            result["ok"] = False
            result["failures"].append(f"fill {col}={rate:.0%} < {min_rate:.0%}")

    if n >= 20:
        for alias in contract.not_constant:
            col = _pick_col(df, (alias,))
            if col is None:
                continue
            s = df[col].dropna()
            if "rest" in col.lower():
                s = pd.to_numeric(s, errors="coerce")
                s = s[s >= 0]
            if len(s) < 10:
                continue
            nunq = int(s.nunique(dropna=True))
            if nunq <= 1:
                result["ok"] = False
                result["failures"].append(f"constant column {col}")

    gd_col = _pick_col(df, contract.game_date_cols)
    if gd_col is not None:
        ok_gd, note_gd = _game_dates_ok(
            df, gd_col, slate_date, window_days=int(contract.date_window_days or 0)
        )
        result["notes"].append(f"Game Date: {note_gd}")
        if not ok_gd:
            result["ok"] = False
            result["failures"].append(f"Game Date mismatch: {note_gd}")
    else:
        result["ok"] = False
        result["failures"].append("missing Game Date column")

    ml_col = _pick_col(df, contract.ml_prob_cols)
    if ml_col is not None:
        bad, note_ml = _ml_saturated(df, ml_col, contract.max_ml_saturated_share)
        result["notes"].append(f"ML Prob: {note_ml}")
        if bad:
            result["ok"] = False
            result["failures"].append(f"ML Prob saturated: {note_ml}")

    # Direction OVER pile (bias signal; warn-only unless extreme)
    dir_col = _pick_col(df, ("Direction", "direction", "bet_direction"))
    if dir_col is not None and n >= 30:
        d = df[dir_col].astype(str).str.upper().str.strip()
        over_share = float((d == "OVER").sum()) / float(n)
        result["notes"].append(f"OVER share={over_share:.0%}")
        if over_share >= 0.93:
            # Flag loudly but do not fail the whole sport on bias alone —
            # fill/schema/date/ML-sat still gate pass/fail.
            result["notes"].append(f"OVER bias WARN {over_share:.0%} (>=93%)")
            if alert:
                try:
                    emit_pipeline_alert(
                        "step8-over-bias",
                        f"{contract.sport} {slate_date}: OVER share={over_share:.0%}",
                        notify=False,
                    )
                except Exception:
                    pass

    return result


def _in_season_flags(slate_date: str) -> dict[str, bool]:
    """Heuristic: expect board when folder has any step* OR sport is core in-season."""
    d = date.fromisoformat(slate_date)
    month = d.month
    # Rough calendars — validator still runs; missing optional sports note only.
    return {
        "nba": month in (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "nba1q": month in (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "nba1h": month in (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "nhl": month in (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "soccer": True,
        "tennis": True,
        "mlb": month in (3, 4, 5, 6, 7, 8, 9, 10),
        "wnba": month in (5, 6, 7, 8, 9, 10),
        "nfl": month in (9, 10, 11, 12, 1, 2),
        "cfb": month in (8, 9, 10, 11, 12, 1),
    }


def validate_slate(slate_date: str, *, alert: bool = True) -> dict[str, Any]:
    contracts = default_contracts()
    season = _in_season_flags(slate_date)
    results = []
    for folder, contract in contracts.items():
        # Expect present if in-season OR any early pipeline artifact exists
        sport_dir = _outputs_root(slate_date) / folder
        has_early = bool(list(sport_dir.glob("step[1-7]*"))) if sport_dir.is_dir() else False
        expect = bool(season.get(folder, False) or has_early)
        # Period boards: only require if early artifacts exist (pipelines often optional)
        if folder in ("nba1q", "nba1h") and not has_early:
            expect = False
        r = validate_one(contract, slate_date, expect_present=expect, alert=alert)
        results.append(r)
        status = "PASS" if r["ok"] else "FAIL"
        detail = "; ".join(r["failures"]) if r["failures"] else "; ".join(r.get("notes") or ["ok"])
        print(f"  [{status}] {contract.sport}: {detail}")
        if alert and not r["ok"]:
            emit_pipeline_alert(
                "step8-validate",
                f"{contract.sport} {slate_date}: {detail}",
            )
    summary = {
        "date": slate_date,
        "pass": sum(1 for r in results if r["ok"]),
        "fail": sum(1 for r in results if not r["ok"]),
        "results": results,
    }
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate step8 boards for a slate date")
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD")
    ap.add_argument("--no-alert", action="store_true", help="Skip pipeline alerts")
    ap.add_argument("--json-out", default="", help="Optional path for JSON report")
    args = ap.parse_args()
    slate = str(args.date).strip()[:10]
    print(f"\n=== step8 validator {slate} ===\n")
    summary = validate_slate(slate, alert=not args.no_alert)
    print(f"\n  Summary: {summary['pass']} pass / {summary['fail']} fail\n")
    if args.json_out:
        out = Path(args.json_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"  Wrote {out}")
    return 0 if summary["fail"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
