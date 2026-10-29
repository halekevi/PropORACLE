#!/usr/bin/env python3
"""Monday-morning check: did 11/1 CBB (ND–Villanova) actually grade?

Looks for graded_cbb_YYYY-MM-DD.xlsx, ND–Villanova rows, attached actuals,
and HIT/MISS/PUSH/VOID results (blank result = fail).

  py -3.14 scripts/check_cbb_opener_graded.py
  py -3.14 scripts/check_cbb_opener_graded.py --date 2026-11-01
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.cbb_season import CBB_GRADE_RESUME

OPENER_DATE = CBB_GRADE_RESUME
ALLOWED_RESULTS = frozenset({"HIT", "MISS", "PUSH", "VOID"})
_NOVA_RE = re.compile(r"villanova|\bnova\b", re.I)
_ND_RE = re.compile(r"notre\s*dame|\bnotredame\b|\bnd\b", re.I)


def _norm_cell(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def row_is_nd_villanova(row: pd.Series) -> bool:
    blob = " ".join(_norm_cell(v) for v in row.tolist())
    return bool(_NOVA_RE.search(blob) and _ND_RE.search(blob))


def find_graded_cbb(date_str: str, root: Path) -> Path | None:
    day = str(date_str or "").strip()[:10]
    date_dir = root / "outputs" / day
    candidates = [
        date_dir / f"graded_cbb_{day}.xlsx",
        date_dir / f"cbb_graded_{day}.xlsx",
        date_dir / "cbb" / f"graded_cbb_{day}.xlsx",
        date_dir / f"graded_cbb_{day}.csv",
        date_dir / f"cbb_graded_{day}.csv",
    ]
    for p in candidates:
        if p.is_file():
            return p
    return None


def load_graded_box(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        xls = pd.ExcelFile(path, engine="openpyxl")
        names = [s for s in xls.sheet_names if str(s) != "_META"]
        sheet = "Box Raw" if "Box Raw" in names else ("ALL" if "ALL" in names else names[0])
        return pd.read_excel(path, sheet_name=sheet)
    return pd.read_csv(path)


def _col(df: pd.DataFrame, *names: str) -> str | None:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def check_opener_graded(
    date_str: str = OPENER_DATE,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    root = root or _REPO
    day = str(date_str or OPENER_DATE).strip()[:10]
    path = find_graded_cbb(day, root)
    out: dict[str, Any] = {
        "ok": False,
        "date": day,
        "path": str(path) if path else "",
        "n_rows": 0,
        "n_opener": 0,
        "n_with_actuals": 0,
        "n_blank_result": 0,
        "result_counts": {},
        "errors": [],
    }
    if path is None:
        out["errors"].append(f"missing graded_cbb_{day}.xlsx under outputs/{day}/")
        return out

    df = load_graded_box(path)
    out["n_rows"] = int(len(df))
    if df.empty:
        out["errors"].append("graded file is empty")
        return out

    result_col = _col(df, "result")
    actual_col = _col(df, "actual_value", "actual", "actuals")
    status_col = _col(df, "actual_status")
    if result_col is None:
        out["errors"].append("no result column")
        return out

    mask = df.apply(row_is_nd_villanova, axis=1)
    opener = df.loc[mask].copy()
    out["n_opener"] = int(len(opener))
    if opener.empty:
        out["errors"].append("no ND–Villanova rows (team/opp/matchup)")
        return out

    results = opener[result_col].map(lambda v: str(v or "").strip().upper())
    blank = results.eq("") | results.isin(["NAN", "NONE", "--"])
    out["n_blank_result"] = int(blank.sum())
    if out["n_blank_result"]:
        out["errors"].append(f"{out['n_blank_result']} opener rows have blank result")

    unknown = results[~blank & ~results.isin(ALLOWED_RESULTS)]
    if len(unknown):
        out["errors"].append(f"unexpected result values: {sorted(set(unknown.tolist()))}")

    counts = {k: int((results == k).sum()) for k in ("HIT", "MISS", "PUSH", "VOID")}
    out["result_counts"] = counts

    if actual_col is not None:
        actuals = pd.to_numeric(opener[actual_col], errors="coerce")
        out["n_with_actuals"] = int(actuals.notna().sum())
    elif status_col is not None:
        st = opener[status_col].astype(str).str.upper()
        out["n_with_actuals"] = int(st.eq("OK").sum())
    if out["n_with_actuals"] <= 0:
        out["errors"].append("opener rows have no attached actuals")

    # PUSH/VOID handled = those labels are valid results, not crashes/blanks.
    # They may be zero on a clean slate; the machinery is confirmed by ALLOWED_RESULTS.
    out["ok"] = not out["errors"]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Confirm 11/1 CBB opener graded (ND–Villanova).")
    ap.add_argument("--date", default=OPENER_DATE)
    ap.add_argument("--root", default="", help="Repo root (default: this checkout).")
    args = ap.parse_args()
    root = Path(args.root).resolve() if str(args.root or "").strip() else _REPO
    report = check_opener_graded(args.date, root=root)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if report.get("ok"):
        print(
            f"[cbb-opener] OK {report['n_opener']} ND–Villanova rows, "
            f"actuals={report['n_with_actuals']} "
            f"results={report['result_counts']}"
        )
        return 0
    print("[cbb-opener] FAIL: " + "; ".join(report.get("errors") or ["unknown"]))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
