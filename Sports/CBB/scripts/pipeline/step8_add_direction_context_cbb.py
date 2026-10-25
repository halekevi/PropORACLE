#!/usr/bin/env python3
"""CBB / WCBB step8 — pass-through board after step7b.

step7b does not score CBB (no trusted model). This writes the ranked ALL
sheet to step8_*_direction_clean.xlsx so opening night still has a board
file when step7b exits OK.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve()
for _ in range(8):
    if (_REPO / "utils" / "step8_edge_direction.py").is_file():
        break
    _REPO = _REPO.parent
else:
    raise RuntimeError("Could not locate repo root")
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from proporacle.data.table_io import (  # noqa: E402
    copy_parquet_sidecar,
    read_table,
    table_exists,
    write_excel_sheets,
    write_parquet_sidecar,
)
from utils.hit_tracking_columns import attach_hit_tracking_columns  # noqa: E402
from utils.step8_edge_direction import reconcile_signed_edge_abs_dataframe  # noqa: E402


def _sport_code(*parts: str) -> str:
    blob = " ".join(str(p or "") for p in parts).lower().replace("\\", "/")
    if "wcbb" in blob:
        return "WCBB"
    return "CBB"


def _copy_dated(out_xlsx: Path, slate_date: str, sport: str) -> None:
    d = (slate_date or "").strip()[:10]
    if not d or not out_xlsx.is_file():
        return
    folder = "wcbb" if sport == "WCBB" else "cbb"
    stem = "step8_wcbb_direction_clean" if sport == "WCBB" else "step8_cbb_direction_clean"
    dated = _REPO / "outputs" / d / folder / f"{stem}_{d}.xlsx"
    try:
        dated.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(out_xlsx, dated)
        copy_parquet_sidecar(out_xlsx, dated)
        print(f"[CBB step8] Dated copy -> {dated}")
    except Exception as exc:
        print(f"[CBB step8] WARN dated copy: {exc}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--date", default="")
    ap.add_argument("--sport", default="")
    ap.add_argument("--sheet", default="ALL")
    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.is_absolute():
        inp = _REPO / inp
    if not table_exists(inp):
        raise SystemExit(f"Missing input: {inp}")

    out_path = Path(args.output)
    if not out_path.is_absolute():
        out_path = _REPO / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sport = str(args.sport or "").strip().upper() or _sport_code(args.input, args.output)
    df = read_table(
        inp,
        sheet=args.sheet,
        sheet_order=(args.sheet, "ALL"),
        prefer_parquet=False,
    )
    if df.empty:
        write_excel_sheets(out_path, {"ALL": pd.DataFrame()})
        write_parquet_sidecar(pd.DataFrame(), out_path)
        print(f"[CBB step8] Wrote empty {out_path}")
        return

    work = attach_hit_tracking_columns(df.copy(), sport)
    work["sport"] = sport
    reconcile_signed_edge_abs_dataframe(work)
    sheets = {"ALL": work}
    if "tier" in work.columns:
        tier = work["tier"].astype(str).str.strip().str.upper()
        for t in ("A", "B", "C", "D"):
            sub = work.loc[tier.eq(t)]
            if len(sub):
                sheets[f"Tier {t}"] = sub
    write_excel_sheets(out_path, sheets)
    write_parquet_sidecar(work, out_path)
    print(f"[CBB step8] Wrote {out_path} rows={len(work)} sport={sport}")
    _copy_dated(out_path, str(args.date or "").strip(), sport)


if __name__ == "__main__":
    main()
