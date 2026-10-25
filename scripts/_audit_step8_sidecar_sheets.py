#!/usr/bin/env python3
"""Compare each sport's latest step8 Excel ALL sheet vs its parquet sidecar.

An unkeyed sidecar that is really ELIGIBLE (or any other sheet) shrinks the
board to Standard-only with no error. Report row count and pick_type mix.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from proporacle.data.table_io import (  # noqa: E402
    _read_parquet,
    excel_data_header_row,
    iter_parquet_sidecars,
    parquet_sidecar_path,
)

SPORTS = (
    "nba",
    "nba1h",
    "nba1q",
    "wnba",
    "wnba1h",
    "wnba1q",
    "mlb",
    "nhl",
    "nfl",
    "cfb",
    "cfb1h",
    "cbb",
    "wcbb",
    "soccer",
    "tennis",
    "golf",
)


def _pick_type_mix(df: pd.DataFrame) -> dict[str, int]:
    for col in ("pick_type", "Pick Type", "odds_type"):
        if col in df.columns:
            s = df[col].astype(str).str.strip()
            s = s.replace({"nan": "", "None": "", "<NA>": ""})
            return s.value_counts().to_dict()
    return {}


def _excel_all(path: Path) -> pd.DataFrame | None:
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
    except Exception as exc:
        print(f"  EXCEL_OPEN_FAIL {path}: {exc}")
        return None
    names = [n for n in xl.sheet_names if str(n) != "_META"]
    chosen = "ALL" if "ALL" in names else (names[0] if names else None)
    if chosen is None:
        return None
    header = excel_data_header_row(path, chosen)
    return pd.read_excel(path, sheet_name=chosen, engine="openpyxl", header=header)


def _latest_step8(root: Path, sport: str) -> Path | None:
    dated = sorted(
        (root / "outputs").glob(f"*/{sport}/step8_*direction_clean*.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    dated = [p for p in dated if "_FILTERED" not in p.name]
    undated = [p for p in dated if not any(ch.isdigit() for ch in p.stem[-10:])]
    if undated:
        return undated[0]
    if dated:
        return dated[0]
    leftovers = sorted(
        (root / "Sports").glob(f"**/*step8*{sport}*direction_clean*.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    leftovers = [p for p in leftovers if p.is_file() and "archive" not in str(p).lower()]
    return leftovers[0] if leftovers else None


def audit_one(path: Path) -> dict:
    excel = _excel_all(path)
    if excel is None:
        return {"path": str(path), "status": "NO_EXCEL_ALL"}
    excel_n = len(excel)
    excel_mix = _pick_type_mix(excel)
    keyed = parquet_sidecar_path(path, sheet="ALL")
    unkeyed = parquet_sidecar_path(path)
    sidecar = keyed if keyed.is_file() else (unkeyed if unkeyed.is_file() else None)
    if sidecar is None:
        extra = [p.name for p in iter_parquet_sidecars(path)]
        return {
            "path": str(path),
            "status": "NO_SIDECAR",
            "excel_n": excel_n,
            "excel_mix": excel_mix,
            "other_sidecars": extra,
        }
    hop = _read_parquet(sidecar)
    hop_n = len(hop)
    hop_mix = _pick_type_mix(hop)
    n_mismatch = hop_n != excel_n
    mix_mismatch = bool(excel_mix) and bool(hop_mix) and hop_mix != excel_mix
    faces = {"Goblin", "Demon", "goblin", "demon"}
    excel_faces = sum(excel_mix.get(k, 0) for k in faces)
    hop_faces = sum(hop_mix.get(k, 0) for k in faces)
    face_drop = excel_faces > 0 and hop_faces == 0
    mismatch = n_mismatch or mix_mismatch or face_drop
    keyed_ok = sidecar == keyed
    return {
        "path": str(path),
        "status": "MISMATCH" if mismatch else "OK",
        "sidecar": sidecar.name,
        "keyed": keyed_ok,
        "excel_n": excel_n,
        "sidecar_n": hop_n,
        "excel_mix": excel_mix,
        "sidecar_mix": hop_mix,
    }


def main() -> int:
    root = _REPO
    print("step8 sidecar vs Excel ALL")
    mismatches = 0
    seen = 0
    for sport in SPORTS:
        path = _latest_step8(root, sport)
        if path is None:
            print(f"\n[{sport}] no step8 file")
            continue
        seen += 1
        row = audit_one(path)
        print(f"\n[{sport}] {row['status']}  {path}")
        if row["status"] == "NO_SIDECAR":
            print(f"  excel n={row['excel_n']} mix={row['excel_mix']}")
            continue
        if row["status"] == "NO_EXCEL_ALL":
            continue
        print(
            f"  sidecar={row['sidecar']} keyed_ALL={row['keyed']}  "
            f"excel n={row['excel_n']} sidecar n={row['sidecar_n']}"
        )
        print(f"  excel mix   {row['excel_mix']}")
        print(f"  sidecar mix {row['sidecar_mix']}")
        if row["status"] == "MISMATCH":
            mismatches += 1
    print(f"\n{seen} boards, {mismatches} mismatch(es)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
