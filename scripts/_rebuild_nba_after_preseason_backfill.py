"""Rebuild tonight's NBA card L5 / attempts / step8 minutes after preseason backfill."""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "Sports" / "NBA" / "scripts"))

from utils.nba_minutes_role import clear_nba_box_frame_cache, load_nba_box_frame, nba_box_max_date

SLATE = "2026-10-22"
OUT = ROOT / "data" / "reports" / f"nba_preseason_rebuild_{SLATE}.txt"


def main() -> int:
    clear_nba_box_frame_cache()
    boxes = load_nba_box_frame()
    mx = nba_box_max_date(boxes)
    print(f"box max={mx} rows={len(boxes)} pre={int((boxes.window=='preseason').sum())}")

    before_path = ROOT / "data" / "reports" / f"nba_card_window_l5_{SLATE}.json"
    before = {}
    if before_path.exists():
        before = json.loads(before_path.read_text(encoding="utf-8"))

    # Card window L5 (goblin + standard)
    subprocess.check_call(
        [
            sys.executable,
            str(ROOT / "scripts" / "report_nba_card_window_l5.py"),
            "--date",
            SLATE,
            "--include-standard",
        ],
        cwd=str(ROOT),
    )
    # Attempts scan
    subprocess.check_call(
        [
            sys.executable,
            str(ROOT / "scripts" / "_scan_nba_attempt_props_l5eq5.py"),
            "--slate",
            SLATE,
        ],
        cwd=str(ROOT),
    )

    # Rebuild step8 clean xlsx minutes/role columns from parquet or existing xlsx
    from step8_add_direction_context import build_clean_xlsx

    clear_nba_box_frame_cache()
    src_parquet = ROOT / "outputs" / SLATE / "nba" / "step8_all_direction_clean.parquet"
    src_xlsx = ROOT / "outputs" / SLATE / "nba" / "step8_all_direction_clean.xlsx"
    dated = ROOT / "outputs" / SLATE / f"step8_nba_direction_clean_{SLATE}.xlsx"
    if src_parquet.exists():
        df = pd.read_parquet(src_parquet)
    else:
        # load from dated step8 with header detect
        path = dated if dated.exists() else src_xlsx
        xl = pd.ExcelFile(path)
        sheet = "ALL" if "ALL" in xl.sheet_names else xl.sheet_names[-1]
        raw = pd.read_excel(xl, sheet_name=sheet, header=None)
        header_i = 0
        for i in range(min(8, len(raw))):
            cells = [str(x).strip().lower() for x in raw.iloc[i].tolist()[:25]]
            if "player" in cells or "prop_type" in cells:
                header_i = i
                break
        df = pd.read_excel(xl, sheet_name=sheet, header=header_i)
    for a, b in {
        "Player": "player",
        "Prop": "prop_type",
        "Pick Type": "pick_type",
        "Line": "line",
        "Team": "team",
    }.items():
        if a in df.columns and b not in df.columns:
            df[b] = df[a]

    build_clean_xlsx(
        df,
        str(src_xlsx),
        source_hint=str(src_parquet or dated),
        as_of=date(2026, 10, 22),
    )
    if src_xlsx.exists() and dated != src_xlsx:
        shutil.copy2(src_xlsx, dated)

    after = json.loads(
        (ROOT / "data" / "reports" / f"nba_card_window_l5_{SLATE}.json").read_text(
            encoding="utf-8"
        )
    )

    lines = [
        f"NBA rebuild after preseason backfill  slate={SLATE}",
        f"box_max={mx}  preseason_rows={(boxes.window=='preseason').sum()}",
        "",
        "===== CARD L5=5 BUCKET COUNTS =====",
        f"before: {before.get('pre_flags')}  n_clears={before.get('n_clears')}",
        f"after:  {after.get('pre_flags')}  n_clears={after.get('n_clears')}",
        "",
        "See also:",
        f"  {ROOT / 'data' / 'reports' / 'nba_preseason_backfill_2026.txt'}",
        f"  {ROOT / 'data' / 'reports' / f'nba_card_window_l5_{SLATE}.txt'}",
        f"  {ROOT / 'data' / 'reports' / f'nba_attempt_l5eq5_{SLATE}.txt'}",
        f"  {ROOT / 'data' / 'reports' / f'nba_card_l5_stale_vs_live_{SLATE}.txt'}",
    ]
    text = "\n".join(lines) + "\n"
    OUT.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
