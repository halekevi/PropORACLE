"""Every sport's step1 must land the PrizePicks face (pick_type) on disk.

Week 1 2026 NFL: odds_type never reached the CSV, step7 defaulted blank to
Standard, and every Goblin / Demon rung graded as a Standard face for six
weeks. The same hole can open in any sport whose step1 drops the column or
whose downstream fills it with a default. Call ``assert_pick_type_persisted``
after the final step1 write.

Fails (SystemExit) when:
  * the output file is missing,
  * no pick_type column (``pick_type`` / ``Pick Type`` / ``odds_type``),
  * the column is entirely blank,
  * >= ``min_rows_for_mixed`` rows and zero Goblin / Demon (the Week-1 hole),
    unless ``allow_standard_only`` or the sport is listed in
    ``STANDARD_ONLY_OK``.

``PROPORACLE_PICK_TYPE_GUARD=warn`` downgrades every failure to a WARN line so a
live window can be rescued without editing code; the default is fail.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd

PICK_COLUMNS = ("pick_type", "Pick Type", "odds_type")
_BLANK = {"", "nan", "none", "null", "<na>", "nat"}

# Sports whose boards can legitimately post with no Goblin / Demon rung.
# Keep empty until a real board proves it; use allow_standard_only per call.
STANDARD_ONLY_OK: frozenset[str] = frozenset()


def norm_pick_type(raw: object) -> str:
    key = str(raw if raw is not None else "").strip().lower()
    if key in _BLANK:
        return ""
    if "gob" in key:
        return "Goblin"
    if "dem" in key:
        return "Demon"
    if "std" in key or "stand" in key:
        return "Standard"
    return "Standard"


def _guard_mode() -> str:
    return str(os.environ.get("PROPORACLE_PICK_TYPE_GUARD") or "fail").strip().lower()


def _fail(msg: str) -> None:
    if _guard_mode() == "warn":
        print(f"WARN {msg}")
        return
    raise SystemExit(msg)


def pick_type_counts(df: pd.DataFrame) -> dict[str, int]:
    col = next((c for c in PICK_COLUMNS if c in df.columns), None)
    counts = {"Standard": 0, "Goblin": 0, "Demon": 0, "blank": 0}
    if col is None:
        return counts
    for v in df[col].tolist():
        k = norm_pick_type(v)
        counts[k or "blank"] += 1
    return counts


def assert_pick_type_persisted(
    out_path: Path | str,
    *,
    sport: str,
    min_rows_for_mixed: int = 100,
    allow_standard_only: bool = False,
    min_goblin_share: float = 0.05,
    quiet_when_empty: bool = True,
) -> dict[str, Any]:
    """Re-read the step1 CSV and refuse to continue when the face was not stored."""
    tag = f"[{str(sport or '').upper()} step1]"
    path = Path(out_path)
    if not path.is_file():
        _fail(f"{tag} FATAL: output missing after write -> {path}")
        return {"ok": False, "reason": "missing_file"}
    try:
        disk = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    except pd.errors.EmptyDataError:
        disk = pd.DataFrame()
    n = len(disk)
    if n == 0:
        if not quiet_when_empty:
            print(f"{tag} pick_type guard: empty board, nothing to verify")
        return {"ok": True, "n": 0, "reason": "empty_board"}
    col = next((c for c in PICK_COLUMNS if c in disk.columns), None)
    if col is None:
        _fail(
            f"{tag} FATAL: pick_type column missing on disk -> {path}. "
            "Downstream will default every row to Standard. Do not continue the pipeline."
        )
        return {"ok": False, "n": n, "reason": "no_column"}
    counts = pick_type_counts(disk)
    if counts["blank"] == n:
        _fail(
            f"{tag} FATAL: pick_type is all blank on disk -> {path}. "
            "Do not continue the pipeline."
        )
        return {"ok": False, "n": n, "reason": "all_blank", "counts": counts}
    shares = {k: (100.0 * v / n) for k, v in counts.items()}
    print(
        f"{tag} pick_type ON DISK (re-read): {counts} "
        f"({shares['Standard']:.1f}%S / {shares['Goblin']:.1f}%G / {shares['Demon']:.1f}%D"
        f"{f' / {shares['blank']:.1f}% blank' if counts['blank'] else ''}) -> {path}"
    )
    std_only_ok = allow_standard_only or str(sport or "").upper() in STANDARD_ONLY_OK
    mixed_expected = n >= int(min_rows_for_mixed) and not std_only_ok
    if mixed_expected and counts["Goblin"] == 0 and counts["Demon"] == 0:
        _fail(
            f"{tag} FATAL: {n} rows on disk but pick_type is 100% Standard (no Goblin/Demon). "
            "This is the Week-1 hole: every rung would grade as a Standard face. "
            "Re-fetch with odds_type wired, or pass allow_standard_only for a known Standard-only board."
        )
        return {"ok": False, "n": n, "reason": "standard_only", "counts": counts}
    goblin_share = counts["Goblin"] / n
    if mixed_expected and goblin_share < float(min_goblin_share):
        print(
            f"{tag} WARN: Goblin share {100.0 * goblin_share:.1f}% (n={counts['Goblin']}) is below "
            f"the {100.0 * min_goblin_share:.0f}% floor. Inspect odds_type before step2."
        )
    if counts["blank"]:
        print(f"{tag} WARN: {counts['blank']} rows have blank pick_type; downstream defaults them to Standard.")
    return {"ok": True, "n": n, "counts": counts, "column": col}
