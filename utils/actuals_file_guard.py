"""Write actuals without throwing away rows already on disk.

A dated backup is taken before any swap. An existing file keeps every row.
New rows are appended when their player, team, and prop are not already
present. An empty fetch does not replace a file that already has rows.
"""
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

_KEYS = ("player", "team", "prop_type")


def dated_backup(path: Path) -> Path | None:
    """Copy ``path`` to a sibling ``.bak_YYYYMMDDTHHMMSS`` file."""
    if not path.is_file():
        return None
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    dest = path.with_name(f"{path.name}.bak_{stamp}")
    shutil.copy2(path, dest)
    return dest


def _row_key(row: dict) -> tuple[str, str, str]:
    return tuple(str(row.get(k) or "").strip().lower() for k in _KEYS)  # type: ignore[return-value]


def write_actuals_csv(path: Path, fresh: pd.DataFrame, *, force: bool = False) -> str:
    """Write ``fresh``. Returns 'created', 'replaced', 'patched', or 'kept'.

    ``force`` replaces the file after a backup. Otherwise existing data rows
    stay, and only unseen player/team/prop keys are appended.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fresh = fresh.copy()
    for col in _KEYS:
        if col not in fresh.columns:
            fresh[col] = ""
    if "actual" not in fresh.columns:
        fresh["actual"] = pd.NA

    if not path.is_file():
        fresh.to_csv(path, index=False)
        return "created"

    try:
        old = pd.read_csv(path, dtype=str)
    except Exception:
        old = pd.DataFrame()
    old_n = 0 if old is None or old.empty else len(old)
    if force or old_n == 0:
        bak = dated_backup(path)
        fresh.to_csv(path, index=False)
        print(f"[actuals] backup {bak}" if bak else "[actuals] no prior rows")
        return "replaced"

    have = {_row_key(rec) for rec in old.to_dict(orient="records")}
    incoming = fresh.to_dict(orient="records")
    add = [rec for rec in incoming if _row_key(rec) not in have]
    if not add:
        print(f"[actuals] kept {path} ({old_n} rows, no new keys)")
        return "kept"
    bak = dated_backup(path)
    # Preserve the on-disk column order, then any new columns.
    cols = list(old.columns)
    for col in fresh.columns:
        if col not in cols:
            cols.append(col)
    merged = pd.concat([old, pd.DataFrame(add)], ignore_index=True)
    for col in cols:
        if col not in merged.columns:
            merged[col] = ""
    merged[cols].to_csv(path, index=False)
    print(f"[actuals] backup {bak}; patched +{len(add)} rows -> {path} ({old_n + len(add)})")
    return "patched"
