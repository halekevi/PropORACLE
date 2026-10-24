"""Versioned overrides for a stored tennis HIT/MISS.

The fill-only guard keeps a regrade from silently flipping grades. A
correction is the explicit path: old value, new value, and a reason code,
appended to ``data/reports/grade_corrections.jsonl``. ``assert_fill_only``
allows the flip only when the log matches.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.graded_fill_only import grade_key

ROOT = Path(__file__).resolve().parents[1]
CORRECTIONS_PATH = ROOT / "data" / "reports" / "grade_corrections.jsonl"
ET = ZoneInfo("America/New_York")

# Reason codes
WRONG_MATCH_ATTACH = "wrong_match_attach"


def load_corrections(path: Path | None = None) -> dict[tuple, dict]:
    """Latest correction per grade key. Later lines bump ``version``."""
    src = path or CORRECTIONS_PATH
    out: dict[tuple, dict] = {}
    if not src.is_file():
        return out
    for line in src.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            continue
        key = grade_key(row)
        prev = out.get(key)
        version = int(row.get("version") or 0)
        if prev is None:
            version = version or 1
        else:
            version = version or int(prev.get("version") or 0) + 1
        row = dict(row)
        row["version"] = version
        out[key] = row
    return out


def log_correction(
    row: dict,
    *,
    old: str,
    new: str,
    reason: str,
    path: Path | None = None,
    note: str = "",
) -> dict:
    """Append one override. Returns the written record."""
    dest = path or CORRECTIONS_PATH
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing = load_corrections(dest)
    key = grade_key(row)
    prev = existing.get(key)
    version = 1 if prev is None else int(prev.get("version") or 1) + 1
    rec = {
        "date": str(row.get("date") or "")[:10],
        "player": row.get("player"),
        "prop": row.get("prop") or row.get("prop_type"),
        "direction": row.get("direction") or row.get("over_under") or row.get("side"),
        "line": row.get("line"),
        "sport": row.get("sport") or "TENNIS",
        "old": str(old).upper(),
        "new": str(new).upper(),
        "reason": reason,
        "version": version,
        "note": note,
        "logged_at": datetime.now(ET).isoformat(timespec="seconds"),
    }
    with dest.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec
