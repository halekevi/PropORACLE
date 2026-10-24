"""Stored HIT/MISS tennis grades are not replaced by a later regrade.

The snapshot under ``data/snapshots/`` is the record. A regrade may add a row
that was missing. It may not flip or drop a stored HIT or MISS.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_PATH = ROOT / "data" / "snapshots" / "tennis_graded_hit_miss_20261007.jsonl"

DECIDED = frozenset({"HIT", "MISS"})


class StoredGradeChanged(RuntimeError):
    """A regrade would change a tennis grade that is already HIT or MISS."""


def grade_key(row: dict) -> tuple[str, str, str, str, str]:
    """Identity of one tennis prop. Line and direction stay in the key."""
    prop = "".join(ch for ch in str(row.get("prop") or row.get("prop_type") or "").lower() if ch.isalnum())
    direction = str(row.get("direction") or row.get("over_under") or "").strip().upper()
    line = str(row.get("line") or "").strip()
    player = " ".join(str(row.get("player") or "").casefold().split())
    day = str(row.get("date") or "")[:10]
    return (day, player, prop, direction, line)


def result_of(row: dict) -> str:
    return str(row.get("result") or "").strip().upper()


def load_snapshot(path: Path | None = None) -> dict[tuple[str, str, str, str, str], str]:
    """Map grade key → HIT or MISS. Rows that are not decided are ignored."""
    src = path or SNAPSHOT_PATH
    out: dict[tuple[str, str, str, str, str], str] = {}
    if not src.is_file():
        return out
    for line in src.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            continue
        res = result_of(row)
        if res not in DECIDED:
            continue
        out[grade_key(row)] = res
    return out


def assert_fill_only(
    stored: dict[tuple[str, str, str, str, str], str],
    fresh_rows: list[dict],
    *,
    date: str = "",
    corrections: dict | None = None,
) -> None:
    """Raise if ``fresh_rows`` flips or drops a stored HIT or MISS.

    Rows absent from ``stored`` may appear. A stored row must still be present
    with the same result. ``date`` limits the check to that slate day.

    A logged correction (``utils.grade_corrections``) may change one stored
    result when ``old`` matches the snapshot and ``new`` matches the regrade.
    """
    if corrections is None:
        from utils.grade_corrections import load_corrections

        corrections = load_corrections()
    day = str(date or "")[:10]
    fresh: dict[tuple[str, str, str, str, str], str] = {}
    for row in fresh_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("sport") or "").upper() not in {"", "TENNIS"}:
            continue
        res = result_of(row)
        if res not in DECIDED:
            continue
        keyed = dict(row)
        if day and not keyed.get("date"):
            keyed["date"] = day
        fresh[grade_key(keyed)] = res
    for key, stored_result in stored.items():
        if day and key[0] != day:
            continue
        got = fresh.get(key)
        if got == stored_result:
            continue
        corr = corrections.get(key) if corrections else None
        if (
            isinstance(corr, dict)
            and str(corr.get("old") or "").upper() == stored_result
            and str(corr.get("new") or "").upper() == (got or "")
            and str(corr.get("reason") or "").strip()
        ):
            continue
        raise StoredGradeChanged(
            f"{key[0]} {key[1]} {key[2]} {key[3]} {key[4]}: "
            f"stored {stored_result}, regrade {got or 'MISSING'}"
        )
