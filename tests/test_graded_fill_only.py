"""A regrade must not change a stored tennis HIT or MISS."""
from __future__ import annotations

import json

import pytest

from utils.graded_fill_only import (
    SNAPSHOT_PATH,
    StoredGradeChanged,
    assert_fill_only,
    load_snapshot,
)


def _row(result: str, **extra: str) -> dict:
    base = {
        "date": "2026-08-28",
        "sport": "TENNIS",
        "player": "Qinwen Zheng",
        "prop": "Total Games Won",
        "direction": "OVER",
        "line": "12.5",
        "result": result,
    }
    base.update(extra)
    return base


def test_regrade_may_add_a_missing_row():
    stored = load_snapshot_from([_row("HIT")])
    fresh = [_row("HIT"), _row("MISS", player="Iga Swiatek", line="10.5")]
    assert_fill_only(stored, fresh, date="2026-08-28")


def test_regrade_that_flips_hit_to_miss_fails():
    stored = load_snapshot_from([_row("HIT")])
    with pytest.raises(StoredGradeChanged):
        assert_fill_only(stored, [_row("MISS")], date="2026-08-28")


def test_logged_correction_allows_a_flip(tmp_path):
    from utils.grade_corrections import log_correction, load_corrections

    stored = load_snapshot_from([_row("HIT")])
    path = tmp_path / "grade_corrections.jsonl"
    log_correction(
        _row("MISS"),
        old="HIT",
        new="MISS",
        reason="wrong_match_attach",
        path=path,
    )
    corrections = load_corrections(path)
    assert_fill_only(stored, [_row("MISS")], date="2026-08-28", corrections=corrections)


def test_regrade_that_drops_a_decided_row_fails():
    stored = load_snapshot_from([_row("HIT")])
    with pytest.raises(StoredGradeChanged):
        assert_fill_only(stored, [], date="2026-08-28")


def test_snapshot_file_holds_the_decided_tennis_rows():
    stored = load_snapshot(SNAPSHOT_PATH)
    # 15,929 stored rows, 82 repeated keys with the same result.
    assert len(stored) == 15847
    assert set(stored.values()) <= {"HIT", "MISS"}


def load_snapshot_from(rows: list[dict]) -> dict:
    blob = "\n".join(json.dumps(r) for r in rows)
    # load_snapshot reads a file; build the map the same way the writer does.
    from utils.graded_fill_only import grade_key, result_of

    return {grade_key(r): result_of(r) for r in rows if result_of(r) in {"HIT", "MISS"}}
