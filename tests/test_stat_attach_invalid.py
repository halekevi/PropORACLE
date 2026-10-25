"""Sibling-mapped NFL props are invalid for analysis (2026-10-25)."""
from __future__ import annotations

from utils.stat_attach_invalid import (
    analysis_invalid_reason,
    is_analysis_invalid_prop,
    tag_rows,
)


def test_ypc_first_n_longest_completion_tagged_nfl_only():
    assert is_analysis_invalid_prop("NFL", "ypc")
    assert is_analysis_invalid_prop("NFL", "yards_per_carry")
    assert "YPC" in analysis_invalid_reason("NFL", "YPC")
    assert is_analysis_invalid_prop("NFL", "yards_on_first_reception")
    assert is_analysis_invalid_prop("NFL", "rush_yards_in_first_5_attempts")
    assert is_analysis_invalid_prop("NFL", "quarters_with_25_pass_yards")
    assert is_analysis_invalid_prop("NFL", "longest_reception")
    assert is_analysis_invalid_prop("NFL", "completion_percentage")
    assert is_analysis_invalid_prop("NFL", "pass_completions")
    assert is_analysis_invalid_prop("NFL", "longest_completion")
    assert is_analysis_invalid_prop("CFB", "longest_reception")
    # Real volume / the shadow props stay measurable
    assert not is_analysis_invalid_prop("NFL", "rushing_yards")
    assert not is_analysis_invalid_prop("NFL", "receiving_yards")
    assert not is_analysis_invalid_prop("NFL", "tackles")
    assert not is_analysis_invalid_prop("NFL", "sacks")  # held on prereg, not this tag
    assert not is_analysis_invalid_prop("NBA", "longest_reception")


def test_tag_rows_drops_and_counts():
    rows = [
        {"sport": "NFL", "prop": "tackles"},
        {"sport": "NFL", "prop": "sacks"},
        {"sport": "NFL", "prop": "longest_reception"},
        {"sport": "NFL", "prop": "YPC"},
    ]
    kept, meta = tag_rows(rows)
    assert [r["prop"] for r in kept] == ["tackles", "sacks"]
    assert meta["dropped"] == 2
    assert meta["tagged"] == "2026-10-25"
