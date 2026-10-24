"""Merge grade_history copies instead of picking the newest-last-date file."""
from __future__ import annotations

import json
from pathlib import Path

from utils.proporacle_data_root import (
    load_best_grade_history_runs,
    merge_grade_history_runs,
)


def test_merge_keeps_old_and_new_dates():
    old = [{"date": "2026-04-09", "track": "graded_main", "n_tickets": 6, "roi_pct": -10}]
    new = [{"date": "2026-09-06", "track": "graded_main", "n_tickets": 27, "roi_pct": 200}]
    out = merge_grade_history_runs(old, new)
    dates = [r["date"] for r in out]
    assert dates == ["2026-04-09", "2026-09-06"]


def test_merge_prefers_larger_n_on_clash():
    a = [{"date": "2026-08-27", "track": "graded_main", "n_tickets": 10, "roi_pct": 1}]
    b = [{"date": "2026-08-27", "track": "graded_main", "n_tickets": 104, "roi_pct": 11}]
    out = merge_grade_history_runs(a, b)
    assert len(out) == 1
    assert out[0]["n_tickets"] == 104
    assert out[0]["roi_pct"] == 11


def test_load_best_merges_repo_copies(tmp_path: Path):
    repo = tmp_path / "PropORACLE"
    data = repo / "data"
    tmpl = repo / "ui_runner" / "templates"
    data.mkdir(parents=True)
    tmpl.mkdir(parents=True)
    (data / "grade_history.json").write_text(
        json.dumps(
            [
                {
                    "date": "2026-04-09",
                    "track": "graded_main",
                    "n_tickets": 6,
                    "roi_pct": -10,
                }
            ]
        ),
        encoding="utf-8",
    )
    (tmpl / "grade_history.json").write_text(
        json.dumps(
            [
                {
                    "date": "2026-09-06",
                    "track": "graded_main",
                    "n_tickets": 27,
                    "roi_pct": 200,
                }
            ]
        ),
        encoding="utf-8",
    )
    runs = load_best_grade_history_runs(repo, templates_dir=tmpl)
    assert [r["date"] for r in runs] == ["2026-04-09", "2026-09-06"]
