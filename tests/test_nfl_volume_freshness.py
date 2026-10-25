"""NFL volume week assertion and pre-kick snapshots."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from utils.nfl_volume_freshness import (
    NflVolumeCacheStale,
    assert_nfl_volume_week_fresh,
    calendar_completed_week,
    volume_week_report,
)
from utils.nfl_volume_snapshot import (
    append_pre_kick_snapshot,
    apply_pre_kick_volume,
    load_pre_kick_lookup,
)


def _write_caches(root: Path, *, snap_week: int | None, route_week: int) -> None:
    data = root / "Sports" / "NFL" / "data"
    data.mkdir(parents=True, exist_ok=True)
    snap_meta = {"season": 2026, "players": 1}
    if snap_week is not None:
        snap_meta["through_week"] = snap_week
    (data / "nfl_snap_pct_cache.json").write_text(
        json.dumps({"meta": snap_meta, "players": {}}),
        encoding="utf-8",
    )
    (data / "nfl_route_participation_meta.json").write_text(
        json.dumps({"season": 2026, "through_week": route_week}),
        encoding="utf-8",
    )


def test_completed_week_sunday_of_week_n_is_n_minus_1():
    # 2026 week 1 Thursday is Sept 10. Sunday Oct 25 is week 7; week 7
    # completes Tuesday Oct 27.
    assert calendar_completed_week(date(2026, 9, 9)) == 0
    assert calendar_completed_week(date(2026, 9, 14)) == 0
    assert calendar_completed_week(date(2026, 9, 15)) == 1
    assert calendar_completed_week(date(2026, 10, 25)) == 6
    assert calendar_completed_week(date(2026, 10, 27)) == 7


def test_offseason_skips():
    assert calendar_completed_week(date(2026, 4, 15)) is None


def test_stale_week_2_cache_fails(tmp_path: Path):
    _write_caches(tmp_path, snap_week=2, route_week=2)
    with pytest.raises(NflVolumeCacheStale, match="NFL_VOLUME_STALE"):
        assert_nfl_volume_week_fresh(
            as_of=date(2026, 10, 25),
            root=tmp_path,
            label="test",
        )


def test_missing_through_week_counts_as_zero(tmp_path: Path):
    _write_caches(tmp_path, snap_week=None, route_week=7)
    with pytest.raises(NflVolumeCacheStale, match="snap_through_week=0"):
        assert_nfl_volume_week_fresh(
            as_of=date(2026, 10, 25),
            root=tmp_path,
            label="test",
        )


def test_ahead_of_completed_week_passes(tmp_path: Path):
    _write_caches(tmp_path, snap_week=7, route_week=7)
    rep = assert_nfl_volume_week_fresh(
        as_of=date(2026, 10, 25),
        root=tmp_path,
        label="test",
    )
    assert rep["ok"] is True
    assert rep["completed_week"] == 6
    assert volume_week_report(as_of=date(2026, 10, 25), root=tmp_path)["ok"] is True


def test_pre_kick_lock_per_game(tmp_path: Path):
    log = tmp_path / "nfl_volume_snapshots.jsonl"
    df = pd.DataFrame(
        [
            {
                "player": "Early Kick",
                "team": "NYJ",
                "start_time": "2026-10-25T17:00:00+00:00",
                "snap_pct_L3": 80.0,
                "target_share_L3": 0.22,
                "route_pct_source": "snap_proxy",
            },
            {
                "player": "Late Kick",
                "team": "KC",
                "start_time": "2026-10-25T20:25:00+00:00",
                "snap_pct_L3": 90.0,
                "target_share_L3": 0.28,
                "route_pct_source": "snap_proxy",
            },
        ]
    )
    noon = datetime(2026, 10, 25, 16, 0, tzinfo=timezone.utc)
    n = append_pre_kick_snapshot(df, slate="2026-10-25", now=noon, path=log)
    assert n == 2

    after_early = datetime(2026, 10, 25, 18, 30, tzinfo=timezone.utc)
    moved = df.copy()
    moved.loc[moved["player"] == "Early Kick", "snap_pct_L3"] = 10.0
    moved.loc[moved["player"] == "Late Kick", "snap_pct_L3"] = 95.0
    n2 = append_pre_kick_snapshot(moved, slate="2026-10-25", now=after_early, path=log)
    assert n2 == 1

    lookup = load_pre_kick_lookup(log)
    early = lookup[("2026-10-25", "early kick")]
    late = lookup[("2026-10-25", "late kick")]
    assert early["snap_pct_L3"] == 80.0
    assert late["snap_pct_L3"] == 95.0


def test_apply_pre_kick_restores_frozen(tmp_path: Path):
    log = tmp_path / "nfl_volume_snapshots.jsonl"
    before = pd.DataFrame(
        [
            {
                "player": "Ja'Marr Chase",
                "start_time": "2026-10-25T20:25:00+00:00",
                "snap_pct_L3": 91.0,
                "target_share_L3": 0.31,
            }
        ]
    )
    append_pre_kick_snapshot(
        before,
        slate="2026-10-25",
        now=datetime(2026, 10, 25, 16, 0, tzinfo=timezone.utc),
        path=log,
    )
    later = before.copy()
    later["snap_pct_L3"] = 40.0
    later["target_share_L3"] = 0.05
    applied = apply_pre_kick_volume(later, slate="2026-10-25", path=log)
    assert float(applied.iloc[0]["snap_pct_L3"]) == 91.0
    assert float(applied.iloc[0]["target_share_L3"]) == 0.31
    assert bool(applied.iloc[0]["volume_pre_kick"]) is True
