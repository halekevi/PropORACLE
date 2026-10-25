"""Projection as-of: later boxes must not change an earlier slate."""
from __future__ import annotations

import pandas as pd

from utils.nba_player_projection import NbaBoxIndex, project_from_index
from datetime import datetime, timezone

from utils.nba_proj_edge_snapshot import (
    apply_frozen_proj,
    leg_key,
    write_frozen_day,
    write_pre_tip_frozen_day,
)


def _boxes() -> pd.DataFrame:
    # Same player: 20 pts before the slate, 40 pts after. If the after-game
    # leaks, mu jumps.
    return pd.DataFrame(
        [
            {
                "game_date": "2026-02-01",
                "player": "Test Guard",
                "team": "LAL",
                "position": "SG",
                "espn_athlete_id": "1",
                "minutes": 30.0,
                "pts": 20.0,
                "reb": 4.0,
                "ast": 5.0,
                "stl": 1.0,
                "blk": 0.0,
                "tov": 2.0,
                "fg3m": 2.0,
                "pra": 29.0,
                "pr": 24.0,
                "pa": 25.0,
                "ra": 9.0,
            },
            {
                "game_date": "2026-03-01",
                "player": "Test Guard",
                "team": "LAL",
                "position": "SG",
                "espn_athlete_id": "1",
                "minutes": 30.0,
                "pts": 40.0,
                "reb": 4.0,
                "ast": 5.0,
                "stl": 1.0,
                "blk": 0.0,
                "tov": 2.0,
                "fg3m": 2.0,
                "pra": 49.0,
                "pr": 44.0,
                "pa": 45.0,
                "ra": 9.0,
            },
        ]
    )


def test_later_boxes_do_not_change_february_mu():
    full = NbaBoxIndex.from_frame(_boxes())
    sliced = full.restrict_before("2026-02-19")
    assert len(full.df) == 2
    assert len(sliced.df) == 1
    kw = dict(espn_id="1", player="Test Guard", prop="Points", line=19.5, before_date="2026-02-19")
    a = project_from_index(full, **kw)
    b = project_from_index(sliced, **kw)
    assert a is not None and b is not None
    assert abs(a.mu_shrunk - b.mu_shrunk) < 1e-9
    assert abs(a.proj_minutes - b.proj_minutes) < 1e-9


def test_frozen_snapshot_roundtrip(tmp_path, monkeypatch):
    from utils import nba_proj_edge_snapshot as snap

    monkeypatch.setattr(snap, "SNAPSHOT_DIR", tmp_path)
    df = pd.DataFrame(
        [
            {
                "file_date": "2026-02-19",
                "player": "Test Guard",
                "prop": "Points",
                "line": 19.5,
                "direction": "UNDER",
                "pick_type": "Standard",
                "proj_edge": 0.11,
                "proj_mu": 18.0,
                "proj_hold": 0.0,
                "_gate": True,
            }
        ]
    )
    assert write_frozen_day(df, "2026-02-19") == 1
    assert write_frozen_day(df, "2026-02-19") == 0  # no overwrite
    later = df.copy()
    later["proj_edge"] = 0.99
    later["proj_mu"] = 99.0
    out = apply_frozen_proj(later, snap.load_frozen_day("2026-02-19"))
    assert float(out.loc[0, "proj_edge"]) == 0.11
    assert float(out.loc[0, "proj_mu"]) == 18.0
    assert int(out.loc[0, "proj_edge_frozen"]) == 1
    assert "2026-02-19|test guard|points|19.5|standard|UNDER" == leg_key(df.iloc[0])


def test_live_freeze_replaces_before_tip_only(tmp_path, monkeypatch):
    from utils import nba_proj_edge_snapshot as snap

    monkeypatch.setattr(snap, "SNAPSHOT_DIR", tmp_path)
    first = pd.DataFrame(
        [
            {
                "file_date": "2026-10-25",
                "player": "Austin Reaves",
                "prop": "Points",
                "line": 16.5,
                "direction": "OVER",
                "pick_type": "Goblin",
                "proj_edge": 0.10,
                "proj_mu": 18.0,
            }
        ]
    )
    later = first.copy()
    later["proj_edge"] = 0.20
    post = first.copy()
    post["proj_edge"] = 0.90
    assert write_pre_tip_frozen_day(first, "2026-10-25", pre_tip=True) == 1
    assert write_pre_tip_frozen_day(later, "2026-10-25", pre_tip=True) == 1
    rec = snap.load_frozen_day("2026-10-25")
    key = leg_key(first.iloc[0])
    assert rec[key]["proj_edge"] == 0.20
    assert rec[key]["pre_tip"] is True
    assert write_pre_tip_frozen_day(post, "2026-10-25", pre_tip=False) == 0
    rec2 = snap.load_frozen_day("2026-10-25")
    assert rec2[key]["proj_edge"] == 0.20


def test_freeze_locks_per_game_not_per_file(tmp_path, monkeypatch):
    from utils import nba_proj_edge_snapshot as snap

    monkeypatch.setattr(snap, "SNAPSHOT_DIR", tmp_path)
    early_tip = "2026-10-25T21:00:00+00:00"
    late_tip = "2026-10-25T23:00:00+00:00"
    first = pd.DataFrame(
        [
            {
                "file_date": "2026-10-25",
                "player": "Austin Reaves",
                "team": "LAL",
                "opp_team": "UTA",
                "prop": "Points",
                "line": 16.5,
                "direction": "OVER",
                "pick_type": "Goblin",
                "proj_edge": 0.10,
                "tip_utc": early_tip,
            },
            {
                "file_date": "2026-10-25",
                "player": "Michael Porter",
                "team": "BKN",
                "opp_team": "IND",
                "prop": "Points",
                "line": 14.5,
                "direction": "OVER",
                "pick_type": "Goblin",
                "proj_edge": 0.12,
                "tip_utc": late_tip,
            },
        ]
    )
    t0 = datetime(2026, 10, 25, 16, 0, tzinfo=timezone.utc)
    assert write_pre_tip_frozen_day(first, "2026-10-25", now=t0) == 2
    after_lal = first.copy()
    after_lal.loc[after_lal["player"].eq("Austin Reaves"), "proj_edge"] = 0.99
    after_lal.loc[after_lal["player"].eq("Michael Porter"), "proj_edge"] = 0.22
    t1 = datetime(2026, 10, 25, 21, 30, tzinfo=timezone.utc)
    assert write_pre_tip_frozen_day(after_lal, "2026-10-25", now=t1) == 1
    rec = snap.load_frozen_day("2026-10-25")
    reaves = rec[leg_key(first.iloc[0])]
    porter = rec[leg_key(first.iloc[1])]
    assert reaves["proj_edge"] == 0.10
    assert reaves["frozen_at"].startswith("2026-10-25T16:00")
    assert porter["proj_edge"] == 0.22
    assert porter["frozen_at"].startswith("2026-10-25T21:30")
