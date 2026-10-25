"""Volume-floor backtests A/B: locked rules, no frozen-track edits."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from ml import volume_floor as V
from ml.volume_floor_backtest import persistence_table


def test_prereg_locked():
    spec = V.assert_prereg()
    assert spec["honest_ev"] is False
    assert "nfl_demon_volume_shadow" in spec["frozen_forward_tracks_do_not_edit"]
    assert "nfl_demon_rec_floor_shadow" in spec["frozen_forward_tracks_do_not_edit"]
    assert spec["on_better_rule"].startswith("register a new")


def test_frozen_prereg_files_untouched():
    root = Path(__file__).resolve().parents[1] / "ml" / "prereg"
    vol = (root / "nfl_demon_volume_2026-10-25.json").read_text(encoding="utf-8")
    rec = (root / "nfl_demon_rec_floor_2026-10-25.json").read_text(encoding="utf-8")
    assert '"amendments_closed": true' in vol.lower() or '"amendments_closed": true' in vol
    assert "nfl_demon_volume_shadow" in vol
    assert "nfl_demon_rec_floor_shadow" in rec
    assert "do_not_edit_sibling" in rec


def test_volume_props_only():
    assert V.is_volume_prop("NBA", "fga")
    assert V.is_volume_prop("NBA", "fta")
    assert not V.is_volume_prop("NBA", "points")
    assert not V.is_volume_prop("NBA", "fgm")
    assert V.is_volume_prop("MLB", "pitches_thrown")
    assert not V.is_volume_prop("MLB", "total_bases")
    assert V.is_volume_prop("NFL", "receptions")
    assert V.is_volume_prop("NFL", "rush_att")
    assert not V.is_volume_prop("NFL", "receiving_yards")
    assert V.is_volume_prop("Soccer", "shots")
    assert not V.is_volume_prop("Soccer", "sog")


def test_over_floor_is_l5_eq_5():
    assert V.floor_over({"l5_over": 5})
    assert not V.floor_over({"l5_over": 4})
    assert not V.floor_over({"l5_over": None})


def test_gate_needs_l5_l10_d():
    row = {"l5_over": 5, "l10_over": 8, "def_tier": "Weak"}
    assert V.passes_gate(row)
    assert not V.passes_gate({**row, "def_tier": "Avg"})
    assert not V.passes_gate({**row, "l10_over": 7})


def test_top2_share_rank():
    rows = [
        {"date": "2026-03-01", "team": "MIA", "player_norm": "a", "usage_pct": 30},
        {"date": "2026-03-01", "team": "MIA", "player_norm": "b", "usage_pct": 22},
        {"date": "2026-03-01", "team": "MIA", "player_norm": "c", "usage_pct": 10},
    ]
    ranks = V.team_share_ranks(rows)
    assert V.top2(rows[0], ranks)
    assert V.top2(rows[1], ranks)
    assert not V.top2(rows[2], ranks)


def test_bh_keeps_strong_cells_only():
    keep = V.bh_keep([0.001, 0.80, 0.02, None], alpha=0.10)
    assert 0 in keep
    assert 1 not in keep


def test_oos_split_is_by_date():
    train, test = V.oos_split_dates([f"2026-03-{d:02d}" for d in range(1, 11)])
    assert max(train) < min(test)
    assert len(train) + len(test) == 10


def test_persistence_does_not_use_same_game():
    df = pd.DataFrame(
        {
            "player_id": ["p"] * 6,
            "season": [2024] * 6,
            "week": [1, 2, 3, 4, 5, 6],
            "team": ["NYG"] * 6,
            "position": ["RB"] * 6,
            "rush_attempts": [8, 9, 10, 11, 12, 4],
            "receptions": [1] * 6,
            "targets": [2] * 6,
            "target_share": [0.1] * 6,
        }
    )
    out = persistence_table(df)
    # Week 6 L5 min must be min(8..12)=8, not include this week's 4.
    week6 = out[out["week"] == 6].iloc[0]
    assert float(week6["l5_min_rush"]) == 8.0
    assert V.persistence_hit(4.0, 8.0) is False
    assert V.persistence_hit(17.0, 8.0) is True


def test_volume_floor_module_does_not_import_frozen_loggers():
    import ml.volume_floor as mod
    import ml.volume_floor_backtest as run

    for path in (Path(mod.__file__), Path(run.__file__)):
        src = path.read_text(encoding="utf-8")
        assert "from utils.nfl_demon" not in src
        assert "import utils.nfl_demon" not in src
        assert "from utils.nfl_std_under_shadow" not in src
