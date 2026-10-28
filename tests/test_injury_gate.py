"""Phase 1 injury gate: OUT hard-drop, soft flags, staleness."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from utils.injury_gate import (
    drop_injury_out_rows,
    injury_snapshot_stale,
    is_injury_out,
    is_injury_soft_flag,
    normalize_inj_status,
    reset_injury_out_cache,
)


def setup_function() -> None:
    reset_injury_out_cache()


def test_normalize_out_variants() -> None:
    assert normalize_inj_status("Out") == "OUT"
    assert normalize_inj_status("O") == "OUT"
    assert normalize_inj_status("IR") == "OUT"
    assert normalize_inj_status("PUP") == "OUT"
    assert normalize_inj_status("Questionable") == "Questionable"
    assert normalize_inj_status("GTD") == "GTD"
    assert normalize_inj_status("Day-To-Day") == "GTD"
    assert normalize_inj_status("Doubtful") == "Doubtful"
    assert normalize_inj_status("Probable") == "Probable"
    assert normalize_inj_status("") == ""


def test_is_injury_out_row() -> None:
    assert is_injury_out({"injury_status": "Out"})
    assert is_injury_out({"inj_status": "IR"})
    assert not is_injury_out({"injury_status": "Questionable"})
    assert not is_injury_out({"injury_status": "GTD"})
    assert not is_injury_out({})


def test_soft_flag() -> None:
    assert is_injury_soft_flag("Questionable")
    assert is_injury_soft_flag("GTD")
    assert is_injury_soft_flag("Doubtful")
    assert not is_injury_soft_flag("Out")
    assert not is_injury_soft_flag("Probable")


def test_stale_fail_closed() -> None:
    now = datetime(2026, 10, 27, 18, 0, tzinfo=timezone.utc)
    fresh = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    old = (now - timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert not injury_snapshot_stale(fresh, now=now)
    assert injury_snapshot_stale(old, now=now)
    assert injury_snapshot_stale("", now=now)
    assert injury_snapshot_stale(None, now=now)


def test_step8_drops_out_rows() -> None:
    import pandas as pd

    df = pd.DataFrame(
        {
            "player": ["A", "B", "B", "C"],
            "injury_status": ["Out", "Questionable", "Questionable", ""],
            "player_injury_status": ["OUT", "Questionable", "Questionable", "HEALTHY"],
        }
    )
    out = drop_injury_out_rows(df, log_prefix="NBA OUT dropped", feed_rows=[])
    assert list(out["player"]) == ["B", "B", "C"]


def test_feed_out_matches_late_scratch(capsys) -> None:
    import pandas as pd

    df = pd.DataFrame(
        {
            "player": ["Jalen Brunson", "Jalen Brunson", "Josh Hart"],
            "injury_status": ["healthy", "healthy", "Questionable"],
        }
    )
    feed = [
        {"sport": "NBA", "player": "Jalen Brunson", "status": "Out"},
        {"sport": "NBA", "player": "Victor Wembanyama", "status": "Out"},
        {"sport": "WNBA", "player": "Josh Hart", "status": "Out"},
    ]
    out = drop_injury_out_rows(
        df, log_prefix="NBA OUT dropped", sport="NBA", feed_rows=feed
    )
    assert list(out["player"]) == ["Josh Hart"]
    log = capsys.readouterr().out
    assert "feed_out=2 matched=1 unmatched=1" in log
    assert "Victor Wembanyama" in log


def test_goblin70_drops_out() -> None:
    from utils.ticket_70_pool import goblin_70_eligible

    row = {
        "pick_type": "Goblin",
        "side": "OVER",
        "sport": "NBA",
        "prop_type": "Points",
        "prop": "points",
        "line": 10.5,
        "l5_over": 5,
        "l10_over": 8,
        "injury_status": "Out",
    }
    assert goblin_70_eligible(row) is False
