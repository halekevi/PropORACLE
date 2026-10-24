"""3PM volume observe: prior-box attach + join. Live Goblin-70 is unchanged."""
from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from utils.ticket_70_pool import (  # noqa: E402
    goblin_70_eligible,
    wnba_threes_vol_observe_eligible,
)
from utils.wnba_threes_vol_observe import (  # noqa: E402
    attach_vol,
    join_grades,
    vol_before_slate,
)


def _gob(**kwargs) -> dict:
    row = {
        "sport": "WNBA",
        "player": "Allisha Gray",
        "prop": "3-PT Made",
        "side": "OVER",
        "line": 0.5,
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 6,
        "cover": 0.8,
        "def": "Avg",
    }
    row.update(kwargs)
    return row


def test_vol_before_slate_skips_on_slate_box(monkeypatch):
    from utils import wnba_threes_vol_observe as O

    fake = {
        ("allisha gray", "2026-10-19"): {
            "l10_3pa_g": 5.0,
            "season_3pp": 0.348,
            "season_3pa": 80.0,
        },
        ("allisha gray", "2026-10-21"): {
            "l10_3pa_g": 9.9,
            "season_3pp": 0.99,
            "season_3pa": 999.0,
        },
    }
    monkeypatch.setattr(O, "_prior_index", lambda: fake)
    O._boxes_by_player.cache_clear()
    try:
        st = vol_before_slate("Allisha Gray", "2026-10-21")
        assert st["l10_3pa_g"] == 5.0
        assert st["season_3pa"] == 80.0
        rec = attach_vol(_gob(), "2026-10-21")
        assert rec["l10_3pa_g"] == 5.0
        assert wnba_threes_vol_observe_eligible(rec)
        assert not goblin_70_eligible(rec)
    finally:
        O._boxes_by_player.cache_clear()


def test_allisha_gray_2026_10_21_against_espn_cache():
    from utils import wnba_threes_vol_observe as O
    from utils.wnba_threes_vol_observe import CACHE

    if not CACHE.is_file():
        return
    O._prior_index.cache_clear()
    O._boxes_by_player.cache_clear()
    rec = attach_vol(_gob(), "2026-10-21")
    # Closing L10 through the last completed box (not the empty on-slate key).
    assert rec.get("l10_3pa_g") is not None
    assert float(rec["l10_3pa_g"]) > 0
    assert rec.get("season_3pp") is not None
    assert float(rec.get("season_3pa") or 0) > 0
    assert rec.get("l10_3pa_g") != 9.9


def test_join_fills_hit_miss(tmp_path, monkeypatch):
    from utils import wnba_threes_vol_observe as O

    monkeypatch.setattr(O, "LEDGER", tmp_path / "obs.jsonl")
    monkeypatch.setattr(O, "LATEST", tmp_path / "latest.json")
    monkeypatch.setattr(O, "SUMMARY", tmp_path / "summary.json")
    row = attach_vol(_gob(l10_3pa_g=6.0, season_3pp=0.36, season_3pa=80), "2026-10-21")
    O.log_board("2026-10-21", [row])
    graded = [
        {
            "sport": "WNBA",
            "player": "Allisha Gray",
            "prop": "3-PT Made",
            "line": 0.5,
            "direction": "OVER",
            "pick_type": "Goblin",
            "result": "HIT",
            "actual_value": 2,
        }
    ]
    gp = tmp_path / "graded_props_2026-10-21.json"
    gp.write_text(json.dumps({"props": graded}), encoding="utf-8")
    summary = join_grades("2026-10-21", graded_file=gp)
    assert summary["proposed_decided"] == 1
    assert summary["proposed_hit"] == 1
    assert summary["hit"] == 1
    kept = O._read_jsonl()
    assert kept[0]["result"] == "HIT"
    assert not goblin_70_eligible(row)
