"""analysis_legs must drop leftover-board rows, not count them as hits."""
from __future__ import annotations

import pandas as pd

from proporacle.data import leg_ledger as L


def test_analysis_legs_drops_stale_board(monkeypatch, capsys):
    df = pd.DataFrame(
        [
            {
                "sport": "NBA1Q",
                "slate_date": "2026-10-08",
                "player": "phantom",
                "face_verified": True,
                "stat_attach_ok": True,
                "invalid_reason": "stale_board",
            },
            {
                "sport": "NBA",
                "slate_date": "2026-10-27",
                "player": "real",
                "face_verified": True,
                "stat_attach_ok": True,
                "invalid_reason": "",
            },
        ]
    )
    monkeypatch.setattr(L, "current_legs", lambda root=None: df)
    kept = L.analysis_legs()
    assert list(kept["player"]) == ["real"]
    assert "stale_board" in capsys.readouterr().out
