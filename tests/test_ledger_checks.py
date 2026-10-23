from __future__ import annotations

import json
from pathlib import Path

from proporacle.data.ledger_checks import run_ledger_checks
from proporacle.data.leg_ledger import ingest_graded_file, ingest_tickets_file


def test_warn_only_join_and_usage(tmp_path: Path):
    ledger = tmp_path / "ledger"
    gpath = tmp_path / "graded_props_2026-10-20.json"
    gpath.write_text(
        json.dumps(
            {
                "date": "2026-10-20",
                "props": [
                    {
                        "sport": "NBA",
                        "player": "Joel Embiid",
                        "prop": "Steals",
                        "line": 0.5,
                        "direction": "UNDER",
                        "pick_type": "Standard",
                        "usage_pct": "",
                        "league": "",
                        "ml_prob": "0.67",
                        "result": "MISS",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    ingest_graded_file(gpath, root=ledger)
    tpath = tmp_path / "tickets.json"
    tpath.write_text(
        json.dumps(
            {
                "date": "2026-10-20",
                "groups": [
                    {
                        "tickets": [
                            {
                                "ticket_id": "t1",
                                "legs": [
                                    {
                                        "sport": "TENNIS",
                                        "player": "Schoolkate",
                                        "prop": "Total Games Won",
                                        "line": 12.5,
                                        "direction": "OVER",
                                        "pick_type": "standard",
                                    }
                                ],
                            }
                        ]
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    ingest_tickets_file(tpath, root=ledger)
    report = run_ledger_checks(ledger, since="2026-10-01", repo=tmp_path)
    rules = {w.rule for w in report.warnings}
    assert "usage_fill" in rules
    assert "league_fill" in rules
    assert "ticket_join" in rules
    assert report.ok is False
