"""Scraped payouts land on the dated archive, not only the live card."""
from __future__ import annotations

import json
from pathlib import Path

from utils.archive_payout_stamp import stamp_scraped_payouts_on_archives


def test_live_cdp_is_written_onto_goblin70_archive(tmp_path: Path):
    day = "2026-09-01"
    path = tmp_path / "data" / "reports" / f"goblin70_tickets_{day}.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "date": day,
                "tickets": [
                    {
                        "ticket_id": "g70-1",
                        "play": "power",
                        "n_legs": 2,
                        "legs": [
                            {
                                "player": "A Player",
                                "prop": "Pitcher Strikeouts",
                                "direction": "OVER",
                                "line": 5.5,
                                "pick_type": "goblin",
                                "sport": "MLB",
                            },
                            {
                                "player": "B Player",
                                "prop": "Hits",
                                "direction": "OVER",
                                "line": 0.5,
                                "pick_type": "goblin",
                                "sport": "MLB",
                            },
                        ],
                        "payout_source": "n_correct_p25",
                        "power_payout": 1.65,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    patch = {
        "by_ticket_id": {
            "g70-1": {
                "ticket_id": "g70-1",
                "power_min_x": 1.8,
                "display_min_x": 1.8,
                "payout_source": "live_cdp",
                "captured_at": "2026-09-01T01:00:00+00:00",
                "legs": [
                    {
                        "player": "A Player",
                        "prop": "Pitcher Strikeouts",
                        "direction": "OVER",
                        "line": 5.5,
                        "pick_type": "goblin",
                        "sport": "MLB",
                    },
                    {
                        "player": "B Player",
                        "prop": "Hits",
                        "direction": "OVER",
                        "line": 0.5,
                        "pick_type": "goblin",
                        "sport": "MLB",
                    },
                ],
                "n_legs": 2,
                "play": "power",
            }
        }
    }
    n = stamp_scraped_payouts_on_archives(day, patch, root=tmp_path)
    assert n == 1
    saved = json.loads(path.read_text(encoding="utf-8"))
    ticket = saved["tickets"][0]
    assert ticket["payout_source"] == "live_cdp"
    assert ticket["payout"]["power_min_x"] == 1.8
    assert ticket["payout"]["payout_source"] == "live_cdp"


def test_quote_rows_are_not_stamped(tmp_path: Path):
    day = "2026-09-02"
    path = tmp_path / "data" / "reports" / f"goblin70_tickets_{day}.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({"tickets": [{"ticket_id": "g70-2", "payout_source": "n_correct_p25"}]}),
        encoding="utf-8",
    )
    n = stamp_scraped_payouts_on_archives(
        day,
        {
            "by_ticket_id": {
                "g70-2": {
                    "ticket_id": "g70-2",
                    "power_min_x": 2.3,
                    "payout_source": "n_correct_p25",
                }
            }
        },
        root=tmp_path,
    )
    assert n == 0
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["tickets"][0]["payout_source"] == "n_correct_p25"
