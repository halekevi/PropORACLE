"""Detail the Power full-win and refund slips against our leg grades."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path

from _power_slip_conflicts import _attrs, _is_power

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "db" / "MyTicketPerformance.db"


def main() -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    grades = {}
    for row in conn.execute("SELECT entry_id, result FROM entry_leg_results"):
        grades.setdefault(str(row["entry_id"]), Counter())[str(row["result"] or "").upper()] += 1
    legs = {}
    for row in conn.execute("SELECT entry_id FROM entry_legs"):
        legs[str(row["entry_id"])] = legs.get(str(row["entry_id"]), 0) + 1

    for row in conn.execute("SELECT entry_id, raw_json FROM user_entries WHERE raw_json IS NOT NULL"):
        attrs = _attrs(row["raw_json"] or "")
        if _is_power(attrs) is not True:
            continue
        result = str(attrs.get("result") or "").lower()
        if result not in {"won", "refunded", "partial_win"}:
            continue
        eid = str(row["entry_id"])
        print(
            eid,
            result,
            "bet",
            attrs.get("amount_bet_cents"),
            "won_cents",
            attrs.get("amount_won_cents"),
            "legs",
            attrs.get("parlay_count"),
            "stored_legs",
            legs.get(eid, 0),
            "our",
            dict(grades.get(eid, {})),
        )


if __name__ == "__main__":
    main()
