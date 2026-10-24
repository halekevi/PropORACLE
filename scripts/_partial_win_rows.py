import sqlite3
from pathlib import Path

from _power_slip_conflicts import _attrs, _is_power

c = sqlite3.connect(Path(__file__).resolve().parents[1] / "data" / "db" / "MyTicketPerformance.db")
rows = c.execute("SELECT entry_id, raw_json FROM user_entries").fetchall()
for eid, raw in rows:
    attrs = _attrs(raw or "")
    if str(attrs.get("result") or "").lower() != "partial_win":
        continue
    print(
        eid,
        "power" if _is_power(attrs) else "flex_or_other",
        "protect",
        attrs.get("pick_protection"),
        "legs",
        attrs.get("parlay_count"),
        "bet",
        attrs.get("amount_bet_cents"),
        "won",
        attrs.get("amount_won_cents"),
        "payouts",
        attrs.get("payouts"),
    )
