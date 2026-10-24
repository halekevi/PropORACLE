"""Power wins and refunds: original leg count vs payout, then backfill VOID on full wins."""
from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path

from _power_slip_conflicts import _attrs, _is_power, _payout_map

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "db" / "MyTicketPerformance.db"


def _paid_multiple(attrs: dict) -> float | None:
    try:
        bet = float(attrs.get("amount_bet_cents") or 0)
        won = float(attrs.get("amount_won_cents") or 0)
    except (TypeError, ValueError):
        return None
    if bet <= 0:
        return None
    return won / bet


def _full_multiple(attrs: dict) -> float | None:
    try:
        legs = int(attrs.get("parlay_count") or 0)
    except (TypeError, ValueError):
        return None
    pays = _payout_map(attrs)
    if legs and legs in pays and pays[legs] > 0:
        return pays[legs]
    return None


def main() -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    grades: dict[str, Counter] = {}
    for row in conn.execute("SELECT entry_id, result FROM entry_leg_results"):
        grades.setdefault(str(row["entry_id"]), Counter())[str(row["result"] or "").upper()] += 1
    leagues: dict[str, Counter] = {}
    for row in conn.execute("SELECT entry_id, league FROM entry_legs"):
        leagues.setdefault(str(row["entry_id"]), Counter())[str(row["league"] or "")] += 1

    power_settled = 0
    wins = []
    refunds = []
    sport_counts: Counter = Counter()
    for row in conn.execute("SELECT entry_id, created_at, raw_json FROM user_entries"):
        attrs = _attrs(row["raw_json"] or "")
        if _is_power(attrs) is not True:
            continue
        result = str(attrs.get("result") or "").lower()
        if result not in {"won", "lost", "refunded", "partial_win"}:
            continue
        power_settled += 1
        for league, n in leagues.get(str(row["entry_id"]), {}).items():
            sport_counts[league] += n
        if result == "won":
            wins.append((str(row["entry_id"]), attrs, grades.get(str(row["entry_id"]), Counter())))
        elif result == "refunded":
            refunds.append((str(row["entry_id"]), attrs, grades.get(str(row["entry_id"]), Counter())))

    print(f"settled power {power_settled} full wins {len(wins)} refunds {len(refunds)}")
    print("leg leagues", sport_counts.most_common(12))
    backfill_ids = []
    print("WINS")
    for eid, attrs, our in wins:
        paid = _paid_multiple(attrs)
        full = _full_multiple(attrs)
        match = paid is not None and full is not None and abs(paid - full) < 0.08
        print(
            eid,
            "legs",
            attrs.get("parlay_count"),
            "paid",
            None if paid is None else round(paid, 3),
            "full",
            full,
            "matches_original",
            match,
            "our",
            dict(our),
            "created",
            attrs.get("created_at"),
        )
        if match:
            backfill_ids.append(eid)
    print("REFUNDS")
    for eid, attrs, our in refunds:
        voids = our.get("VOID", 0) + our.get("NO_ACTUAL", 0)
        decided = our.get("HIT", 0) + our.get("MISS", 0)
        try:
            legs = int(attrs.get("parlay_count") or 0)
        except (TypeError, ValueError):
            legs = 0
        remaining = legs - voids
        print(
            eid,
            "legs",
            legs,
            "paid",
            _paid_multiple(attrs),
            "our",
            dict(our),
            "remaining_if_voids_drop",
            remaining,
            "refund_fits_under_2_legs",
            remaining < 2,
        )
    if backfill_ids:
        cur = conn.execute(
            f"""
            UPDATE entry_leg_results
            SET result = 'HIT', source = 'pp_power_full_win'
            WHERE result = 'VOID'
              AND entry_id IN ({",".join("?" for _ in backfill_ids)})
            """,
            backfill_ids,
        )
        conn.commit()
        print(f"backfilled VOID to HIT on {cur.rowcount} legs across {len(backfill_ids)} full-pay wins")
    conn.close()


if __name__ == "__main__":
    main()
