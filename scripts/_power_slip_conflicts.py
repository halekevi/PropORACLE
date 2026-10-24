"""Power slips: PrizePicks full win vs our MISS, and refunds vs NO_ACTUAL.

Flex pays with misses, so Flex is excluded. Per-leg PrizePicks grades are not
in the capture yet; this uses the slip-level result on user_entries.raw_json
against entry_leg_results.
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "db" / "MyTicketPerformance.db"


def _attrs(raw: str) -> dict:
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    data = obj.get("data") if isinstance(obj.get("data"), dict) else obj
    attrs = data.get("attributes") if isinstance(data, dict) else None
    return attrs if isinstance(attrs, dict) else {}


def _payout_map(attrs: dict) -> dict[int, float]:
    """Correct-count -> multiplier. A Flex slip has a payout below the full count."""
    out: dict[int, float] = {}
    payouts = attrs.get("payouts")
    if isinstance(payouts, dict):
        for key, val in payouts.items():
            try:
                n = int(key)
            except (TypeError, ValueError):
                continue
            if isinstance(val, dict):
                mult = val.get("multiplier")
            else:
                mult = val
            try:
                out[n] = float(mult or 0)
            except (TypeError, ValueError):
                continue
    return out


def _is_power(attrs: dict) -> bool | None:
    """True when only the all-correct line pays. Flex pays with misses."""
    mode = str(attrs.get("game_mode") or "").lower()
    if "flex" in mode:
        return False
    if mode in {"power", "pickem"}:
        return True
    try:
        legs = int(attrs.get("parlay_count") or 0)
    except (TypeError, ValueError):
        legs = 0
    pays = _payout_map(attrs)
    if legs and pays:
        partial = any(mult > 0 for n, mult in pays.items() if n < legs)
        full = pays.get(legs, 0) > 0
        if full and not partial:
            return True
        if partial:
            return False
    return None


def main() -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    entries = conn.execute(
        "SELECT entry_id, status, raw_json FROM user_entries WHERE raw_json IS NOT NULL"
    ).fetchall()
    grades = conn.execute(
        "SELECT entry_id, result FROM entry_leg_results"
    ).fetchall()
    by_entry: dict[str, Counter] = {}
    for row in grades:
        by_entry.setdefault(str(row["entry_id"]), Counter())[str(row["result"] or "").upper()] += 1

    key_counts: Counter = Counter()
    result_counts: Counter = Counter()
    sample_printed = 0
    power_n = flex_n = unknown_n = 0
    checked = 0
    no_grades = 0
    conflicts = []
    refund_checked = 0
    refund_mismatch = []
    refund_ok = 0

    for row in entries:
        attrs = _attrs(row["raw_json"] or "")
        for key in attrs:
            key_counts[key] += 1
        result = str(attrs.get("result") or "").strip().lower()
        if not result:
            result = str(row["status"] or "").strip().lower()
        result_counts[result] += 1
        if result in {"won", "lost", "refunded", "partial_win"} and sample_printed < 8:
            sample_printed += 1
            try:
                top = json.loads(row["raw_json"] or "{}")
            except Exception:
                top = {}
            data = top.get("data") if isinstance(top, dict) else None
            print(
                "SAMPLE",
                row["entry_id"],
                "status",
                row["status"],
                "type",
                (data or {}).get("type") if isinstance(data, dict) else type(top).__name__,
                sorted(attrs.keys())[:30],
            )
            for key in (
                "game_mode", "payouts", "payout_multipliers", "full_payout_multipliers",
                "pick_protection", "amount_won_cents", "amount_bet_cents",
                "parlay_count", "result", "contest_result",
            ):
                if key in attrs:
                    val = attrs[key]
                    text = str(val)
                    print(f"  {key}={text[:400]}")
        kind = _is_power(attrs)
        if kind is True:
            power_n += 1
        elif kind is False:
            flex_n += 1
            continue
        else:
            unknown_n += 1
            continue
        legs = by_entry.get(str(row["entry_id"]))
        if result in {"refunded", "refund", "void", "voided", "cancelled", "canceled"}:
            refund_checked += 1
            if not legs:
                refund_mismatch.append((row["entry_id"], "no our grades", result))
                continue
            decided = legs.get("HIT", 0) + legs.get("MISS", 0)
            open_legs = legs.get("NO_ACTUAL", 0) + legs.get("VOID", 0)
            if decided and not open_legs:
                refund_mismatch.append((row["entry_id"], dict(legs), result))
            elif open_legs or not decided:
                refund_ok += 1
            continue
        if result not in {"won", "win"}:
            continue
        checked += 1
        if not legs:
            no_grades += 1
            continue
        if legs.get("MISS", 0) > 0:
            conflicts.append((row["entry_id"], dict(legs), result))

    print(f"entries {len(entries)}")
    print(f"power {power_n} flex {flex_n} unknown {unknown_n}")
    print(f"slip results {result_counts.most_common(12)}")
    print(f"power full-win checked {checked} missing our grades {no_grades}")
    print(f"conflicts (PP full win + our MISS) {len(conflicts)}")
    for item in conflicts[:25]:
        print("  conflict", item)
    print(f"refunded power checked {refund_checked} lined up {refund_ok} mismatch {len(refund_mismatch)}")
    for item in refund_mismatch[:25]:
        print("  refund", item)
    print("attribute keys", key_counts.most_common(20))


if __name__ == "__main__":
    main()
