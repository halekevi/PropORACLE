"""Detect props whose history is skewed vs the global chron OOS split.

WNBA Goblin FGA failure mode (2026-09-18): the prop barely existed before late
July, so a sport-wide 70/30 date split put nearly all FGA rows in the
\"holdout.\" Ranked tables then showed oos_ticket_clear on what was mostly the
full prop sample — not unseen data.

Call ``split_balance_for_cell`` per book×prop after the global train/oos dates
are fixed. Flag when most of that cell's rows (or dates) sit on one side of the
split. Downstream should treat ``oos_ticket_clear`` as unreliable when flagged.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

# Most of the cell's history on the OOS side of the *global* split.
OOS_ROW_SHARE_FLAG = 0.70
# Same for train (less dangerous for false OOS clears, still noted).
TRAIN_ROW_SHARE_FLAG = 0.70
# Absolute floor: tiny train + large OOS is the FGA signature even if share
# is just under 70% on small cells.
MIN_TRAIN_ROWS_FOR_RELIABLE_OOS = 15
MIN_OOS_ROWS_TO_CARE = 25


def split_balance_for_cell(
    rows: Iterable[dict[str, Any]],
    train_dates: set[str],
    test_dates: set[str],
) -> dict[str, Any]:
    """Summarize how one book×prop pool sits vs the global chron split."""
    pool = list(rows)
    n = len(pool)
    train_n = sum(1 for r in pool if r.get("date") in train_dates)
    oos_n = sum(1 for r in pool if r.get("date") in test_dates)
    dates = sorted({str(r.get("date") or "") for r in pool if r.get("date")})
    train_dates_hit = sorted(d for d in dates if d in train_dates)
    oos_dates_hit = sorted(d for d in dates if d in test_dates)
    train_share = (train_n / n) if n else None
    oos_share = (oos_n / n) if n else None

    reasons: list[str] = []
    if n > 0 and oos_share is not None and oos_share >= OOS_ROW_SHARE_FLAG:
        reasons.append(
            f"late_prop_oos_skew: {oos_share:.0%} of cell rows fall in global OOS "
            f"({oos_n}/{n}); holdout is mostly this prop's own history"
        )
    if n > 0 and train_share is not None and train_share >= TRAIN_ROW_SHARE_FLAG:
        reasons.append(
            f"early_prop_train_skew: {train_share:.0%} of cell rows fall in global train "
            f"({train_n}/{n})"
        )
    if (
        train_n < MIN_TRAIN_ROWS_FOR_RELIABLE_OOS
        and oos_n >= MIN_OOS_ROWS_TO_CARE
        and n >= MIN_OOS_ROWS_TO_CARE
    ):
        reasons.append(
            f"train_starved_vs_oos: train_n={train_n} < {MIN_TRAIN_ROWS_FOR_RELIABLE_OOS} "
            f"while oos_n={oos_n} (global split unreliable for this prop)"
        )

    # Deduplicate overlapping late/starved messages for the same cell.
    reliable = not any(
        r.startswith("late_prop_oos_skew") or r.startswith("train_starved_vs_oos")
        for r in reasons
    )
    return {
        "n": n,
        "train_n": train_n,
        "oos_n": oos_n,
        "train_share": None if train_share is None else round(train_share, 4),
        "oos_share": None if oos_share is None else round(oos_share, 4),
        "n_dates": len(dates),
        "train_dates_n": len(train_dates_hit),
        "oos_dates_n": len(oos_dates_hit),
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
        "oos_split_reliable": reliable,
        "flags": reasons,
    }


def split_balance_by_cell(
    rows: list[dict[str, Any]],
    train_dates: set[str],
    test_dates: set[str],
    *,
    book_key: str = "book",
    prop_key: str = "prop",
) -> dict[tuple[str, str], dict[str, Any]]:
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        book = str(r.get(book_key) or "")
        prop = str(r.get(prop_key) or "")
        if not book or not prop:
            continue
        buckets[(book, prop)].append(r)
    return {
        key: split_balance_for_cell(pool, train_dates, test_dates)
        for key, pool in buckets.items()
    }


def flagged_split_imbalance(
    by_cell: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    out = []
    for (book, prop), bal in sorted(by_cell.items(), key=lambda x: -(x[1].get("oos_share") or 0)):
        if bal.get("oos_split_reliable", True) and not bal.get("flags"):
            continue
        if not bal.get("flags"):
            continue
        out.append({"book": book, "prop": prop, **bal})
    return out
