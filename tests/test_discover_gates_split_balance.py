"""Prop vs global chron-split balance (WNBA FGA failure mode)."""

from __future__ import annotations

from utils.discover_gates_split_balance import (
    flagged_split_imbalance,
    split_balance_by_cell,
    split_balance_for_cell,
)


def _rows(dates_hits: list[tuple[str, int]]) -> list[dict]:
    out = []
    for d, n in dates_hits:
        for i in range(n):
            out.append({"date": d, "book": "Goblin OVER", "prop": "fga", "hit": 1})
    return out


def test_late_prop_oos_skew_flags_fga_style():
    # Sport calendar: May–Aug; prop only appears late July+.
    train = {f"2026-05-{d:02d}" for d in range(1, 32)} | {
        f"2026-06-{d:02d}" for d in range(1, 31)
    } | {f"2026-07-{d:02d}" for d in range(1, 16)}
    test = {f"2026-07-{d:02d}" for d in range(16, 32)} | {
        f"2026-08-{d:02d}" for d in range(1, 31)
    }
    # 18 train-window rows, 300 oos-window rows
    rows = _rows([("2026-07-12", 10), ("2026-07-30", 8)] + [(f"2026-08-{d:02d}", 10) for d in range(1, 31)])
    bal = split_balance_for_cell(rows, train, test)
    assert bal["oos_split_reliable"] is False
    assert bal["oos_share"] is not None and bal["oos_share"] >= 0.70
    assert any("late_prop_oos_skew" in f or "train_starved" in f for f in bal["flags"])


def test_balanced_prop_is_reliable():
    train = {f"2026-05-{d:02d}" for d in range(1, 22)}
    test = {f"2026-05-{d:02d}" for d in range(22, 32)}
    rows = _rows([(d, 5) for d in sorted(train | test)])
    for r in rows:
        r["prop"] = "points"
    bal = split_balance_for_cell(rows, train, test)
    assert bal["oos_split_reliable"] is True
    assert not any("late_prop_oos_skew" in f for f in bal["flags"])


def test_flagged_split_imbalance_lists_cells():
    train = {f"2026-0{m}-{d:02d}" for m in (5, 6) for d in range(1, 29)}
    test = {f"2026-0{m}-{d:02d}" for m in (7, 8) for d in range(1, 29)}
    rows = []
    for d in sorted(train)[:20]:
        rows.append({"date": d, "book": "Goblin OVER", "prop": "points", "hit": 1})
    for d in sorted(test)[:40]:
        rows.append({"date": d, "book": "Goblin OVER", "prop": "fga", "hit": 1})
        rows.append({"date": d, "book": "Goblin OVER", "prop": "points", "hit": 1})
    by = split_balance_by_cell(rows, train, test)
    flagged = flagged_split_imbalance(by)
    props = {(f["book"], f["prop"]) for f in flagged if not f.get("oos_split_reliable", True)}
    assert ("Goblin OVER", "fga") in props
    assert ("Goblin OVER", "points") not in props or by[("Goblin OVER", "points")].get(
        "oos_split_reliable", True
    )
