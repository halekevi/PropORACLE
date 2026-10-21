#!/usr/bin/env python3
"""Verify Mitchell multi-stat cannot co-occur on Goblin-70 / diversity tickets."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from build_goblin70_tickets import can_add, load_today_board, unique_by_player  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, goblin_sort_key  # noqa: E402
from utils.ticket_diversity import _has_same_player_multistat, apply_diversity_filter  # noqa: E402

DATE = "2026-09-18"


def main() -> int:
    rows = [r for r in load_today_board(DATE) if str(r.get("sport") or "").upper() == "WNBA"]
    mit = [
        r
        for r in rows
        if "mitchell" in str(r.get("player") or "").lower() and goblin_70_eligible(r)
    ]
    print(f"Mitchell goblin_70_eligible raw: {len(mit)}")
    for r in sorted(mit, key=goblin_sort_key)[:15]:
        print(
            f"  {str(r.get('prop')):20} O{r.get('line')} "
            f"L5={r.get('l5_over')} L10={r.get('l10_over')} "
            f"badge={r.get('promo') or r.get('badge')} tier={r.get('prop_tier')}"
        )
    uniq = unique_by_player(mit, goblin_sort_key)
    print(f"After unique_by_player: {len(uniq)} -> {[(u.get('prop'), u.get('line')) for u in uniq]}")
    assert len(uniq) <= 1, "G70 pool must keep at most one Mitchell leg"

    if len(mit) >= 2 and uniq:
        other = next(r for r in mit if str(r.get("prop")) != str(uniq[0].get("prop")))
        ok = can_add([uniq[0]], other, wnba_cap=2)
        print(f"can_add second Mitchell prop ({other.get('prop')}): {ok} (expect False)")
        assert ok is False

    fake = {
        "ticket_id": "mitchell_stack",
        "n_legs": 4,
        "base_ev": 2.0,
        "legs": [
            {
                "player": "Kelsey Mitchell",
                "prop_type": "Points",
                "line": 19.5,
                "direction": "OVER",
                "edge": 2.0,
                "sample_size": 20,
            },
            {
                "player": "Kelsey Mitchell",
                "prop_type": "Pts+Rebs+Asts",
                "line": 24.5,
                "direction": "OVER",
                "edge": 2.0,
                "sample_size": 20,
            },
            {
                "player": "Aliyah Boston",
                "prop_type": "Points",
                "line": 11.5,
                "direction": "OVER",
                "edge": 2.0,
                "sample_size": 20,
            },
            {
                "player": "Caitlin Clark",
                "prop_type": "Assists",
                "line": 6.5,
                "direction": "OVER",
                "edge": 2.0,
                "sample_size": 20,
            },
        ],
    }
    print(f"diversity _has_same_player_multistat: {_has_same_player_multistat(fake['legs'])}")
    cfg = json.loads((ROOT / "config" / "diversity_config.json").read_text(encoding="utf-8"))
    kept = apply_diversity_filter([fake], cfg)
    print(f"diversity filter kept: {len(kept)} (expect 0)")
    assert len(kept) == 0
    print("OK: Mitchell multi-stat blocked at G70 pack + mixer diversity")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
