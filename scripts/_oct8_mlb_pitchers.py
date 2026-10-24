"""Which Oct 8 MLB pitcher Goblins the builder would price, and why the pool was empty."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import build_goblin70_tickets as g70  # noqa: E402
from utils.mlb_keep_gates import mlb_goblin_keep_eligible  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

FOCUS = {"pitcher_ks", "hits_allowed", "pitching_outs", "earned_runs", "walks_allowed", "pitches_thrown"}


def main() -> None:
    board = g70.load_today_board("2026-10-08", skip_golf=True)
    print("board rows", len(board), flush=True)
    n_mlb = 0
    for r in board:
        if str(r.get("sport") or "").upper() != "MLB":
            continue
        if "goblin" not in str(r.get("pick_type") or "").lower():
            continue
        prop = canon_prop("MLB", str(r.get("prop") or ""))
        if prop not in FOCUS:
            continue
        n_mlb += 1
        print(
            f"{r.get('player')} {r.get('prop')} line={r.get('line')} "
            f"l10={r.get('l10_over')} l5={r.get('l5_over')} "
            f"keep={mlb_goblin_keep_eligible(r)} g70={goblin_70_eligible(r)}",
            flush=True,
        )
    print("pitcher goblins", n_mlb, flush=True)


if __name__ == "__main__":
    main()
