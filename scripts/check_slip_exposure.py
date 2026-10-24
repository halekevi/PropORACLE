#!/usr/bin/env python3
"""Check a proposed slip against the slate exposure ledger before CDP submit.

Manual / session builds must use this CLI **or**
``utils.pp_submit_guard.submit_lineup_guarded`` (hard gate before Submit):

  py -3.14 scripts/check_slip_exposure.py --date 2026-09-19 \\
      --leg "Devon Dampier|Pass Yards|OVER|Goblin" \\
      --leg "Cam Schlittler|Pitching Outs|OVER|Goblin" \\
      --anchor "Devon Dampier"

Exit 0 = ok to submit. Exit 2 = blocked.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.manual_slip_exposure import (  # noqa: E402
    check_slip_exposure,
    register_after_submit,
)


def _parse_leg(raw: str) -> dict:
    parts = [p.strip() for p in str(raw).split("|")]
    while len(parts) < 4:
        parts.append("")
    player, prop, direction, pick = parts[:4]
    return {
        "player": player,
        "prop_type": prop,
        "direction": direction or "OVER",
        "pick_type": pick or "Goblin",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD")
    ap.add_argument(
        "--leg",
        action="append",
        default=[],
        help="player|prop|direction|pick_type (repeatable)",
    )
    ap.add_argument(
        "--anchor",
        default="",
        help="Explicit anchor player (manual builds should set this)",
    )
    ap.add_argument(
        "--register",
        action="store_true",
        help="Append this slip to the same-day session register after OK "
        "(so the next manual slip sees it — use after a real Submit)",
    )
    ap.add_argument(
        "--json-slip",
        default="",
        help="Path to a JSON list of leg dicts instead of --leg",
    )
    args = ap.parse_args()

    legs: list[dict] = []
    if args.json_slip:
        raw = json.loads(Path(args.json_slip).read_text(encoding="utf-8"))
        legs = list(raw) if isinstance(raw, list) else list(raw.get("legs") or [])
    for item in args.leg:
        legs.append(_parse_leg(item))
    if not legs:
        print("ERROR: provide --leg and/or --json-slip", file=sys.stderr)
        return 1

    date = str(args.date).strip()[:10]
    anchor = str(args.anchor or "").strip() or None
    ok, reason = check_slip_exposure(
        legs, slate_date=date, explicit_anchor_player=anchor
    )
    if not ok:
        print(f"BLOCKED: {reason}")
        print("Do not submit this slip.")
        return 2

    print("OK: slip clears slate exposure ledger")
    if args.register:
        path = register_after_submit(
            legs, slate_date=date, explicit_anchor_player=anchor
        )
        print(f"Registered session slip -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
