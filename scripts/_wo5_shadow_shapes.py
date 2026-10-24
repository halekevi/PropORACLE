"""Relabel the Oct 8 shadow log as payout-only and count N-correct quotes by shape."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "data" / "reports" / "shadow_slips_2026-10-08.json"
CAP = ROOT / "data" / "reports" / "shadow_slips_2026-10-08_capture.json"


def goblin(leg: dict) -> bool:
    return "goblin" in str(leg.get("pick_type") or "").lower()


def main() -> None:
    payload = json.loads(LOG.read_text(encoding="utf-8"))
    shapes = Counter()
    quoted = []
    missing = 0
    for slip in payload.get("tickets") or []:
        legs = slip.get("legs") or []
        n = int(slip.get("n_legs") or len(legs))
        n_g = sum(1 for leg in legs if goblin(leg))
        live = slip.get("live_n_correct_x")
        if live in (None, "", 0):
            missing += 1
            slip["quote_status"] = "legs_not_on_board"
            continue
        shapes[(n, n_g, n - n_g, float(live))] += 1
        quoted.append(
            {
                "ticket_id": slip.get("ticket_id"),
                "n_legs": n,
                "n_goblin": n_g,
                "n_standard": n - n_g,
                "n_correct_x": float(live),
            }
        )
        slip["quote_status"] = "live_n_correct"
        slip["accounting"] = "payout_only"
    payload["accounting"] = "payout_only"
    payload["roi_sample"] = False
    payload["submit"] = False
    payload["quoted_n"] = len(quoted)
    payload["unquoted_n"] = missing
    payload["quotes_by_shape"] = [
        {"n_legs": k[0], "n_goblin": k[1], "n_standard": k[2], "n_correct_x": k[3], "slips": v}
        for k, v in sorted(shapes.items())
    ]
    LOG.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if CAP.is_file():
        cap = json.loads(CAP.read_text(encoding="utf-8"))
        cap["accounting"] = "payout_only"
        cap["roi_sample"] = False
        CAP.write_text(json.dumps(cap, indent=2), encoding="utf-8")
    print(json.dumps({"quoted": len(quoted), "missing": missing, "shapes": payload["quotes_by_shape"]}, indent=2))


if __name__ == "__main__":
    main()
