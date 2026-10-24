"""Pre-registered shadow: NHL Goblin shots on goal, line 0.5.

The in-sample cell is 298/390 (76.4%). That search had no holdout rows, so
76.4% is an upper estimate. A 3-leg at the 2.2x median is about -2% EV at
that point. A 2-leg at the 1.4x median needs about 84.5% per leg.

No ticket. Log every live-board leg. Wire the cell only after 100 forward
legs if the Wilson low clears break-even at the median quote for that slip.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = _ROOT / "data" / "reports" / "nhl_sog_line05_shadow.jsonl"

# 2.2 ** (-1/3) and 1.4 ** -0.5
BREAK_EVEN_3LEG_2_2X = 0.769
BREAK_EVEN_2LEG_1_4X = 0.845
FORWARD_N = 100

RULE = {
    "cell": "NHL Goblin OVER shots_on_goal line 0.5",
    "ticket": False,
    "in_sample": "298/390",
    "in_sample_hr": 0.764,
    "in_sample_is": "upper_estimate",
    "forward_n": FORWARD_N,
    "wire_when": (
        "Wilson low of the forward legs clears break-even at the median quote: "
        "0.769 for a 3-leg at 2.2x, 0.845 for a 2-leg at 1.4x"
    ),
}


def is_nhl_goblin_sog_line05(row: dict[str, Any]) -> bool:
    sport = str(row.get("sport") or "").strip().upper()
    if sport != "NHL":
        return False
    pick = str(row.get("pick_type") or row.get("pick") or "").lower()
    if "goblin" not in pick:
        return False
    side = str(row.get("direction") or row.get("dir") or row.get("over_under") or "").upper()
    if side not in {"OVER", "O", "MORE"}:
        return False
    prop = str(row.get("prop") or row.get("prop_type") or "").lower().replace(" ", "")
    if prop not in {"sog", "shotsongoal", "shotson goal", "shotsontarget"} and "shot" not in prop:
        return False
    if "block" in prop:
        return False
    try:
        line = float(row.get("line"))
    except (TypeError, ValueError):
        return False
    return abs(line - 0.5) < 1e-9


def log_live_legs(rows: list[dict[str, Any]], *, slate_date: str) -> int:
    """Append live-board legs. A repeat player-date-line is skipped."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    seen: set[tuple[str, str, str]] = set()
    if LOG_PATH.is_file():
        for line in LOG_PATH.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("kind") != "leg":
                continue
            seen.add(
                (
                    str(rec.get("slate_date") or ""),
                    str(rec.get("player") or "").casefold(),
                    str(rec.get("line") or ""),
                )
            )
    wrote = 0
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        if LOG_PATH.stat().st_size == 0:
            fh.write(json.dumps({"kind": "rule", **RULE}) + "\n")
        for row in rows:
            if not is_nhl_goblin_sog_line05(row):
                continue
            player = str(row.get("player") or "")
            key = (slate_date, player.casefold(), "0.5")
            if key in seen:
                continue
            seen.add(key)
            fh.write(
                json.dumps(
                    {
                        "kind": "leg",
                        "slate_date": slate_date,
                        "player": player,
                        "prop": "sog",
                        "line": 0.5,
                        "pick_type": "Goblin",
                        "direction": "OVER",
                        "ticket": False,
                    }
                )
                + "\n"
            )
            wrote += 1
    return wrote
