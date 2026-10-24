"""Name the NFL Standard legs that reached the ticket stage, then test current-season L5."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import prop_hit_tiers as T  # noqa: E402
import utils.ticket_70_pool as pool  # noqa: E402
from build_goblin70_tickets import load_today_board  # noqa: E402
from utils.nfl_keep_gates import nfl_current_season_l5_ok, nfl_standard_ticket_eligible  # noqa: E402
from utils.postseason_slate import season_blocks_ticket  # noqa: E402
from utils.ticket_70_pool import standard_ticket_eligible, ticket_l5_at_least_5  # noqa: E402
from utils.ticket_keep_registry import rule_for  # noqa: E402

DATE = "2026-10-08"
OUT = ROOT / "data" / "reports" / "wo4_nfl_ticket_legs.json"


def directional_l5(row: dict):
    side = str(row.get("side") or row.get("direction") or "").upper()
    raw = row.get("l5_under") if side == "UNDER" else row.get("l5_over")
    if raw in (None, ""):
        raw = row.get("l5")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def main() -> None:
    rows = load_today_board(DATE)
    real = pool.nfl_current_season_l5_ok
    pool.nfl_current_season_l5_ok = lambda _r: True
    reached = []
    try:
        for row in rows:
            if T.norm_sport(str(row.get("sport") or "")) != "NFL":
                continue
            if "goblin" in str(row.get("pick_type") or "").lower():
                continue
            if not row.get("start_time") and not row.get("game_date") and not row.get("slate_date"):
                row["slate_date"] = DATE
            if season_blocks_ticket(row):
                continue
            if directional_l5(row) != 5:
                continue
            rule = rule_for("NFL")
            if rule is None:
                continue
            _mode, fn = rule
            if fn is not None and not fn(row):
                continue
            if not ticket_l5_at_least_5(row):
                continue
            if not nfl_standard_ticket_eligible(row):
                continue
            if not standard_ticket_eligible(row):
                continue
            reached.append(row)
    finally:
        pool.nfl_current_season_l5_ok = real

    named = []
    for row in reached:
        ok = nfl_current_season_l5_ok(row)
        named.append(
            {
                "player": row.get("player"),
                "prop": row.get("prop"),
                "side": row.get("side") or row.get("direction"),
                "line": row.get("line"),
                "pick_type": row.get("pick_type"),
                "l5_sample_n": row.get("l5_sample_n"),
                "l5_window_n": row.get("l5_window_n"),
                "season_l5_flag": row.get("season_l5_flag"),
                "nfl_current_season_l5_ok": ok,
                "ticket_after_fix": standard_ticket_eligible(row),
            }
        )
    payload = {
        "date": DATE,
        "reached_before_current_season_check": len(named),
        "pass_current_season": sum(1 for r in named if r["nfl_current_season_l5_ok"]),
        "legs": named,
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
