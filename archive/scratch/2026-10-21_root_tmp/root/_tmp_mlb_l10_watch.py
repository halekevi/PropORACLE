#!/usr/bin/env python3
"""Stamp today's MLB L10-primary Goblin legs for separate grade tracking.

Pitcher Ks / Walks Allowed / Hits Allowed switched to L10-primary keep gates.
Track these named legs vs the rest of the card after games settle.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from build_goblin70_tickets import load_today_board  # noqa: E402
from utils.mlb_keep_gates import mlb_goblin_keep_eligible  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, goblin_sort_key  # noqa: E402

DATE = "2026-09-18"
# First live confirmation set called out on the morning card.
WATCH = {
    ("tyler mahle", "pitcher strikeouts"),
    ("gerrit cole", "pitcher strikeouts"),
    ("bryan woo", "pitcher strikeouts"),
    ("cade cavalli", "walks allowed"),
    ("kyle leahy", "hits allowed"),
    ("grayson rodriguez", "hits allowed"),
}
L10_PRIMARY_PROPS = {"pitcher strikeouts", "walks allowed", "hits allowed"}


def _norm(s: object) -> str:
    return " ".join(str(s or "").strip().lower().replace("_", " ").split())


def main() -> int:
    board = [r for r in load_today_board(DATE) if str(r.get("sport") or "").upper() == "MLB"]
    gob = [r for r in board if goblin_70_eligible(r)]
    l10_primary = []
    named = []
    for r in sorted(gob, key=goblin_sort_key):
        prop = _norm(r.get("prop") or r.get("prop_type"))
        player = _norm(r.get("player"))
        if prop not in L10_PRIMARY_PROPS:
            continue
        if not mlb_goblin_keep_eligible(r):
            continue
        row = {
            "player": r.get("player"),
            "prop": r.get("prop"),
            "line": r.get("line"),
            "side": r.get("side") or "OVER",
            "pick_type": r.get("pick_type") or "Goblin",
            "l5_over": r.get("l5_over"),
            "l10_over": r.get("l10_over"),
            "cover": r.get("cover"),
            "def": r.get("def"),
            "matchup": r.get("matchup"),
            "team": r.get("team"),
            "badge": r.get("promo") or r.get("badge"),
            "prop_tier": r.get("prop_tier"),
            "gate": "mlb_l10_primary_keep",
            "named_watch": (player, prop) in WATCH,
        }
        l10_primary.append(row)
        if row["named_watch"]:
            named.append(row)

    out = {
        "date": DATE,
        "stamped_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "purpose": (
            "First live OOS check of MLB Goblin L10-primary keep gates "
            "(pitcher_ks / walks_allowed / hits_allowed). Grade these legs "
            "separately from the rest of the card."
        ),
        "named_watch_n": len(named),
        "l10_primary_pool_n": len(l10_primary),
        "named_watch": named,
        "l10_primary_pool": l10_primary,
    }
    dest_dir = ROOT / "data" / "reports"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"mlb_l10_primary_live_watch_{DATE}.json"
    dest.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    latest = dest_dir / "mlb_l10_primary_live_watch_latest.json"
    latest.write_text(dest.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"named_watch={len(named)} pool={len(l10_primary)}")
    for r in named:
        print(
            f"  WATCH  {r['player']:22} {r['prop']:20} O{r['line']} "
            f"L5={r['l5_over']} L10={r['l10_over']} {r.get('matchup')}"
        )
    print(f"wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
