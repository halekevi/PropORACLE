#!/usr/bin/env python3
"""Build Sports/NFL/data/nfl_depth_chart_cache.json from ESPN depth charts."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from utils.nfl_espn_context import (
    expected_snaps_from_slot,
    fetch_nfl_team_list,
    fetch_team_depth_chart,
    save_json,
)

DEFAULT_OUT = _SCRIPT_DIR.parent / "data" / "nfl_depth_chart_cache.json"


def _norm_name(s: object) -> str:
    return " ".join(str(s or "").lower().split())


SKILL_POS = frozenset({"QB", "RB", "WR", "TE", "FB", "K", "PK"})


def _slot_priority(rec: dict) -> tuple[int, int]:
    pos = str(rec.get("position") or "").upper()
    try:
        rank = int(rec.get("depth_rank") or 99)
    except (TypeError, ValueError):
        rank = 99
    skill = 0 if pos in SKILL_POS else 1
    return (skill, rank)


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch ESPN NFL depth charts.")
    ap.add_argument("--output", default=str(DEFAULT_OUT))
    args = ap.parse_args()

    teams = fetch_nfl_team_list()
    if not teams:
        print("[NFL depth] ERROR: no ESPN teams", file=sys.stderr)
        return 1

    by_player: dict = {}
    n_rows = 0
    for i, t in enumerate(teams):
        rows = fetch_team_depth_chart(t["team_id"], t["team_abbr"])
        for rec in rows:
            n_rows += 1
            key = _norm_name(rec.get("player"))
            aid = str(rec.get("espn_athlete_id") or "").strip()
            payload = {
                "player": rec.get("player"),
                "team": rec.get("team"),
                "position": rec.get("position"),
                "depth_rank": rec.get("depth_rank"),
                "depth_slot": rec.get("depth_slot"),
                "expected_snaps": rec.get("expected_snaps") or expected_snaps_from_slot(str(rec.get("depth_slot") or "")),
                "espn_athlete_id": aid,
            }
            if key:
                old = by_player.get(key)
                if old is None or _slot_priority(payload) < _slot_priority(old):
                    by_player[key] = payload
            if aid:
                old = by_player.get(aid)
                if old is None or _slot_priority(payload) < _slot_priority(old):
                    by_player[aid] = payload
        print(f"  {t['team_abbr']}: {len(rows)} depth rows")
        if i + 1 < len(teams):
            time.sleep(0.12)

    out = Path(args.output)
    save_json(
        out,
        {
            "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "teams": len(teams),
            "players": len({k: v for k, v in by_player.items() if not str(k).isdigit()}),
            "by_player": by_player,
        },
    )
    print(f"[NFL depth] Wrote {out} rows={n_rows} keys={len(by_player)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
