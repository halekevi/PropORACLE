"""Name NFL ticket-stage legs and locate CFB graded rows."""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402
from build_goblin70_tickets import load_today_board  # noqa: E402
from utils.nfl_keep_gates import nfl_current_season_l5_ok  # noqa: E402
from utils.postseason_slate import season_blocks_ticket  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    goblin_70_eligible,
    standard_ticket_eligible,
    ticket_gate_passes,
)
from utils.ticket_keep_registry import rule_for  # noqa: E402


def cfb_counts(root: Path, day: str) -> None:
    path = root / "ui_runner" / "templates" / f"graded_props_{day}.json"
    print("FILE", path, path.is_file(), path.stat().st_size if path.is_file() else 0)
    if not path.is_file():
        return
    raw = json.loads(path.read_text(encoding="utf-8"))
    sports = Counter()
    cfb = Counter()
    for row in raw.get("props") or []:
        if not isinstance(row, dict):
            continue
        sport = str(row.get("sport") or "")
        sports[sport] += 1
        if T.norm_sport(sport) == "CFB" or "cfb" in sport.lower() or "college" in sport.lower():
            res = str(row.get("result") or "").upper()
            ms = str(row.get("minutes_status") or "")
            cfb[(res, ms, T.norm_sport(sport))] += 1
    print(" sports", sports.most_common(8))
    print(" cfb-like", cfb.most_common(12))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main() -> None:
    for root in (MAIN, ROOT):
        cfb_counts(root, "2026-09-03")
    rows = load_today_board("2026-10-08")
    nfl = []
    ten = []
    for row in rows:
        sport = T.norm_sport(str(row.get("sport") or ""))
        if sport == "NFL" and standard_ticket_eligible(row) and not season_blocks_ticket(row):
            l5 = num(row.get("l5_under") if str(row.get("side")).upper() == "UNDER" else row.get("l5_over"))
            if l5 == 5 or True:
                # ticket stage in the waterfall also required L5=5 except skip props,
                # then keep, then ticket_gate. Mirror unlock: ticket_gate after L5=5
                # unless the waterfall counted ticket_gate without re-checking L5.
                pass
        if sport == "NFL" and "standard" in str(row.get("pick_type") or "").lower():
            if season_blocks_ticket(row):
                continue
            side = str(row.get("side") or "").upper()
            l5 = num(row.get("l5_under") if side == "UNDER" else row.get("l5_over"))
            rule = rule_for(sport)
            if l5 != 5:
                # waterfall required L5=5 before ticket. Skip-L5 props can ticket
                # without L5=5, so also keep those that pass the ticket gate.
                if not standard_ticket_eligible(row):
                    continue
            elif rule is None or not standard_ticket_eligible(row):
                continue
            if not standard_ticket_eligible(row):
                continue
            nfl.append(row)
        if sport == "Tennis" and goblin_70_eligible(row):
            ten.append(row)
    print("NFL_TICKET", len(nfl))
    for row in nfl:
        print(
            json.dumps(
                {
                    "player": row.get("player"),
                    "prop": row.get("prop"),
                    "side": row.get("side"),
                    "line": row.get("line"),
                    "l5_over": row.get("l5_over"),
                    "l5_under": row.get("l5_under"),
                    "l5_sample_n": row.get("l5_sample_n"),
                    "l5_window_n": row.get("l5_window_n"),
                    "season_l5_flag": row.get("season_l5_flag") or row.get("Season L5 Flag"),
                    "current_ok": nfl_current_season_l5_ok(row),
                    "gate": ticket_gate_passes(row),
                },
                default=str,
            )
        )
    print("TENNIS_TICKET", len(ten))
    for row in ten:
        print(
            json.dumps(
                {
                    "player": row.get("player"),
                    "prop": row.get("prop"),
                    "side": row.get("side"),
                    "line": row.get("line"),
                    "pick": row.get("pick_type"),
                    "l5": row.get("l5_over"),
                    "l10": row.get("l10_over"),
                    "p": row.get("p"),
                    "p_cell": row.get("p_cell"),
                },
                default=str,
            )
        )


if __name__ == "__main__":
    main()
