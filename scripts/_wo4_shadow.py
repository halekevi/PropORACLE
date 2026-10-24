"""Shadow slips: build, quote, log. Never submit and never publish."""
from __future__ import annotations

import json
import sys
from itertools import combinations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import prop_hit_tiers as T  # noqa: E402
from build_goblin70_tickets import load_today_board  # noqa: E402
from utils.n_correct_payout import resolve_n_correct  # noqa: E402
from utils.postseason_slate import season_blocks_ticket  # noqa: E402
from utils.ticket_70_pool import P_TENNIS_MATCH_TOTAL_GOBLIN, goblin_ticket_p  # noqa: E402

DATE = "2026-10-08"
OUT = ROOT / "data" / "reports" / "shadow_slips_2026-10-08.json"
CAPTURE = ROOT / "data" / "reports" / "shadow_slips_2026-10-08_tickets.json"


def directional_l5(row: dict):
    side = str(row.get("side") or row.get("direction") or "").upper()
    raw = row.get("l5_under") if side == "UNDER" else row.get("l5_over")
    if raw in (None, ""):
        raw = row.get("l5")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def leg_view(row: dict) -> dict:
    return {
        "player": str(row.get("player") or "").strip(),
        "prop_type": str(row.get("prop") or "").strip(),
        "direction": str(row.get("side") or row.get("direction") or "OVER").strip().upper(),
        "line": row.get("line"),
        "pick_type": str(row.get("pick_type") or "Standard").strip(),
        "sport": T.norm_sport(str(row.get("sport") or "")),
        "p": goblin_ticket_p(row),
    }


def main() -> None:
    ranked = []
    seen = set()
    for row in load_today_board(DATE):
        sport = T.norm_sport(str(row.get("sport") or ""))
        if sport not in {"Tennis", "Soccer", "NFL", "CFB", "Golf"}:
            continue
        if not row.get("start_time") and not row.get("game_date") and not row.get("slate_date"):
            row["slate_date"] = DATE
        if season_blocks_ticket(row):
            continue
        if directional_l5(row) != 5:
            continue
        if "demon" in str(row.get("pick_type") or "").lower():
            continue
        player = str(row.get("player") or "").strip()
        if not player or player in seen or " + " in player:
            continue
        seen.add(player)
        prop = T.canon_prop(sport, str(row.get("prop") or ""))
        score = 0
        if sport == "Tennis" and prop == "match_total_games" and "goblin" in str(row.get("pick_type") or "").lower():
            score = 100
        ranked.append((score, leg_view(row)))
    ranked.sort(key=lambda item: -item[0])
    pool = [leg for _score, leg in ranked[:12]]
    slips = []
    for pair in combinations(pool, 2):
        if len(slips) >= 30:
            break
        legs = list(pair)
        quote = resolve_n_correct(legs, "Power", "goblin70", date=DATE, repo=ROOT)
        table = quote.get("n_correct") or {}
        try:
            pay = float(table.get(2) or table.get("2") or 0)
        except (TypeError, ValueError):
            pay = 0.0
        p = 1.0
        for leg in legs:
            p *= float(leg["p"] or 0)
        ev = (p * pay - 1.0) if pay else None
        slips.append(
            {
                "ticket_id": f"shadow|{DATE}|{len(slips) + 1}",
                "submit": False,
                "product": "Power",
                "n_legs": 2,
                "legs": legs,
                "quote_source": quote.get("payout_source"),
                "n_correct": table,
                "all_hit_x": pay or None,
                "ev_at_quote": None if ev is None else round(ev, 4),
            }
        )
    tiafoe = next((r for r in pool if "tiafoe" in r["player"].lower()), None)
    rublev = next((r for r in pool if "rublev" in r["player"].lower()), None)
    pair_ev = None
    if tiafoe and rublev:
        p = P_TENNIS_MATCH_TOTAL_GOBLIN
        pair_ev = round(p * p * 1.40 - 1.0, 4)
    payload = {
        "date": DATE,
        "submit": False,
        "pool_n": len(pool),
        "slips": len(slips),
        "tennis_total_games_pair_ev_at_1_40x": pair_ev,
        "tennis_price": P_TENNIS_MATCH_TOTAL_GOBLIN,
        "two_leg_break_even": round(1.40 ** -0.5, 4),
        "legs": pool,
        "tickets": slips,
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    CAPTURE.write_text(
        json.dumps(
            {
                "date": DATE,
                "groups": [
                    {
                        "group_name": "shadow",
                        "tickets": [
                            {
                                "ticket_id": s["ticket_id"],
                                "play": "Power",
                                "strong_builder": False,
                                "legs": s["legs"],
                            }
                            for s in slips
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    print(json.dumps({k: payload[k] for k in payload if k not in {"tickets", "legs"}}, indent=2))


if __name__ == "__main__":
    main()
