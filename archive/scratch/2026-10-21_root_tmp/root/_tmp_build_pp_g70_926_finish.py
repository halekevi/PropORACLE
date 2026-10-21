#!/usr/bin/env python3
"""Finish remaining live G70 Power 3 tabs for 2026-09-26. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, order_legs_tennis_first, react_search

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = _REPO / "data" / "reports" / "pp_g70_finish_2026-09-26.json"

# Remaining live faces after P3-3 (Wesneski/Quintana/Lowder already READY)
TICKETS = [
    {
        "id": "P3-1",
        "mode": "power",
        "legs": [
            {"sport": "MLB", "player": "Bailey Ober", "search": "Bailey Ober", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Davis Martin", "search": "Davis Martin", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Griffin Jax", "search": "Griffin Jax", "prop_type": "Hits Allowed", "direction": "OVER", "line": 1.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "P3-2",
        "mode": "power",
        "legs": [
            {"sport": "MLB", "player": "Tanner Bibee", "search": "Tanner Bibee", "prop_type": "Earned Runs Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "SOCCER", "player": "Luka Sucic", "search": "Luka Sucic", "prop_type": "Shots", "direction": "OVER", "line": 1.5, "pick_type": "goblin"},
            {"sport": "SOCCER", "player": "Lukás Hornícek", "search": "Hornicek", "prop_type": "Goalie Saves", "direction": "OVER", "line": 1.5, "pick_type": "goblin"},
        ],
    },
]


def build_one(page, ticket: dict) -> dict:
    tid, mode = ticket["id"], ticket["mode"]
    legs = order_legs_tennis_first(list(ticket["legs"]))
    print(f"\n======== {tid} ========", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    # Always goto first sport — reliable after multi-tab
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    added = []
    for i, leg in enumerate(legs):
        print(f"=== {leg['sport']} {leg['player']} {leg['prop_type']} {leg['line']} ===", flush=True)
        if i == 0:
            frame = P.find_prizepicks_frame(page)
        else:
            # Prefer tab click; if soccer miss, goto (clears slip — so only if empty fail)
            switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1600)
            if switched is None:
                print(f"tab miss {leg['sport']} — goto", flush=True)
                P.navigate_board_for_sport(page, leg["sport"], settle_ms=2000)
                # re-add prior legs after goto wipe
                frame = P.find_prizepicks_frame(page)
                P.clear_slip(frame)
                P.set_ticket_type(frame, mode)
                for prev in legs[:i]:
                    react_search(page, "")
                    page.wait_for_timeout(100)
                    P.navigate_board_for_sport(page, prev["sport"], settle_ms=1400)
                    chip = P.prop_type_to_board_filter(prev["prop_type"])
                    P._switch_board_filter(P.find_prizepicks_frame(page), page, chip)
                    page.wait_for_timeout(500)
                    react_search(page, prev["search"])
                    page.wait_for_timeout(1100)
                    rr = add_cycled_leg(P, P.find_prizepicks_frame(page), page, prev)
                    if not rr.ok:
                        print(rr.loud_fail(), flush=True)
                        return {"id": tid, "ready": False, "fails": [prev["player"]], "n": 0}
                P.navigate_board_for_sport(page, leg["sport"], settle_ms=1600)
            frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(700)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, leg["search"])
        page.wait_for_timeout(1300)
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": len(added)}
        added.append(leg["player"])
        time.sleep(0.2)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception:
        pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, 3)
    print(f"[{tid}] ready={n==3} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    return {"id": tid, "ready": n == 3, "n": n, "px": px, "fails": [], "raw": raw[:400]}


def main() -> int:
    results = []
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [
            pg for pg in ctx.pages
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
        ]
        while len(boards) < 2:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1000)
            boards.append(pg)
        # Use last two boards (leave P3-3 alone if still up)
        for page, ticket in zip(boards[-2:], TICKETS):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append({"id": ticket["id"], "ready": False, "fails": [str(e)], "n": 0})
    OUT.write_text(json.dumps({"results": results}, indent=2), encoding="utf-8")
    print("\nSUMMARY", flush=True)
    for r in results:
        print(f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} N-correct={r.get('px')} fails={r.get('fails')}", flush=True)
    # P3-3 already ready from prior run
    print("P3-3 already READY (MLB Wesneski / Quintana / Lowder). No submit.", flush=True)
    return 0 if all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
