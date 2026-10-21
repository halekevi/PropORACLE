#!/usr/bin/env python3
"""Retry remaining live G70 Power 3s on PrizePicks. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = _REPO / "data" / "reports" / "pp_g70_livepack_retry_2026-09-27.json"

TICKETS = [
    {
        "id": "P3-L3b",
        "legs": [
            {"sport": "MLB", "player": "Andre Pallante", "prop_type": "Hits Allowed", "prop": "Hits Allowed", "line": 2.5, "pick_type": "goblin", "direction": "OVER", "side": "OVER", "search": "Andre Pallante"},
            {"player": "Sean Manaea", "sport": "MLB", "prop_type": "Hits Allowed", "prop": "Hits Allowed", "line": 3.5, "pick_type": "goblin", "direction": "OVER", "side": "OVER", "search": "Sean Manaea"},
            {"player": "Parker Messick", "sport": "MLB", "prop_type": "Pitcher Strikeouts", "prop": "Pitcher Strikeouts", "line": 2.5, "pick_type": "goblin", "direction": "OVER", "side": "OVER", "search": "Parker Messick"},
        ],
    },
    {
        "id": "P3-L4b",
        "legs": [
            {"sport": "MLB", "player": "Jacob Misiorowski", "prop_type": "Pitcher Strikeouts", "prop": "Pitcher Strikeouts", "line": 4.5, "pick_type": "goblin", "direction": "OVER", "side": "OVER", "search": "Jacob Misiorowski"},
            {"sport": "MLB", "player": "Max Scherzer", "prop_type": "Pitching Outs", "prop": "Pitching Outs", "line": 8.5, "pick_type": "goblin", "direction": "OVER", "side": "OVER", "search": "Max Scherzer"},
            {"sport": "SOCCER", "player": "Konstantinos Mavropanos", "prop_type": "Shots", "prop": "Shots", "line": 0.5, "pick_type": "goblin", "direction": "OVER", "side": "OVER", "search": "Konstantinos Mavropanos"},
        ],
    },
]


def build_one(page, ticket):
    tid, legs = ticket["id"], ticket["legs"]
    print(f"\n======== {tid} ========", flush=True)
    for x in legs:
        print(f"  {x['sport']} {x['player']} {x['prop']} {x['line']}", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=1600)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    for i, leg in enumerate(legs):
        frame = P.find_prizepicks_frame(page)
        if i > 0:
            landed = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1400)
            if landed is None:
                P.navigate_board_for_sport(page, leg["sport"], settle_ms=1600)
            else:
                frame = landed
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(600)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, leg["player"])
        page.wait_for_timeout(1200)
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [leg["player"]], "n": 0}
        time.sleep(0.2)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception:
        pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else 3)
    print(f"[{tid}] slip_ok={n==3} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    return {
        "id": tid,
        "ok": n == 3,
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["sport"], x["player"], x["prop"], x["line"]) for x in legs],
    }


def main():
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = next(
            (pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")),
            None,
        )
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(900)
        results = []
        for t in TICKETS:
            try:
                results.append(build_one(page, t))
            except Exception as e:
                print(f"{t['id']} EXC {e}", flush=True)
                results.append({"id": t["id"], "ok": False, "fails": [str(e)], "n": 0})
        OUT.write_text(json.dumps({"results": results}, indent=2, default=str), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        for r in results:
            print(f"  {r.get('id')} ok={r.get('ok')} n={r.get('n')} N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}", flush=True)
        print(f"wrote {OUT}", flush=True)
        return 0 if results and all(r.get("ok") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
