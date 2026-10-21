#!/usr/bin/env python3
"""Build Power 3 #1 on the warmed PrizePicks CDP board. Does NOT submit."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

# Power 3 #1 — confirmed card
LEGS = [
    {
        "sport": "MLB",
        "player": "Cam Schlittler",
        "prop_type": "Pitcher Strikeouts",
        "direction": "OVER",
        "line": 3.5,
        "pick_type": "goblin",
    },
    {
        "sport": "WNBA",
        "player": "Arike Ogunbowale",
        "prop_type": "Rebs+Asts",
        "direction": "OVER",
        "line": 4.5,
        "pick_type": "goblin",
    },
    {
        "sport": "CFB",
        "player": "Devon Dampier",
        "prop_type": "Pass Yards",
        "direction": "OVER",
        "line": 159.5,
        "pick_type": "goblin",
    },
]


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=30_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                url = (pg.url or "").lower()
                if "prizepicks.com" in url and "board" in url:
                    page = pg
                    break
            if page:
                break
        if page is None:
            page = browser.contexts[0].pages[0] if browser.contexts and browser.contexts[0].pages else None
        if page is None:
            print("ERROR: no PrizePicks board tab on CDP")
            return 1

        print(f"[BUILD] using tab: {page.url[:80]}")
        page.bring_to_front()

        # Start on first leg's sport
        first = LEGS[0]["sport"]
        P.navigate_board_for_sport(page, first, settle_ms=2500)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        frame.wait_for_timeout(800)

        active = first
        clicked = 0
        for leg in LEGS:
            sp = leg["sport"]
            if sp != active:
                switched = P.switch_sport_tab(frame, page, sp)
                if switched is not None:
                    frame = switched
                    active = sp
                else:
                    P.navigate_board_for_sport(page, sp, settle_ms=2000)
                    frame = P.find_prizepicks_frame(page)
                    active = sp
            P.dismiss_modal(frame, page)
            ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
            print(f"  [{'OK' if ok else 'FAIL'}] {sp} {leg['player']} {leg['prop_type']} {leg['direction']} {leg['line']} Goblin")
            if ok:
                clicked += 1
            frame.wait_for_timeout(1200)

        slip = P.read_slip(frame, n_legs=clicked, ticket_type="power")
        print()
        print(f"[BUILD] clicked={clicked}/3")
        if slip:
            print(f"[BUILD] slip n_selected={slip.get('n_selected')}")
            raw = (slip.get("raw_slip_section") or slip.get("raw_text") or "")[:800]
            print(f"[BUILD] slip text:\n{raw}")
        else:
            print("[BUILD] could not read slip panel — check the Chrome window")

        if clicked == 3:
            print()
            print("READY — Power Play slip built in your CDP Chrome. Hit Submit yourself.")
            return 0
        print()
        print("INCOMPLETE — open the PrizePicks Chrome tab and finish missing legs manually.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
