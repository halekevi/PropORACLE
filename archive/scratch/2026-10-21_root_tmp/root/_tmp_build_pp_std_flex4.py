#!/usr/bin/env python3
"""Build Standard Flex 4. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

LEGS = [
    {
        "sport": "CFB",
        "player": "George Pettaway",
        "search": "Pettaway",
        "prop_type": "Rush Yards",
        "direction": "UNDER",
        "line": 70.5,
        "pick_type": "standard",
    },
    {
        "sport": "CFB",
        "player": "Carson Bruhn",
        "search": "Bruhn",
        "prop_type": "Receiving Yards",
        "direction": "OVER",
        "line": 10.5,
        "pick_type": "standard",
    },
    {
        "sport": "CFB",
        "player": "Devon Dampier",
        "search": "Dampier",
        "prop_type": "Rush Yards",
        "direction": "OVER",
        "line": 37.5,
        "pick_type": "standard",
    },
    {
        "sport": "WNBA",
        "player": "Veronica Burton",
        "search": "Burton",
        "prop_type": "Pts+Rebs",
        "direction": "UNDER",
        "line": 18.5,
        "pick_type": "standard",
    },
]


def react_search(page, q: str) -> None:
    page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return;
      const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
      inp.dispatchEvent(new Event('change', {bubbles:true}));
    }""",
        q,
    )


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=30_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if page is None:
            print("ERROR: no board")
            return 1
        page.bring_to_front()

        P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "flex")
        page.wait_for_timeout(500)

        clicked = 0
        for leg in LEGS:
            print(f"\n=== {leg['sport']} {leg['player']} {leg['direction']} {leg['line']} ===")
            switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1500)
            if switched is not None:
                frame = switched
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            react_search(page, leg["search"])
            page.wait_for_timeout(1600)
            ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
            print("  add_leg", "OK" if ok else "FAIL")
            if not ok:
                # softer: cycle faces
                ok = P.add_leg(frame, page, leg, strict_lines=False, require_line=True)
                print("  retry", "OK" if ok else "FAIL")
            if ok:
                clicked += 1
            try:
                P.set_ticket_type(frame, "flex")
            except Exception:
                pass
            page.wait_for_timeout(600)

        try:
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2200])
        n = int((slip or {}).get("n_selected") or 0)
        names = ("Pettaway", "Bruhn", "Dampier", "Burton")
        ok_names = all(x in raw for x in names)
        if n >= 4 and ok_names:
            print("\nREADY — Standard Flex 4 built. Hit Submit in CDP Chrome.")
            return 0
        print("\nINCOMPLETE", n, ok_names, "clicked", clicked)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
