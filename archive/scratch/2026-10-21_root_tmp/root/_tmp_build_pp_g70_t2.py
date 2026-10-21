#!/usr/bin/env python3
"""Ticket 2 after Kirby/Sale/Malonga submit. No submit."""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.manual_slip_exposure import assert_clears_exposure

CDP = "http://127.0.0.1:9222"

LEGS = [
    {"sport": "WNBA", "player": "Paige Bueckers", "search": "Bueckers", "prop_type": "Pts+Asts", "direction": "OVER", "line": 19.5, "pick_type": "goblin"},
    {"sport": "MLB", "player": "Logan Henderson", "search": "Logan Henderson", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 3.5, "pick_type": "goblin"},
    {"sport": "MLB", "player": "Merrill Kelly", "search": "Merrill Kelly", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
]


def react_search(page, q: str) -> None:
    page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return;
      const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
    }""",
        q,
    )


def main() -> int:
    assert_clears_exposure(LEGS, slate_date="2026-09-23", explicit_anchor_player="Paige Bueckers")
    print("exposure OK")
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=60_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if page is None:
            print("ERROR: no PrizePicks board tab")
            return 1
        print("TAB", page.url[:80])
        page.bring_to_front()
        P.navigate_board_for_sport(page, LEGS[0]["sport"], settle_ms=2200)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        page.wait_for_timeout(400)

        clicked = 0
        fails = []
        for leg in LEGS:
            print(f"\n=== {leg['sport']} {leg['player']} {leg['prop_type']} {leg['line']} ===")
            switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1500)
            if switched is not None:
                frame = switched
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            react_search(page, "")
            page.wait_for_timeout(200)
            react_search(page, leg["search"])
            page.wait_for_timeout(1600)
            frame = P.find_prizepicks_frame(page)
            ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
            print("add_leg", ok)
            if ok:
                clicked += 1
            else:
                fails.append(leg["player"])
            try:
                P.set_ticket_type(P.find_prizepicks_frame(page), "power")
            except Exception:
                pass
            time.sleep(0.5)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
        except Exception:
            pass
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
        n = int(slip.get("n_selected") or 0)
        m = re.search(r"Power Play.*?3 correct pays\s*(\d+(?:\.\d+)?)\s*X", raw, flags=re.I | re.S)
        px = float(m.group(1)) if m else None
        shot = _REPO / "data" / "reports" / "pp_g70_t2_bueckers_henderson_kelly.png"
        page.screenshot(path=str(shot), full_page=False)
        print("\n[FINAL] clicked", clicked, "n_selected", n, "fails", fails, "3-correct", px)
        print(raw[:1200])
        print("SHOT", shot)
        if n >= 3:
            print("READY — Power 3 on the board. Confirm 3-correct. Do not use 1st place. I did not submit.")
            return 0
        print("INCOMPLETE")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
