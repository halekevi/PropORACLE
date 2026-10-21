#!/usr/bin/env python3
"""9/24 T2: Wilson RA 9.5 / Mahle PO 14.5 / Mertens TG 19.5. No submit.

Tennis first so navigate/goto cannot drop the other legs. Chip is Rebs+Asts
(not Reb+Asts). Schlittler/Glasnow G70 Ks lines are gone live.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.manual_slip_exposure import load_session_slips

CDP = "http://127.0.0.1:9222"
SLATE = "2026-09-24"

LEGS = [
    {
        "sport": "TENNIS",
        "player": "Elise Mertens",
        "search": "Mertens",
        "prop_type": "Total Games",
        "chip": "Total Games",
        "direction": "OVER",
        "line": 19.5,
        "pick_type": "goblin",
        "navigate": True,
    },
    {
        "sport": "WNBA",
        "player": "A'ja Wilson",
        "search": "Wilson",
        "prop_type": "Rebs+Asts",
        "chip": "Rebs+Asts",
        "direction": "OVER",
        "line": 9.5,
        "pick_type": "goblin",
    },
    {
        "sport": "MLB",
        "player": "Tyler Mahle",
        "search": "Tyler Mahle",
        "prop_type": "Pitching Outs",
        "chip": "Pitching Outs",
        "direction": "OVER",
        "line": 14.5,
        "pick_type": "goblin",
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
    }""",
        q,
    )


def main() -> int:
    used = set()
    for slip in load_session_slips(SLATE):
        for row in slip.get("legs") or []:
            used.add(str(row.get("player") or "").casefold())
    overlap = [leg["player"] for leg in LEGS if leg["player"].casefold() in used]
    if overlap:
        print("BLOCKED session overlap", overlap)
        return 4
    print("session exposure OK")
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
            print("ERROR: no board")
            return 1
        print("TAB", page.url[:80])
        page.bring_to_front()
        P.navigate_board_for_sport(page, "TENNIS", settle_ms=2200)
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
            frame = P.find_prizepicks_frame(page)
            if leg.get("navigate"):
                P.navigate_board_for_sport(page, leg["sport"], settle_ms=1800)
                frame = P.find_prizepicks_frame(page)
            else:
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
            P._switch_board_filter(frame, page, leg["chip"])
            page.wait_for_timeout(900)
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
        shot = _REPO / "data" / "reports" / "pp_g70_924_t2.png"
        page.screenshot(path=str(shot), full_page=False)
        print("\n[FINAL] clicked", clicked, "n", n, "fails", fails, "3-correct", px)
        print(raw[:1400])
        print("SHOT", shot)
        if n >= 3 and px is not None and px + 1e-9 < 1.4:
            print("UNDER 1.4x — do not submit")
            return 3
        if n >= 3:
            print("READY on the board. Confirm 3-correct >= 1.4x. I did not submit.")
            return 0
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
