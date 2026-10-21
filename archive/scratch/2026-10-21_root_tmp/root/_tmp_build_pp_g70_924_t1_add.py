#!/usr/bin/env python3
"""Add Leahy Hits Allowed 1.5 Goblin to Lambert/Young slip. No submit."""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
LEG = {
    "sport": "MLB",
    "player": "Kyle Leahy",
    "search": "Kyle Leahy",
    "prop_type": "Hits Allowed",
    "direction": "OVER",
    "line": 1.5,
    "pick_type": "goblin",
}


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
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)
        switched = P.switch_sport_tab(frame, page, "MLB", settle_ms=1500)
        if switched is not None:
            frame = switched
        P.dismiss_modal(frame, page)
        react_search(page, "Kyle Leahy")
        page.wait_for_timeout(1600)
        frame = P.find_prizepicks_frame(page)
        ok = P.add_leg(frame, page, LEG, strict_lines=True, require_line=True)
        print("add_leg", ok)
        try:
            P.set_ticket_type(P.find_prizepicks_frame(page), "power")
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
        except Exception:
            pass
        time.sleep(0.4)
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
        n = int(slip.get("n_selected") or 0)
        m = re.search(r"Power Play.*?3 correct pays\s*(\d+(?:\.\d+)?)\s*X", raw, flags=re.I | re.S)
        px = float(m.group(1)) if m else None
        shot = _REPO / "data" / "reports" / "pp_g70_924_t1b.png"
        page.screenshot(path=str(shot), full_page=False)
        print("[FINAL] n", n, "3-correct", px)
        print(raw[:1100])
        if n >= 3 and px is not None and px + 1e-9 < 1.4:
            print("UNDER 1.4x")
            return 3
        return 0 if n >= 3 else 2


if __name__ == "__main__":
    raise SystemExit(main())
