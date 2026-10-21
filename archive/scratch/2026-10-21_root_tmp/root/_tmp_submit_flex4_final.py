#!/usr/bin/env python3
"""Submit Standard Flex 4 if Jones/Overton/Pendergrass/Hiedeman are on slip."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_submit_guard import ExposureBlocked, submit_lineup_guarded

CDP = "http://127.0.0.1:9222"
NEED = ("Jones", "Overton", "Pendergrass", "Hiedeman")
EXPOSURE_LEGS = [
    {"player": "Tavorus Jones", "prop_type": "Rush Yards", "direction": "UNDER", "pick_type": "Standard"},
    {"player": "Jeffrey Overton Jr.", "prop_type": "Rush Yards", "direction": "OVER", "pick_type": "Standard"},
    {"player": "Dawson Pendergrass", "prop_type": "Rec Yards", "direction": "OVER", "pick_type": "Standard"},
    {"player": "Natisha Hiedeman", "prop_type": "3PTM", "direction": "UNDER", "pick_type": "Standard"},
]


def submit_lineup(page, frame) -> str:
    """Hard-gated Submit — raises ExposureBlocked (do not swallow as soft miss)."""
    return submit_lineup_guarded(
        page,
        frame,
        EXPOSURE_LEGS,
        explicit_anchor_player="Tavorus Jones",
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
        frame = P.find_prizepicks_frame(page)
        try:
            P.set_ticket_type(frame, "flex")
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1200)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or ""
        n = int((slip or {}).get("n_selected") or 0)
        print("[SLIP]", n)
        print(raw[:900])
        if n < 4 or not all(x in raw for x in NEED) or "Kukuk" in raw:
            print("INCOMPLETE / wrong slip — will not submit")
            return 2

        print("\nSubmitting Flex 4...")
        try:
            print("submit:", submit_lineup(page, frame))
        except ExposureBlocked as e:
            print(e)
            print("EXPOSURE BLOCKED — no Submit click. Fix the slip or pick another path.")
            return 2
        page.wait_for_timeout(1800)
        for _ in range(4):
            conf = page.evaluate(
                """() => {
                  const els = Array.from(document.querySelectorAll('button, [role=button]'));
                  for (const el of els) {
                    const t = (el.innerText || '').trim().toLowerCase();
                    if (['confirm', 'submit', 'submit lineup', 'place entry', 'yes'].includes(t)) {
                      el.click(); return t;
                    }
                  }
                  return '';
                }"""
            )
            if conf:
                print("confirm:", conf)
                page.wait_for_timeout(1200)
            else:
                break

        frame = P.find_prizepicks_frame(page)
        slip2 = P.read_slip(frame, n_legs=4, ticket_type="flex")
        n2 = int((slip2 or {}).get("n_selected") or 0)
        body = page.evaluate(
            "() => (document.body && document.body.innerText || '').slice(0, 3000)"
        )
        low = (body or "").lower()
        print("\n[AFTER] n=", n2)
        if n2 == 0 or any(
            s in low
            for s in (
                "submitted",
                "good luck",
                "entry placed",
                "lineup submitted",
                "successfully",
            )
        ):
            print("SUBMITTED")
            return 0
        if n2 >= 4 and "submit lineup" in low:
            print("WARN: still on slip — confirm manually in Chrome if needed")
            return 3
        print("CHECK Chrome for confirmation UI")
        print((body or "")[:800])
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
