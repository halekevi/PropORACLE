"""Debug CFB board search — find what actually narrows cards."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"


def main() -> int:
    with sync_playwright() as p:
        b = p.chromium.connect_over_cdp(CDP, timeout=60_000)
        page = next(
            pg
            for pg in b.contexts[0].pages
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
        )
        page.bring_to_front()
        P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, "Pass Yards")
        page.wait_for_timeout(800)
        frame = P.find_prizepicks_frame(page)
        info = frame.evaluate(
            """() => {
              const sels = [
                "input[placeholder*='Search']",
                "input[type='search']",
                "input[aria-label*='Search' i]",
                "input[aria-label='search']",
                "input",
              ];
              const found = [];
              for (const sel of sels) {
                document.querySelectorAll(sel).forEach((el, i) => {
                  const r = el.getBoundingClientRect();
                  found.push({
                    sel, i,
                    aria: el.getAttribute('aria-label'),
                    ph: el.getAttribute('placeholder'),
                    type: el.getAttribute('type'),
                    visible: r.width > 0 && r.height > 0,
                    w: r.width, h: r.height,
                    val: el.value,
                  });
                });
              }
              return found;
            }"""
        )
        print("inputs:", flush=True)
        for row in info:
            print(f"  {row}", flush=True)

        # Try playwright type with delay on first visible search-like input
        for sel in [
            "input[placeholder*='Search']",
            "input[type='search']",
            "input[aria-label*='Search']",
            "input[aria-label='search']",
        ]:
            box = frame.locator(sel).first
            if box.count() == 0:
                continue
            try:
                box.click(force=True, timeout=1500)
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.wait_for_timeout(200)
                page.keyboard.type("Arch Manning", delay=40)
                page.wait_for_timeout(1500)
                n = len(P.get_all_cards(frame))
                print(f"type via {sel} -> {n} cards", flush=True)
                # clear
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.wait_for_timeout(500)
                break
            except Exception as e:
                print(f"fail {sel}: {e}", flush=True)

        # Also try main page (not frame)
        for sel in ["input[placeholder*='Search']", "input[type='search']"]:
            box = page.locator(sel).first
            if box.count() == 0:
                continue
            try:
                box.click(force=True, timeout=1500)
                page.keyboard.press("Control+A")
                page.keyboard.type("Arch Manning", delay=40)
                page.wait_for_timeout(1500)
                frame = P.find_prizepicks_frame(page)
                n = len(P.get_all_cards(frame))
                print(f"page type via {sel} -> {n} cards", flush=True)
                break
            except Exception as e:
                print(f"page fail {sel}: {e}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
