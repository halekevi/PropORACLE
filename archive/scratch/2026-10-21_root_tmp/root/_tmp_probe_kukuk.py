#!/usr/bin/env python3
"""Navigate CFB board and probe Kukuk availability."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"


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
        page.bring_to_front()
        P.navigate_board_for_sport(page, "CFB", settle_ms=2500)
        frame = P.find_prizepicks_frame(page)
        page.evaluate(
            """(q) => {
              const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
              if (!inp) return;
              const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
              if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
              inp.dispatchEvent(new Event('input', {bubbles:true}));
              inp.dispatchEvent(new Event('change', {bubbles:true}));
            }""",
            "Kukuk",
        )
        page.wait_for_timeout(2200)
        probe = page.evaluate(
            """() => {
              const t = document.body.innerText || '';
              const hits = [];
              let idx = 0;
              while ((idx = t.indexOf('Kukuk', idx)) >= 0 && hits.length < 5) {
                hits.push(t.slice(Math.max(0,idx-30), idx+180).replace(/\\s+/g,' '));
                idx += 5;
              }
              return hits.length ? hits : ['NO_KUKUK_ON_BOARD'];
            }"""
        )
        print("URL", page.url)
        for h in probe:
            print("HIT:", h)
        # also try Louisiana Tech / LT QB search
        for q in ("Louisiana Tech", "LT - QB", "Trey"):
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
            page.wait_for_timeout(1500)
            has = page.evaluate(
                """() => {
                  const t = document.body.innerText || '';
                  return {
                    kukuk: t.includes('Kukuk'),
                    rush43: t.includes('43.5') && t.includes('Kukuk'),
                    sample: (() => { const i=t.indexOf('Kukuk'); return i<0? '' : t.slice(i, i+160).replace(/\\s+/g,' '); })()
                  };
                }"""
            )
            print(q, has)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
