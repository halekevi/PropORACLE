#!/usr/bin/env python3
"""Add Tavorus Jones Rush UNDER to existing 3-leg slip; probe live line."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"


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
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)
        switched = P.switch_sport_tab(frame, page, "CFB", settle_ms=1500)
        if switched is not None:
            frame = switched
        frame = P.find_prizepicks_frame(page)

        react_search(page, "Tavorus Jones")
        page.wait_for_timeout(2000)
        probe = page.evaluate(
            """() => {
              const t = document.body.innerText || '';
              const i = t.indexOf('Tavorus Jones');
              if (i < 0) {
                const j = t.indexOf('Jones');
                return j < 0 ? 'NO_JONES' : t.slice(j, j+300).replace(/\\s+/g,' ');
              }
              return t.slice(i, i+320).replace(/\\s+/g,' ');
            }"""
        )
        print("PROBE:", probe)

        # Click Less on any Tavorus Jones Rush Yards standard face
        res = page.evaluate(
            """() => {
              const less = Array.from(document.querySelectorAll('button')).filter(b =>
                ((b.innerText||'').trim() === 'Less'));
              const candidates = [];
              for (const btn of less) {
                let root = btn;
                for (let i=0;i<9 && root;i++) {
                  const t = root.innerText || '';
                  if (t.includes('Tavorus Jones') && /rush\\s*yards/i.test(t)
                      && !/longest/i.test(t) && t.length < 400) {
                    candidates.push(t.replace(/\\s+/g,' ').slice(0,160));
                    // prefer 37.5, else any
                    if (t.includes('37.5')) {
                      btn.click();
                      return 'ok37 ' + candidates[candidates.length-1];
                    }
                  }
                  root = root.parentElement;
                }
              }
              // any rush under
              for (const btn of less) {
                let root = btn;
                for (let i=0;i<9 && root;i++) {
                  const t = root.innerText || '';
                  if (t.includes('Tavorus Jones') && /rush\\s*yards/i.test(t)
                      && !/longest/i.test(t) && t.length < 400) {
                    btn.click();
                    return 'any ' + t.replace(/\\s+/g,' ').slice(0,160);
                  }
                  root = root.parentElement;
                }
              }
              return 'no cands=' + JSON.stringify(candidates.slice(0,3));
            }"""
        )
        print("click", res)

        try:
            P.set_ticket_type(frame, "flex")
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1200)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2000])
        n = int((slip or {}).get("n_selected") or 0)
        if n >= 4 and all(x in raw for x in ("Jones", "Kukuk", "Overton", "Hiedeman")):
            print("\nREADY — Flex 4 complete. Hit Submit in Chrome.")
            return 0
        print("\nINCOMPLETE", n)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
