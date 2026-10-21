#!/usr/bin/env python3
"""Navigate CFB and probe Tavorus Jones without clearing slip first."""
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
        # snapshot slip before navigate
        frame = P.find_prizepicks_frame(page)
        before = (P.read_slip(frame, n_legs=4, ticket_type="flex") or {}).get(
            "raw_slip_section"
        ) or ""
        print("BEFORE n check:\n", before[:400])

        P.navigate_board_for_sport(page, "CFB", settle_ms=2500)
        frame = P.find_prizepicks_frame(page)
        after_nav = (P.read_slip(frame, n_legs=4, ticket_type="flex") or {}).get(
            "raw_slip_section"
        ) or ""
        print("\nAFTER NAV:\n", after_nav[:400])

        page.evaluate(
            """(q) => {
              const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
              if (!inp) return;
              const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
              if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
              inp.dispatchEvent(new Event('input', {bubbles:true}));
            }""",
            "Tavorus",
        )
        page.wait_for_timeout(2200)
        probe = page.evaluate(
            """() => {
              const t = document.body.innerText || '';
              const keys = ['Tavorus', 'UTEP', '37.5'];
              return {
                tavorus: t.includes('Tavorus'),
                utep: t.includes('UTEP'),
                sample: (() => {
                  const i = t.indexOf('Tavorus');
                  if (i >= 0) return t.slice(i, i+280).replace(/\\s+/g,' ');
                  const u = t.indexOf('UTEP');
                  return u < 0 ? 'none' : t.slice(u, u+200).replace(/\\s+/g,' ');
                })()
              };
            }"""
        )
        print("PROBE", probe)

        if probe.get("tavorus"):
            res = page.evaluate(
                """() => {
                  const less = Array.from(document.querySelectorAll('button')).filter(b =>
                    ((b.innerText||'').trim() === 'Less'));
                  for (const btn of less) {
                    let root = btn;
                    for (let i=0;i<9 && root;i++) {
                      const t = root.innerText || '';
                      if (t.includes('Tavorus') && /rush\\s*yards/i.test(t)
                          && !/longest/i.test(t) && t.length < 400) {
                        btn.click();
                        return 'ok ' + t.replace(/\\s+/g,' ').slice(0,160);
                      }
                      root = root.parentElement;
                    }
                  }
                  return 'no';
                }"""
            )
            print("click", res)

        frame = P.find_prizepicks_frame(page)
        try:
            P.set_ticket_type(frame, "flex")
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1200)
        except Exception:
            pass
        slip = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:1800])
        n = int((slip or {}).get("n_selected") or 0)
        print("n=", n)
        return 0 if n >= 4 and "Jones" in raw and "Kukuk" in raw else 2


if __name__ == "__main__":
    raise SystemExit(main())
