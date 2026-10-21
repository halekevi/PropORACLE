#!/usr/bin/env python3
"""Add Kukuk Rush O43.5 to existing 3-leg slip."""
from __future__ import annotations

import sys
import time
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
        P.switch_sport_tab(frame, page, "CFB", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)

        for q in ("Trey Kukuk", "Kukuk"):
            react_search(page, q)
            page.wait_for_timeout(2000)
            probe = page.evaluate(
                """() => {
                  const t = document.body.innerText || '';
                  const i = t.indexOf('Kukuk');
                  if (i < 0) return 'no Kukuk';
                  return t.slice(Math.max(0,i-40), i+220).replace(/\\s+/g,' ');
                }"""
            )
            print("probe", q, probe)
            # list cards
            cards = P.get_all_cards(frame)
            for c in cards:
                pname = str(c.get("player") or "")
                if "Kukuk" not in pname and "Kukuk" not in str(
                    (P._read_card_state_from_more(
                        c.get("more_btn"), frame=frame, more_index=c.get("more_index")
                    ) or {}).get("text")
                    or ""
                ):
                    continue
                print("card", pname, c.get("prop_type"), c.get("line"), c.get("pick_type"))

            res = page.evaluate(
                """() => {
                  const more = Array.from(document.querySelectorAll('button')).filter(b =>
                    ((b.innerText||'').trim() === 'More'));
                  for (const btn of more) {
                    let root = btn;
                    for (let i=0;i<9 && root;i++) {
                      const t = root.innerText || '';
                      if (!t.includes('Kukuk')) { root = root.parentElement; continue; }
                      const tl = t.toLowerCase();
                      if (!(tl.includes('rush yards') || tl.includes('rush yds'))) {
                        root = root.parentElement; continue;
                      }
                      if (tl.includes('longest')) { root = root.parentElement; continue; }
                      // prefer 43.5 standard
                      if (t.includes('43.5') || t.includes('43')) {
                        btn.click();
                        return 'ok43 ' + t.replace(/\\s+/g,' ').slice(0,140);
                      }
                      root = root.parentElement;
                    }
                  }
                  // any rush yards More for Kukuk
                  for (const btn of more) {
                    let root = btn;
                    for (let i=0;i<9 && root;i++) {
                      const t = root.innerText || '';
                      if (t.includes('Kukuk') && /rush\\s*yards/i.test(t) && !/longest/i.test(t) && t.length < 400) {
                        btn.click();
                        return 'any ' + t.replace(/\\s+/g,' ').slice(0,140);
                      }
                      root = root.parentElement;
                    }
                  }
                  return 'no';
                }"""
            )
            print("click", res)
            if str(res).startswith("ok") or str(res).startswith("any"):
                break

        # If landed wrong face, try alt swap to 43.5 then More
        page.wait_for_timeout(800)
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
