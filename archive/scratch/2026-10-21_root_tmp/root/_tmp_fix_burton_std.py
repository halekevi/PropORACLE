#!/usr/bin/env python3
"""Fix Burton: remove Points 14.5, add Pts+Rebs UNDER 18.5."""
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

        # Remove wrong Burton Points via More (toggle off) or Less if UNDER was wrong
        res = page.evaluate(
            """() => {
              const btns = Array.from(document.querySelectorAll('button'));
              for (const btn of btns) {
                const label = (btn.innerText||'').trim();
                if (label !== 'More' && label !== 'Less') continue;
                let root = btn;
                for (let i=0;i<10 && root;i++) {
                  const t = root.innerText || '';
                  if (t.includes('Burton') && t.includes('14.5') && t.includes('Points') && t.length < 400) {
                    btn.click();
                    return label + ' ' + t.replace(/\\s+/g,' ').slice(0,120);
                  }
                  root = root.parentElement;
                }
              }
              return 'no';
            }"""
        )
        print("remove", res)
        page.wait_for_timeout(900)

        P.switch_sport_tab(frame, page, "WNBA", settle_ms=1400)
        frame = P.find_prizepicks_frame(page)
        react_search(page, "Burton")
        page.wait_for_timeout(1600)

        # Click Less on Pts+Rebs 18.5 standard
        res2 = page.evaluate(
            """() => {
              const less = Array.from(document.querySelectorAll('button')).filter(b =>
                ((b.innerText||'').trim() === 'Less'));
              for (const btn of less) {
                let root = btn;
                for (let i=0;i<8 && root;i++) {
                  const t = root.innerText || '';
                  if (t.includes('Burton') && t.includes('18.5')
                      && (t.includes('Pts+Rebs') || t.includes('Pts + Rebs') || t.includes('P+R'))
                      && t.length < 400) {
                    btn.click();
                    return 'ok ' + t.replace(/\\s+/g,' ').slice(0,120);
                  }
                  root = root.parentElement;
                }
              }
              // fallback any 18.5 Burton Less
              for (const btn of less) {
                let root = btn;
                for (let i=0;i<8 && root;i++) {
                  const t = root.innerText || '';
                  if (t.includes('Burton') && t.includes('18.5') && t.length < 350) {
                    btn.click();
                    return 'soft ' + t.replace(/\\s+/g,' ').slice(0,120);
                  }
                  root = root.parentElement;
                }
              }
              return 'no';
            }"""
        )
        print("add", res2)

        leg = {
            "sport": "WNBA",
            "player": "Veronica Burton",
            "prop_type": "Pts+Rebs",
            "direction": "UNDER",
            "line": 18.5,
            "pick_type": "standard",
        }
        if not str(res2).startswith("ok") and not str(res2).startswith("soft"):
            ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
            print("add_leg", ok)

        try:
            P.set_ticket_type(frame, "flex")
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2000])
        ok = (
            "Pettaway" in raw
            and "Bruhn" in raw
            and "Dampier" in raw
            and "Burton" in raw
            and "18.5" in raw
            and "14.5" not in raw.split("Burton", 1)[-1][:80]
        )
        if ok and int((slip or {}).get("n_selected") or 0) >= 4:
            print("\nREADY — Standard Flex 4 fixed.")
            return 0
        print("\nCHECK slip — Burton prop may still be wrong")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
