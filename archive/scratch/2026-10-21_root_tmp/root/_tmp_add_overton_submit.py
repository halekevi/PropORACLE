#!/usr/bin/env python3
"""Add Overton Rush OVER (any nearby line) to complete Flex 4, then submit."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_submit_guard import ExposureBlocked, submit_lineup_guarded

CDP = "http://127.0.0.1:9222"

EXPOSURE_LEGS = [
    {"player": "Tavorus Jones", "prop_type": "Rush Yards", "direction": "UNDER", "pick_type": "Standard"},
    {"player": "Jeffrey Overton Jr.", "prop_type": "Rush Yards", "direction": "OVER", "pick_type": "Standard"},
    {"player": "Dawson Pendergrass", "prop_type": "Rec Yards", "direction": "OVER", "pick_type": "Standard"},
    {"player": "Natisha Hiedeman", "prop_type": "3PTM", "direction": "UNDER", "pick_type": "Standard"},
]


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
        else:
            # don't full-navigate if it wipes — try search on current
            pass
        frame = P.find_prizepicks_frame(page)

        react_search(page, "Overton")
        page.wait_for_timeout(2000)
        probe = page.evaluate(
            """() => {
              const t = document.body.innerText || '';
              const i = t.indexOf('Overton');
              return i < 0 ? 'MISSING' : t.slice(i, i+280).replace(/\\s+/g,' ');
            }"""
        )
        print("PROBE", probe)

        res = page.evaluate(
            """() => {
              const more = Array.from(document.querySelectorAll('button')).filter(b =>
                ((b.innerText||'').trim() === 'More'));
              for (const want of ['44.5','43.5','45.5','42.5','46.5']) {
                for (const btn of more) {
                  let root = btn;
                  for (let i=0;i<9 && root;i++) {
                    const t = root.innerText || '';
                    if (t.includes('Overton') && t.includes(want)
                        && /rush\\s*yards/i.test(t) && !/longest/i.test(t) && t.length < 400) {
                      btn.click();
                      return 'ok ' + want + ' ' + t.replace(/\\s+/g,' ').slice(0,140);
                    }
                    root = root.parentElement;
                  }
                }
              }
              // any Overton rush More
              for (const btn of more) {
                let root = btn;
                for (let i=0;i<9 && root;i++) {
                  const t = root.innerText || '';
                  if (t.includes('Overton') && /rush\\s*yards/i.test(t) && !/longest/i.test(t) && t.length < 400) {
                    btn.click();
                    return 'soft ' + t.replace(/\\s+/g,' ').slice(0,140);
                  }
                  root = root.parentElement;
                }
              }
              return 'no';
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
        n = int((slip or {}).get("n_selected") or 0)
        print("\n[SLIP]", n)
        print(raw[:1600])
        need = ("Jones", "Overton", "Pendergrass", "Hiedeman")
        if n < 4 or not all(x in raw for x in need):
            print("INCOMPLETE")
            return 2

        print("\nSubmitting...")
        try:
            result = submit_lineup_guarded(
                page,
                frame,
                EXPOSURE_LEGS,
                explicit_anchor_player="Tavorus Jones",
            )
            print("submit:", result)
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
                    if (['confirm','confirm entry','place entry','yes'].includes(t)) {
                      el.click(); return t;
                    }
                  }
                  return '';
                }"""
            )
            if conf:
                print("confirm", conf)
                page.wait_for_timeout(1200)
            else:
                break

        n2 = int(
            (P.read_slip(frame, n_legs=4, ticket_type="flex") or {}).get("n_selected")
            or 0
        )
        body = (
            page.evaluate(
                "() => (document.body && document.body.innerText || '').slice(0,2000)"
            )
            or ""
        ).lower()
        print("after n=", n2)
        if n2 == 0 or "submitted" in body or "good luck" in body or "entry" in body:
            print("SUBMITTED")
            return 0
        print("CHECK Chrome")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
