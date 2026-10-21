#!/usr/bin/env python3
"""Probe Dampier Pass Yards card DOM for alt-line swap controls."""
from __future__ import annotations

import json
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
        frame = P.find_prizepicks_frame(page)
        P.switch_sport_tab(frame, page, "CFB", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)
        # search
        page.evaluate(
            """() => {
          const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
          if (!inp) return;
          const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
          proto.set.call(inp, 'Dampier');
          inp.dispatchEvent(new Event('input', {bubbles:true}));
        }"""
        )
        page.wait_for_timeout(2000)

        info = page.evaluate(
            """() => {
          const moreBtns = Array.from(document.querySelectorAll('button')).filter(b =>
            ((b.innerText||'').trim() === 'More' || (b.innerText||'').trim() === '+ More')
          );
          let root = null;
          let more = null;
          for (const btn of moreBtns) {
            let r = btn;
            for (let i=0;i<10 && r;i++) {
              const t = r.innerText || '';
              if (t.includes('Dampier') && /Pass\\s*Yards/i.test(t) && t.length < 500) {
                root = r; more = btn; break;
              }
              r = r.parentElement;
            }
            if (root) break;
          }
          if (!root) return {ok:false, moreCount: moreBtns.length};
          const buttons = Array.from(root.querySelectorAll('button, [role=button], [class*=arrow], [class*=Arrow], [class*=swap], [class*=Swap], svg'));
          const details = buttons.map(b => {
            const r = b.getBoundingClientRect();
            return {
              tag: b.tagName,
              txt: (b.innerText||'').trim().slice(0,40),
              al: (b.getAttribute('aria-label')||'').slice(0,60),
              cls: (b.className||'').toString().slice(0,80),
              html: b.outerHTML.slice(0,160),
              w: Math.round(r.width), h: Math.round(r.height),
              x: Math.round(r.x), y: Math.round(r.y),
            };
          }).filter(d => d.w > 0 && d.h > 0);
          // also look for img/icons
          const imgs = Array.from(root.querySelectorAll('img, [class*=icon], [class*=Icon]')).map(el => ({
            tag: el.tagName,
            cls: (el.className||'').toString().slice(0,80),
            src: (el.getAttribute('src')||'').slice(0,80),
            al: (el.getAttribute('aria-label')||el.getAttribute('alt')||'').slice(0,60),
          }));
          return {
            ok: true,
            cardText: (root.innerText||'').replace(/\\s+/g,' ').slice(0,300),
            cardCls: (root.className||'').toString().slice(0,100),
            buttons: details.slice(0,40),
            imgs: imgs.slice(0,20),
            // try clicking soFresh specifically
            sofresh: Array.from(document.querySelectorAll('[class*=soFresh], [class*=sofresh], [class*=alt], [class*=Alt]')).slice(0,10).map(el => ({
              cls: (el.className||'').toString().slice(0,100),
              txt: (el.innerText||'').trim().slice(0,40),
            })),
          };
        }"""
        )
        print(json.dumps(info, indent=2)[:8000])

        # Also dump how collect_payout_data finds alt swap
        cards = P.get_all_cards(frame)
        damp = [c for c in cards if "Dampier" in str(c.get("player") or "") or (
            "Pass" in str(c.get("prop_type") or "") and "218" in str(c.get("line") or "")
        )]
        # cards may say UTAH - QB
        utah = [c for c in cards if "Pass" in str(c.get("prop_type") or "") and abs(float(c.get("line") or 0) - 218.5) < 0.01]
        print("utah pass cards", len(utah))
        for c in utah[:3]:
            print({k: c.get(k) for k in ("player", "prop_type", "line", "pick_type", "has_alt_lines", "more_index")})
            if c.get("more_btn") is not None or c.get("more_index") is not None:
                st = P._read_card_state_from_more(
                    c.get("more_btn"), frame=frame, more_index=c.get("more_index")
                )
                print("  state", st)
                # try one swap via library
                ok = P._click_alt_line_swap(
                    c.get("more_btn"),
                    frame=frame,
                    more_index=c.get("more_index"),
                    player="Devon Dampier",
                    prop="Pass Yards",
                )
                print("  swap click", ok)
                page.wait_for_timeout(600)
                st2 = P._read_card_state_from_more(
                    c.get("more_btn"), frame=frame, more_index=c.get("more_index")
                )
                print("  state after", st2)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
