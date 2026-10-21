#!/usr/bin/env python3
"""Fix Malen leg: remove wrong Shots, add SOT 0.5 Goblin. No submit."""
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


def remove_malen(page) -> str:
    return page.evaluate(
        """() => {
      const nodes = Array.from(document.querySelectorAll('div, li, article, section'));
      for (const n of nodes) {
        const t = n.innerText || '';
        if (!t.includes('Malen') || t.length > 500) continue;
        if (!/Current Lineup|Players Selected|Shots|SOT/.test(t)) continue;
        const btns = Array.from(n.querySelectorAll('button, [role=button], [aria-label]'));
        for (const b of btns) {
          const al = ((b.getAttribute && b.getAttribute('aria-label')) || '') + (b.className || '');
          const txt = (b.innerText || '').trim();
          if (/remove|delete|close|lineupclose/i.test(al) || txt === '×' || txt === 'X') {
            (b.closest('button') || b).click();
            return 'removed ' + (al || txt).slice(0, 40);
          }
        }
      }
      return 'fail';
    }"""
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

        r = remove_malen(page)
        print("[REMOVE]", r)
        page.wait_for_timeout(800)
        slip = P.read_slip(frame, n_legs=2, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("[SLIP]", raw[:600])
        if "Malen" in raw and "2.5" in raw:
            # try again with playwright close on Malen row
            try:
                loc = frame.get_by_text("Donyell Malen", exact=False).first
                parent = loc.locator("xpath=ancestor::*[contains(., 'Shots') or contains(., 'SOT')][1]")
                for sel in [
                    "button[aria-label*='emove']",
                    "button[aria-label*='lose']",
                    ".lineupclose-btn",
                    "button.close-btn",
                ]:
                    try:
                        parent.locator(sel).first.click(timeout=800, force=True)
                        print("pw remove", sel)
                        break
                    except Exception:
                        pass
            except Exception as e:
                print("pw err", e)
            page.wait_for_timeout(600)
            raw = (P.read_slip(frame, n_legs=2, ticket_type="power") or {}).get(
                "raw_slip_section"
            ) or ""
            print("[SLIP2]", raw[:600])

        # Soccer + search Malen
        page.evaluate(
            """() => {
          const row = Array.from(document.querySelectorAll('div,nav')).find(el => {
            const t = el.innerText || '';
            return t.includes('NFL') && t.includes('CFB') && t.includes('MLB') && el.scrollWidth > el.clientWidth;
          });
          if (row) row.scrollLeft = row.scrollWidth;
        }"""
        )
        page.wait_for_timeout(300)
        for label in ("Soccer", "SOCCER"):
            try:
                frame.get_by_text(label, exact=True).first.click(timeout=2000)
                page.wait_for_timeout(1500)
                print("sport", label)
                break
            except Exception:
                pass
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        react_search(page, "Malen")
        page.wait_for_timeout(1800)

        cards = P.get_all_cards(frame)
        sot = None
        for c in cards:
            st = P._read_card_state_from_more(
                c.get("more_btn"), frame=frame, more_index=c.get("more_index")
            ) or {}
            blob = " ".join(str(x) for x in (st.get("text") or []))
            prop = str(c.get("prop_type") or "")
            if "Malen" not in blob and "Malen" not in str(c.get("player") or ""):
                continue
            if prop.upper() == "SOT" or "SOT" in blob or "Shots On Target" in blob:
                # prefer already 0.5 goblin
                if abs(float(c.get("line") or -1) - 0.5) < 0.01 or "0.5" in blob:
                    sot = c
                    break
                if sot is None:
                    sot = c
        if sot is None:
            print("no SOT card")
            return 2
        print(
            "SOT card",
            sot.get("player"),
            sot.get("prop_type"),
            sot.get("line"),
            sot.get("pick_type"),
        )
        # cycle if needed
        btn, idx = sot.get("more_btn"), sot.get("more_index")
        for i in range(8):
            st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
            pt = str(st.get("pickType") or "").lower()
            line = st.get("line")
            blob = " ".join(str(x) for x in (st.get("text") or []))
            print(f" face[{i}] {pt} {line} {blob[:80]}")
            if "SOT" in blob or (sot.get("prop_type") or "").upper() == "SOT":
                if pt == "goblin" and abs(float(line or -1) - 0.5) < 0.01:
                    break
            if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
                break
            time.sleep(0.45)
        # click More
        try:
            if btn is not None:
                btn.click(force=True, timeout=2000, no_wait_after=True)
            else:
                P._eval_more_js(
                    "el => { el.click(); return true; }", None, frame=frame, more_index=idx
                )
        except Exception as e:
            print("click err", e)
            return 2
        time.sleep(0.5)
        try:
            P.set_ticket_type(frame, "power")
            frame.get_by_text("Power Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=3, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:1800])
        good = (
            "Gasser" in raw
            and "Freeman" in raw
            and "Malen" in raw
            and ("0.5SOT" in raw.replace(" ", "") or "0.5 SOT" in raw or "SOT" in raw)
            and "2.5Shots" not in raw.replace(" ", "")
        )
        # softer: 0.5 and SOT
        soft = "Gasser" in raw and "Josiah Freeman" in raw and "Malen" in raw and "0.5" in raw and "SOT" in raw
        if good or soft:
            print("\nREADY — Power 3 #2 built. Hit Submit.")
            return 0
        print("\nINCOMPLETE")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
