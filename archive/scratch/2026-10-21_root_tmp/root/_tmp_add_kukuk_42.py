#!/usr/bin/env python3
"""Finish Flex 4: add Kukuk Rush OVER 42.5 (line moved from 43.5)."""
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
        P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
        frame = P.find_prizepicks_frame(page)
        react_search(page, "Kukuk")
        page.wait_for_timeout(2000)

        # Cycle to standard 42.5 if needed, then More
        res = page.evaluate(
            """() => {
              const more = Array.from(document.querySelectorAll('button')).filter(b =>
                ((b.innerText||'').trim() === 'More'));
              for (const btn of more) {
                let root = btn;
                for (let i=0;i<9 && root;i++) {
                  const t = root.innerText || '';
                  if (!t.includes('Kukuk')) { root = root.parentElement; continue; }
                  if (!/rush\\s*yards/i.test(t) || /longest/i.test(t) || t.length > 400) {
                    root = root.parentElement; continue;
                  }
                  // accept 42.5 or 43.5
                  if (t.includes('42.5') || t.includes('43.5')) {
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

        # If face is goblin, try alt swap via library on first Kukuk rush card
        if not str(res).startswith("ok"):
            cards = P.get_all_cards(frame)
            for c in cards:
                st = P._read_card_state_from_more(
                    c.get("more_btn"), frame=frame, more_index=c.get("more_index")
                ) or {}
                blob = " ".join(str(x) for x in (st.get("text") or []))
                if "Kukuk" not in blob and "Kukuk" not in str(c.get("player") or ""):
                    continue
                prop = str(c.get("prop_type") or "")
                if "Rush" not in prop:
                    continue
                print("card", c.get("player"), prop, c.get("line"), c.get("pick_type"))
                btn, idx = c.get("more_btn"), c.get("more_index")
                for i in range(12):
                    st2 = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
                    pt = str(st2.get("pickType") or "").lower()
                    line = st2.get("line")
                    print(f"  face[{i}] {pt} {line}")
                    if pt == "standard" and line is not None and abs(float(line) - 42.5) < 0.01:
                        try:
                            btn.click(timeout=1500, no_wait_after=True)
                        except Exception:
                            btn.click(force=True, timeout=2000, no_wait_after=True)
                        res = f"cycled {line}"
                        break
                    if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
                        break
                    page.wait_for_timeout(450)
                break

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
            line_note = "42.5" if "42.5" in raw else ("43.5" if "43.5" in raw else "?")
            print(f"\nREADY — Flex 4 built (Kukuk Rush {line_note}). Hit Submit in Chrome.")
            return 0
        print("\nINCOMPLETE", n)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
