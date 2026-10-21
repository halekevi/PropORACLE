#!/usr/bin/env python3
"""Rebuild Standard Flex 4 with JS More/Less clicks. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

# Burton Points U14.5 is D-pass L5=5 (same family as Pts+Rebs U18.5)
LEGS = [
    ("CFB", "Pettaway", "Pettaway", "Rush Yards", 70.5, "Less"),
    ("CFB", "Bruhn", "Bruhn", "Rec Yards", 10.5, "More"),
    ("CFB", "Dampier", "Dampier", "Rush Yards", 37.5, "More"),
    ("WNBA", "Burton", "Burton", "Points", 14.5, "Less"),
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


def click_face(page, name: str, prop_sub: str, line: float, action: str) -> str:
    return page.evaluate(
        """({name, propSub, line, action}) => {
          const want = String(line);
          const btns = Array.from(document.querySelectorAll('button')).filter(b =>
            ((b.innerText||'').trim() === action));
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<9 && root;i++) {
              const t = root.innerText || '';
              if (!t.includes(name)) { root = root.parentElement; continue; }
              if (!t.includes(want)) { root = root.parentElement; continue; }
              const tl = t.toLowerCase();
              const ps = propSub.toLowerCase();
              const propOk = tl.includes(ps)
                || (ps.includes('rush') && tl.includes('rush yards'))
                || (ps.includes('rec') && (tl.includes('rec yards') || tl.includes('receiving')))
                || (ps === 'points' && /\\bpoints\\b/.test(tl) && !tl.includes('pts+'));
              if (!propOk || t.length > 450) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,140);
            }
          }
          return 'no';
        }""",
        {"name": name, "propSub": prop_sub, "line": line, "action": action},
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
        P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "flex")

        results = []
        for sport, search, name, prop, line, action in LEGS:
            print(f"\n=== {sport} {name} {action} {line} {prop} ===")
            switched = P.switch_sport_tab(frame, page, sport, settle_ms=1400)
            if switched is not None:
                frame = switched
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            react_search(page, search)
            page.wait_for_timeout(1800)
            res = click_face(page, name, prop, line, action)
            print(res)
            results.append(str(res).startswith("ok"))
            try:
                P.set_ticket_type(frame, "flex")
            except Exception:
                pass
            page.wait_for_timeout(500)

        try:
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2200])
        n = int((slip or {}).get("n_selected") or 0)
        ok = (
            n >= 4
            and "Pettaway" in raw
            and "Bruhn" in raw
            and "Dampier" in raw
            and "Burton" in raw
            and "Shepard" not in raw
        )
        if ok:
            print("\nREADY — Standard Flex 4. Hit Submit in CDP Chrome.")
            print("N-correct Flex: 4 correct 6x / 3 correct 1.5x")
            return 0
        print("\nINCOMPLETE", results)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
