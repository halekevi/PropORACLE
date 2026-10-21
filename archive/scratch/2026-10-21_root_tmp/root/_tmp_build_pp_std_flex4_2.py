#!/usr/bin/env python3
"""Standard Flex 4 #2: Jones / Kukuk / Overton / Hiedeman. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

LEGS = [
    ("CFB", "Jones", "Tavorus Jones", "Rush Yards", 37.5, "Less"),
    ("CFB", "Kukuk", "Kukuk", "Rush Yards", 43.5, "More"),
    ("CFB", "Overton", "Overton", "Rush Yards", 44.5, "More"),
    ("WNBA", "Hiedeman", "Hiedeman", "3PT", 1.5, "Less"),
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
              let propOk = false;
              if (ps.includes('rush')) propOk = tl.includes('rush yards') || tl.includes('rush yds');
              else if (ps.includes('3pt') || ps.includes('3-pt'))
                propOk = /3\\s*-?\\s*pt|3ptm|three/.test(tl);
              else propOk = tl.includes(ps);
              if (!propOk || t.length > 450) { root = root.parentElement; continue; }
              // avoid longest / combo
              if (tl.includes('longest') || tl.includes('combo') || tl.includes('+')) {
                if (!(ps.includes('3pt'))) { root = root.parentElement; continue; }
              }
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
        if page is None:
            print("ERROR: no board")
            return 1
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
            # Prefer full name match first
            res = click_face(page, name.split()[-1] if " " in name else name, prop, line, action)
            if not str(res).startswith("ok") and " " in name:
                res = click_face(page, name, prop, line, action)
            # Hiedeman prop label variants
            if not str(res).startswith("ok") and "3PT" in prop:
                for label in ("3-PT Made", "3PTM", "3-Pointers Made", "Made Threes"):
                    res = click_face(page, "Hiedeman", label, line, action)
                    if str(res).startswith("ok"):
                        break
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
            and "Jones" in raw
            and "Kukuk" in raw
            and "Overton" in raw
            and "Hiedeman" in raw
        )
        if ok:
            print("\nREADY — Standard Flex 4 #2. Hit Submit in CDP Chrome.")
            return 0
        print("\nINCOMPLETE", results)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
