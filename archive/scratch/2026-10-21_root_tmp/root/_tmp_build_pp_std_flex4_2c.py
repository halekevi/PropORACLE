#!/usr/bin/env python3
"""Standard Flex 4: Jones U / Overton O / Pendergrass O / Hiedeman U. No Kukuk."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

LEGS = [
    ("CFB", "Tavorus", "Tavorus", "rush yards", 37.5, "Less"),
    ("CFB", "Overton", "Overton", "rush yards", 44.5, "More"),
    ("CFB", "Pendergrass", "Pendergrass", "rec yards", 8.5, "More"),
    ("WNBA", "Hiedeman", "Hiedeman", "3pt", 1.5, "Less"),
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
              if (!t.includes(name) || !t.includes(want) || t.length > 450) {
                root = root.parentElement; continue;
              }
              const tl = t.toLowerCase();
              const ps = propSub.toLowerCase();
              let propOk = false;
              if (ps.includes('rush')) propOk = tl.includes('rush yards') || tl.includes('rush yds');
              else if (ps.includes('rec')) propOk = tl.includes('rec yards') || tl.includes('receiving');
              else if (ps.includes('3pt')) propOk = /3\\s*-?\\s*pt|3ptm|three/.test(tl);
              else propOk = tl.includes(ps);
              if (!propOk || tl.includes('longest')) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,150);
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
        P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "flex")

        results = []
        for sport, search, name, prop, line, action in LEGS:
            print(f"\n=== {name} {action} {line} {prop} ===")
            switched = P.switch_sport_tab(frame, page, sport, settle_ms=1400)
            if switched is not None:
                frame = switched
            # avoid full navigate after first leg
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            react_search(page, search)
            page.wait_for_timeout(1800)
            res = click_face(page, name, prop, line, action)
            if not str(res).startswith("ok") and name == "Tavorus":
                res = click_face(page, "Jones", prop, line, action)
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
            and "Overton" in raw
            and "Pendergrass" in raw
            and "Hiedeman" in raw
            and "Kukuk" not in raw
        )
        if ok:
            print("\nREADY — Flex 4 (no QB). Hit Submit in Chrome.")
            return 0
        print("\nINCOMPLETE", results)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
