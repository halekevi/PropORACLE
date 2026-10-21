#!/usr/bin/env python3
"""Add non-QB 4th leg: try Pendergrass / Martin / Freeman / Davidson."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

CANDIDATES = [
    ("Pendergrass", "Pendergrass", "rec yards", [8.5, 9.5, 7.5], "More"),
    ("LJ Martin", "Martin", "rec yards", [19.5, 18.5, 20.5], "More"),
    ("Josiah Freeman", "Freeman", "rec", [4.5, 3.5, 5.5], "More"),  # Receptions
    ("Gideon Davidson", "Davidson", "rush yards", [60.5, 59.5, 61.5], "Less"),
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


def click_any(page, name: str, prop_sub: str, lines: list, action: str) -> str:
    return page.evaluate(
        """({name, propSub, lines, action}) => {
          const btns = Array.from(document.querySelectorAll('button')).filter(b =>
            ((b.innerText||'').trim() === action));
          const lineSet = new Set(lines.map(String));
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<9 && root;i++) {
              const t = root.innerText || '';
              if (!t.includes(name) || t.length > 450) { root = root.parentElement; continue; }
              const tl = t.toLowerCase();
              const ps = propSub.toLowerCase();
              let propOk = false;
              if (ps.includes('rush')) propOk = tl.includes('rush yards');
              else if (ps === 'rec') propOk = /\\brecs?\\b/.test(tl) && !tl.includes('yards');
              else if (ps.includes('rec')) propOk = tl.includes('rec yards') || tl.includes('receiving');
              if (!propOk || tl.includes('longest')) { root = root.parentElement; continue; }
              const hitLine = [...lineSet].find(l => t.includes(l));
              if (!hitLine) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + hitLine + ' ' + t.replace(/\\s+/g,' ').slice(0,140);
            }
          }
          // soft: any matching prop for name
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<9 && root;i++) {
              const t = root.innerText || '';
              if (!t.includes(name) || t.length > 450) { root = root.parentElement; continue; }
              const tl = t.toLowerCase();
              const ps = propSub.toLowerCase();
              let propOk = false;
              if (ps.includes('rush')) propOk = tl.includes('rush yards');
              else if (ps === 'rec') propOk = /\\brecs?\\b/.test(tl) && !tl.includes('yards');
              else if (ps.includes('rec')) propOk = tl.includes('rec yards') || tl.includes('receiving');
              if (!propOk || tl.includes('longest')) { root = root.parentElement; continue; }
              btn.click();
              return 'soft ' + t.replace(/\\s+/g,' ').slice(0,140);
            }
          }
          return 'no';
        }""",
        {"name": name, "propSub": prop_sub, "lines": lines, "action": action},
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
        slip0 = (P.read_slip(frame, n_legs=4, ticket_type="flex") or {}).get(
            "raw_slip_section"
        ) or ""
        print("BEFORE", slip0[:350])

        switched = P.switch_sport_tab(frame, page, "CFB", settle_ms=1500)
        if switched is None:
            P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
            # re-check slip - if wiped, abort and tell user
            frame = P.find_prizepicks_frame(page)
            slip1 = (P.read_slip(frame, n_legs=4, ticket_type="flex") or {}).get(
                "raw_slip_section"
            ) or ""
            if "Jones" not in slip1:
                print("WARN: navigate wiped slip — need full rebuild")
        else:
            frame = switched

        for search, name, prop, lines, action in CANDIDATES:
            print(f"\ntry {search}")
            react_search(page, search)
            page.wait_for_timeout(1800)
            probe = page.evaluate(
                """(name) => {
                  const t = document.body.innerText || '';
                  const i = t.indexOf(name);
                  return i < 0 ? 'missing' : t.slice(i, i+200).replace(/\\s+/g,' ');
                }""",
                name,
            )
            print(" probe", probe[:180])
            res = click_any(page, name, prop, lines, action)
            print(" click", res)
            if str(res).startswith("ok") or str(res).startswith("soft"):
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
        if n >= 4 and "Jones" in raw and "Overton" in raw and "Hiedeman" in raw and "Kukuk" not in raw:
            print("\nREADY — Flex 4 no QB. Hit Submit in Chrome.")
            return 0
        print("\nINCOMPLETE", n)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
