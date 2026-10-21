#!/usr/bin/env python3
"""Rebuild + submit Flex 4: Jones U37.5 / Overton O44.5 / Pendergrass O7.5 / Hiedeman U1.5."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_submit_guard import ExposureBlocked, submit_lineup_guarded

CDP = "http://127.0.0.1:9222"

LEGS = [
    ("CFB", "Tavorus", "Tavorus", "rush yards", [37.5], "Less"),
    ("CFB", "Overton", "Overton", "rush yards", [44.5], "More"),
    ("CFB", "Pendergrass", "Pendergrass", "rec yards", [7.5, 8.5], "More"),
    ("WNBA", "Hiedeman", "Hiedeman", "3pt", [1.5], "Less"),
]

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


def click_face(page, name: str, prop_sub: str, lines: list, action: str) -> str:
    return page.evaluate(
        """({name, propSub, lines, action}) => {
          const lineSet = new Set(lines.map(String));
          const btns = Array.from(document.querySelectorAll('button')).filter(b =>
            ((b.innerText||'').trim() === action));
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<9 && root;i++) {
              const t = root.innerText || '';
              if (!t.includes(name) || t.length > 450) { root = root.parentElement; continue; }
              const tl = t.toLowerCase();
              const ps = propSub.toLowerCase();
              let propOk = false;
              if (ps.includes('rush')) propOk = tl.includes('rush yards');
              else if (ps.includes('rec')) propOk = tl.includes('rec yards') || tl.includes('receiving');
              else if (ps.includes('3pt')) propOk = /3\\s*-?\\s*pt|3ptm|three/.test(tl);
              if (!propOk || tl.includes('longest')) { root = root.parentElement; continue; }
              const hit = [...lineSet].find(l => t.includes(l));
              if (!hit) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + hit + ' ' + t.replace(/\\s+/g,' ').slice(0,140);
            }
          }
          return 'no';
        }""",
        {"name": name, "propSub": prop_sub, "lines": lines, "action": action},
    )


def submit_lineup(page, frame) -> str:
    """Hard-gated Submit — raises ExposureBlocked (do not swallow as soft miss)."""
    return submit_lineup_guarded(
        page,
        frame,
        EXPOSURE_LEGS,
        explicit_anchor_player="Tavorus Jones",
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
        for sport, search, name, prop, lines, action in LEGS:
            print(f"=== {name} {action} {lines} ===")
            switched = P.switch_sport_tab(frame, page, sport, settle_ms=1400)
            if switched is not None:
                frame = switched
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            react_search(page, search)
            page.wait_for_timeout(1800)
            res = click_face(page, name, prop, lines, action)
            if not str(res).startswith("ok") and name == "Tavorus":
                res = click_face(page, "Jones", prop, lines, action)
            print(res)
            results.append(str(res).startswith("ok"))
            try:
                P.set_ticket_type(frame, "flex")
            except Exception:
                pass
            page.wait_for_timeout(450)

        try:
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or ""
        n = int((slip or {}).get("n_selected") or 0)
        print("\n[SLIP]")
        print(raw[:1600])
        need = ("Jones", "Overton", "Pendergrass", "Hiedeman")
        if n < 4 or not all(x in raw for x in need):
            print("INCOMPLETE", results)
            return 2

        print("\nSubmitting...")
        try:
            print(submit_lineup(page, frame))
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
                    if (['confirm','submit','submit lineup','place entry','yes'].includes(t)) {
                      el.click(); return t;
                    }
                  }
                  return '';
                }"""
            )
            if conf:
                print("confirm:", conf)
                page.wait_for_timeout(1200)
            else:
                break

        frame = P.find_prizepicks_frame(page)
        n2 = int(
            (P.read_slip(frame, n_legs=4, ticket_type="flex") or {}).get("n_selected")
            or 0
        )
        body = (
            page.evaluate(
                "() => (document.body && document.body.innerText || '').slice(0, 2500)"
            )
            or ""
        ).lower()
        print("after n=", n2)
        if n2 == 0 or any(
            s in body
            for s in ("submitted", "good luck", "entry placed", "lineup submitted")
        ):
            print("SUBMITTED")
            return 0
        print("CHECK Chrome — may need manual confirm")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
