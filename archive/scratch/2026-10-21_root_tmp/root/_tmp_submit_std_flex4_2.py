#!/usr/bin/env python3
"""Ensure Standard Flex 4 #2 is on slip; submit if complete."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.pp_submit_guard import ExposureBlocked, submit_lineup_guarded

CDP = "http://127.0.0.1:9222"

LEGS = [
    ("CFB", "Jones", "Jones", "Rush Yards", 37.5, "Less"),
    ("CFB", "Kukuk", "Kukuk", "Rush Yards", 43.5, "More"),
    ("CFB", "Overton", "Overton", "Rush Yards", 44.5, "More"),
    ("WNBA", "Hiedeman", "Hiedeman", "3PT", 1.5, "Less"),
]

EXPOSURE_LEGS = [
    {"player": "Tavorus Jones", "prop_type": "Rush Yards", "direction": "UNDER", "pick_type": "Standard"},
    {"player": "Trey Kukuk", "prop_type": "Rush Yards", "direction": "OVER", "pick_type": "Standard"},
    {"player": "Jeffrey Overton Jr.", "prop_type": "Rush Yards", "direction": "OVER", "pick_type": "Standard"},
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
              else if (ps.includes('3pt')) propOk = /3\\s*-?\\s*pt|3ptm|three/.test(tl);
              else propOk = tl.includes(ps);
              if (!propOk) { root = root.parentElement; continue; }
              if (tl.includes('longest')) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,140);
            }
          }
          return 'no';
        }""",
        {"name": name, "propSub": prop_sub, "line": line, "action": action},
    )


def slip_ok(raw: str) -> bool:
    return all(x in raw for x in ("Jones", "Kukuk", "Overton", "Hiedeman"))


def build(page, frame) -> tuple:
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "flex")
    for sport, search, name, prop, line, action in LEGS:
        print(f"=== {name} {action} {line} ===")
        switched = P.switch_sport_tab(frame, page, sport, settle_ms=1400)
        if switched is not None:
            frame = switched
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        react_search(page, search)
        page.wait_for_timeout(1700)
        res = click_face(page, name, prop, line, action)
        if not str(res).startswith("ok") and name == "Hiedeman":
            for label in ("3PTM", "3-PT Made", "3PT"):
                res = click_face(page, name, label, line, action)
                if str(res).startswith("ok"):
                    break
        print(res)
        try:
            P.set_ticket_type(frame, "flex")
        except Exception:
            pass
        page.wait_for_timeout(450)
    try:
        frame.get_by_text("Flex Play", exact=True).first.click(timeout=1500)
    except Exception:
        pass
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=4, ticket_type="flex")
    raw = (slip or {}).get("raw_slip_section") or ""
    return frame, raw, int((slip or {}).get("n_selected") or 0)


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
        if page is None:
            print("ERROR: no board")
            return 1
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)
        slip0 = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw0 = (slip0 or {}).get("raw_slip_section") or ""
        n0 = int((slip0 or {}).get("n_selected") or 0)
        print("[BEFORE]", n0)
        print(raw0[:600])

        if not (n0 >= 4 and slip_ok(raw0)):
            print("\nRebuilding...")
            P.navigate_board_for_sport(page, "CFB", settle_ms=1800)
            frame = P.find_prizepicks_frame(page)
            frame, raw0, n0 = build(page, frame)
            print("[AFTER BUILD]", n0)
            print(raw0[:800])

        if not (n0 >= 4 and slip_ok(raw0)):
            print("INCOMPLETE — cannot submit")
            return 2

        try:
            P.set_ticket_type(frame, "flex")
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1200)
        except Exception:
            pass

        print("\nSubmitting...")
        try:
            sub = submit_lineup(page, frame)
        except ExposureBlocked as e:
            print(e)
            print("EXPOSURE BLOCKED — no Submit click. Fix the slip or pick another path.")
            return 2
        print("submit:", sub)
        page.wait_for_timeout(2000)

        # Confirm dialog / second submit
        for _ in range(3):
            conf = page.evaluate(
                """() => {
                  const els = Array.from(document.querySelectorAll('button, [role=button]'));
                  for (const el of els) {
                    const t = (el.innerText || '').trim().toLowerCase();
                    if (t === 'confirm' || t === 'submit' || t === 'submit lineup' || t === 'place entry') {
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
        slip1 = P.read_slip(frame, n_legs=4, ticket_type="flex")
        raw1 = (slip1 or {}).get("raw_slip_section") or ""
        n1 = int((slip1 or {}).get("n_selected") or 0)
        body = page.evaluate("() => (document.body && document.body.innerText || '').slice(0, 2500)")
        print("\n[AFTER SUBMIT] n=", n1)
        print(raw1[:800])
        # success signals
        low = (body or "").lower()
        if n1 == 0 or "submitted" in low or "good luck" in low or "entry placed" in low or "lineup submitted" in low:
            print("\nSUBMITTED — ticket should be in play.")
            return 0
        if "submit lineup" in low and n1 >= 4:
            print("\nWARN: slip still full — submit may need manual confirm in Chrome.")
            return 3
        print("\nCHECK Chrome for confirmation.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
