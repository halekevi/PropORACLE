#!/usr/bin/env python3
"""Build Power 3 #1 via CDP with forced search. Does NOT submit."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
LEGS = [
    {
        "sport": "MLB",
        "league_id": 2,
        "player": "Cam Schlittler",
        "prop_type": "Pitcher Strikeouts",
        "direction": "OVER",
        "line": 3.5,
        "pick_type": "goblin",
        "search": "Schlittler",
    },
    {
        "sport": "WNBA",
        "league_id": 3,
        "player": "Arike Ogunbowale",
        "prop_type": "Rebs+Asts",
        "direction": "OVER",
        "line": 4.5,
        "pick_type": "goblin",
        "search": "Ogunbowale",
    },
    {
        "sport": "CFB",
        "league_id": 15,
        "player": "Devon Dampier",
        "prop_type": "Pass Yards",
        "direction": "OVER",
        "line": 159.5,
        "pick_type": "goblin",
        "search": "Dampier",
    },
]


def _search(page, q: str) -> None:
    box = page.locator("input[placeholder*='Search'], input[aria-label='search']").first
    box.wait_for(state="attached", timeout=10_000)
    box.click(force=True, timeout=5000)
    box.fill("")
    box.fill(q, force=True)
    page.wait_for_timeout(2000)


def _click_more_less(page, player: str, prop: str, line: float, direction: str) -> bool:
    """Find card containing player+prop+line and click More (OVER) or Less (UNDER)."""
    # Prefer exact line face; Goblin often needs alt-line cycle — try More first.
    want = "More" if direction.upper() == "OVER" else "Less"
    # JS: find More buttons whose card text matches
    js = """
    ([player, prop, line, want]) => {
      const lineStr = String(line);
      const buttons = Array.from(document.querySelectorAll('button'));
      const moreBtns = buttons.filter(b => {
        const t = (b.innerText || '').trim();
        return t === 'More' || t === '+ More' || t === want;
      });
      for (const btn of moreBtns) {
        let root = btn;
        for (let i = 0; i < 8 && root; i++) {
          const t = (root.innerText || '');
          if (t.includes(player) && t.toLowerCase().includes(prop.toLowerCase().replace('+','+'))
              && t.includes(lineStr)) {
            // Prefer the More/Less matching want near this card
            const locals = Array.from(root.querySelectorAll('button'));
            const hit = locals.find(b => (b.innerText||'').trim() === want)
                     || locals.find(b => (b.innerText||'').trim() === 'More' || (b.innerText||'').trim() === '+ More');
            if (hit) { hit.click(); return 'clicked:' + want + ' on ' + player; }
          }
          root = root.parentElement;
        }
      }
      // Fallback: player only
      for (const btn of moreBtns) {
        let root = btn;
        for (let i = 0; i < 8 && root; i++) {
          const t = (root.innerText || '');
          if (t.includes(player) && t.includes(lineStr)) {
            const locals = Array.from(root.querySelectorAll('button'));
            const hit = locals.find(b => (b.innerText||'').trim() === want)
                     || locals.find(b => ['More','+ More','Less'].includes((b.innerText||'').trim()));
            if (hit) { hit.click(); return 'fallback:' + (hit.innerText||'').trim() + ' on ' + player; }
          }
          root = root.parentElement;
        }
      }
      return 'not_found';
    }
    """
    prop_norm = prop.replace("Rebs+Asts", "Rebs+Asts").replace("Pitcher Strikeouts", "Strikeouts")
    # try several prop label variants
    variants = [prop, prop_norm, prop.replace("Pitcher ", ""), "Strikeouts", "Pass Yards", "Rebs+Asts", "Reb+Asts"]
    for pv in variants:
        res = page.evaluate(js, [player, pv, line, want])
        print(f"  [JS] prop={pv!r} -> {res}")
        if res and str(res).startswith(("clicked", "fallback")):
            return True
    return False


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
            print("ERROR: no board tab")
            return 1
        page.bring_to_front()

        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        page.wait_for_timeout(800)

        clicked = 0
        for leg in LEGS:
            lid = leg["league_id"]
            print(f"\n=== {leg['sport']} {leg['player']} {leg['prop_type']} O{leg['line']} Goblin ===")
            page.goto(
                f"https://app.prizepicks.com/board?league_id={lid}",
                wait_until="domcontentloaded",
                timeout=45_000,
            )
            page.wait_for_timeout(3000)
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            try:
                _search(page, leg["search"])
            except Exception as e:
                print(f"  search ERR {e}; trying add_leg path")
            page.wait_for_timeout(1500)

            ok = _click_more_less(
                page,
                leg["player"].split()[-1],  # surname often enough after search
                leg["prop_type"],
                leg["line"],
                leg["direction"],
            )
            if not ok:
                # full name retry
                ok = _click_more_less(
                    page, leg["player"], leg["prop_type"], leg["line"], leg["direction"]
                )
            if not ok:
                # fallback to collect_payout_data add_leg
                frame = P.find_prizepicks_frame(page)
                ok = P.add_leg(frame, page, leg, strict_lines=False, require_line=False)

            print(f"  result={'OK' if ok else 'FAIL'}")
            if ok:
                clicked += 1
            page.wait_for_timeout(1500)

        frame = P.find_prizepicks_frame(page)
        slip = P.read_slip(frame, n_legs=clicked, ticket_type="power")
        print(f"\n[BUILD] clicked={clicked}/3")
        if slip:
            print(f"[BUILD] n_selected={slip.get('n_selected')}")
            raw = (slip.get("raw_slip_section") or slip.get("raw_text") or "")[:1000]
            print(raw)
        if clicked == 3:
            print("\nREADY — Power Play built. Hit Submit in the PrizePicks Chrome window.")
            return 0
        print("\nINCOMPLETE — finish missing legs manually in the CDP Chrome board.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
