#!/usr/bin/env python3
"""Build Power 3 #1 via CDP (sport tabs, strict lines). Does NOT submit."""
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
        "player": "Cam Schlittler",
        "prop_type": "Pitcher Strikeouts",
        "direction": "OVER",
        "line": 3.5,
        "pick_type": "goblin",
    },
    {
        "sport": "WNBA",
        "player": "Arike Ogunbowale",
        "prop_type": "Rebs+Asts",
        "direction": "OVER",
        "line": 4.5,
        "pick_type": "goblin",
    },
    {
        "sport": "CFB",
        "player": "Devon Dampier",
        "prop_type": "Pass Yards",
        "direction": "OVER",
        "line": 159.5,
        "pick_type": "goblin",
    },
]


def _probe_player(page, name: str) -> list[str]:
    """Return card text snippets that mention name."""
    return page.evaluate(
        """(name) => {
      const out = [];
      const nodes = Array.from(document.querySelectorAll('button, [class*="projection"], [class*="card"]'));
      for (const n of nodes) {
        const t = (n.innerText || '').trim();
        if (t && t.includes(name) && t.length < 400) out.push(t.replace(/\\s+/g, ' ').slice(0, 180));
        if (out.length >= 8) break;
      }
      return out;
    }""",
        name,
    )


def _force_open_search(page, frame, query: str) -> bool:
    """Reveal search if needed, then fill query."""
    # Click common search triggers
    for sel in [
        "button[aria-label*='earch']",
        "[data-testid*='search']",
        "svg[data-testid*='Search']",
        "button:has(svg)",
    ]:
        try:
            loc = frame.locator(sel).first
            if loc.count():
                loc.click(timeout=800, force=True)
                page.wait_for_timeout(300)
        except Exception:
            pass
    # Also try clicking placeholder-adjacent icons via JS
    page.evaluate(
        """() => {
      const labels = ['Search', 'search'];
      for (const el of document.querySelectorAll('button, [role=button], a')) {
        const al = (el.getAttribute('aria-label') || '') + ' ' + (el.innerText || '');
        if (/search/i.test(al) && al.length < 40) { el.click(); return true; }
      }
      // MUI search adornment
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (inp) {
        const wrap = inp.closest('.MuiInputBase-root, .search, form') || inp.parentElement;
        if (wrap) {
          const btn = wrap.querySelector('button, svg');
          if (btn) { (btn.closest('button') || btn).dispatchEvent(new MouseEvent('click', {bubbles:true})); }
        }
        inp.focus();
      }
      return false;
    }"""
    )
    page.wait_for_timeout(400)
    for sel in [
        "input[placeholder*='Search']",
        "input[aria-label='search']",
        "input[aria-label*='Search']",
        "input[type='search']",
    ]:
        box = frame.locator(sel).first
        if box.count() == 0:
            continue
        try:
            box.click(force=True, timeout=1500)
            box.fill("", force=True)
            box.fill(query, force=True)
            try:
                box.press("Enter")
            except Exception:
                pass
            page.wait_for_timeout(1800)
            print(f"  [SEARCH] filled {sel!r} -> {query!r}")
            return True
        except Exception as e:
            print(f"  [SEARCH] {sel} fail: {e}")
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

        # Hard reset once onto MLB so slip starts clean
        P.navigate_board_for_sport(page, "MLB", settle_ms=2500)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        page.wait_for_timeout(800)
        active = "MLB"

        clicked = 0
        for leg in LEGS:
            sport = leg["sport"]
            print(
                f"\n=== {sport} {leg['player']} {leg['prop_type']} "
                f"O{leg['line']} Goblin ==="
            )
            if sport != active:
                switched = P.switch_sport_tab(frame, page, sport, settle_ms=2000)
                if switched is not None:
                    frame = switched
                    active = sport
                else:
                    print(f"  WARN: sport tab miss for {sport}")
            P.dismiss_modal(frame, page)
            P.ensure_popular_filter(frame, page)

            # Probe before add
            hits = _probe_player(page, leg["player"].split()[-1])
            print(f"  [PROBE] {len(hits)} snippets for surname")
            for h in hits[:3]:
                print(f"    {h}")

            ok = P.add_leg(
                frame, page, leg, strict_lines=True, require_line=True
            )
            if not ok:
                print("  [RETRY] open search + add_leg")
                _force_open_search(page, frame, leg["player"].split()[-1])
                page.wait_for_timeout(1200)
                hits = _probe_player(page, leg["player"].split()[-1])
                print(f"  [PROBE after search] {len(hits)} snippets")
                for h in hits[:5]:
                    print(f"    {h}")
                ok = P.add_leg(
                    frame, page, leg, strict_lines=True, require_line=True
                )

            print(f"  result={'OK' if ok else 'FAIL'}")
            if ok:
                clicked += 1
            page.wait_for_timeout(1000)
            # Keep Power selected (sport switch can flip UI)
            try:
                P.set_ticket_type(frame, "power")
            except Exception:
                pass

        frame = P.find_prizepicks_frame(page)
        slip = P.read_slip(frame, n_legs=clicked, ticket_type="power")
        print(f"\n[BUILD] clicked={clicked}/3")
        if slip:
            print(f"[BUILD] n_selected={slip.get('n_selected')}")
            raw = (slip.get("raw_slip_section") or slip.get("raw_text") or "")[:1500]
            print(raw)
        if clicked == 3 and slip and int(slip.get("n_selected") or 0) >= 3:
            print("\nREADY — Power Play built. Hit Submit in the PrizePicks Chrome window.")
            return 0
        print("\nINCOMPLETE — check CDP Chrome board / finish missing legs.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
