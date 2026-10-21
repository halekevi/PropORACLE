#!/usr/bin/env python3
"""Continue Power 3 #1: keep Schlittler, add Arike + Dampier. No submit."""
from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

REMAINING = [
    {
        "sport": "WNBA",
        "player": "Arike Ogunbowale",
        "prop_type": "Rebs+Asts",
        "direction": "OVER",
        "line": 4.5,
        "pick_type": "goblin",
        "alt_props": ["Reb+Asts", "Rebs + Asts", "RA", "Rebounds+Assists"],
    },
    {
        "sport": "CFB",
        "player": "Devon Dampier",
        "prop_type": "Pass Yards",
        "direction": "OVER",
        "line": 159.5,
        "pick_type": "goblin",
        "alt_props": ["Passing Yards", "Pass Yds"],
    },
]


def react_fill_search(page, query: str) -> dict:
    return page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return {ok:false, reason:'no_input'};
      // Reveal parent if hidden
      let el = inp;
      for (let i=0;i<6 && el;i++) {
        const s = getComputedStyle(el);
        if (s.display==='none') el.style.display='block';
        if (s.visibility==='hidden') el.style.visibility='visible';
        if (s.opacity==='0') el.style.opacity='1';
        el = el.parentElement;
      }
      inp.focus();
      const proto = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q);
      else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
      inp.dispatchEvent(new Event('change', {bubbles:true}));
      inp.dispatchEvent(new KeyboardEvent('keydown', {key:'Enter', bubbles:true}));
      inp.dispatchEvent(new KeyboardEvent('keyup', {key:'Enter', bubbles:true}));
      const r = inp.getBoundingClientRect();
      return {ok:true, value:inp.value, w:r.width, h:r.height, x:r.x, y:r.y};
    }""",
        query,
    )


def find_name_anywhere(page, name: str) -> list[str]:
    return page.evaluate(
        """(name) => {
      const out = [];
      const walk = document.body.innerText || '';
      if (!walk.includes(name.split(' ').pop())) return out;
      // Collect nearby blocks
      const all = Array.from(document.querySelectorAll('div,button,li,article'));
      for (const n of all) {
        const t = (n.innerText || '').trim();
        if (!t || t.length > 500) continue;
        if (t.includes(name) || t.includes(name.split(' ').slice(-1)[0])) {
          if (t.length > 20 && /\\d\\.\\d/.test(t)) {
            out.push(t.replace(/\\s+/g,' ').slice(0,220));
          }
        }
        if (out.length >= 12) break;
      }
      return out;
    }""",
        name,
    )


def deep_scroll(page, rounds: int = 8) -> None:
    page.evaluate(
        """(rounds) => {
      const scroller = document.querySelector('[class*=board], [class*=Board], main, #root') || document.scrollingElement;
      let y = 0;
      for (let i=0;i<rounds;i++) {
        y += 900;
        if (scroller) scroller.scrollTop = y;
        window.scrollBy(0, 900);
      }
    }""",
        rounds,
    )


def click_exact_goblin(page, player: str, line: float, prop_hints: list[str]) -> str:
    return page.evaluate(
        """([player, line, hints]) => {
      const lineStr = String(line);
      const last = player.split(' ').pop();
      const buttons = Array.from(document.querySelectorAll('button'));
      const more = buttons.filter(b => {
        const t = (b.innerText||'').trim();
        return t === 'More' || t === '+ More';
      });
      for (const btn of more) {
        let root = btn;
        for (let i=0;i<10 && root;i++) {
          const t = root.innerText || '';
          const hasPlayer = t.includes(player) || t.includes(last);
          const hasLine = t.includes(lineStr);
          const hasProp = !hints.length || hints.some(h => t.toLowerCase().includes(h.toLowerCase()));
          if (hasPlayer && hasLine) {
            // Prefer goblin face: green/lower line often already showing
            const locals = Array.from(root.querySelectorAll('button'));
            const hit = locals.find(b => (b.innerText||'').trim() === 'More')
                     || locals.find(b => (b.innerText||'').trim() === '+ More');
            if (hit) {
              hit.click();
              return 'clicked More on card: ' + t.replace(/\\s+/g,' ').slice(0,160);
            }
          }
          root = root.parentElement;
        }
      }
      // Cycle dual-arrow on any matching player card until line appears
      for (const btn of more) {
        let root = btn;
        for (let i=0;i<10 && root;i++) {
          const t = root.innerText || '';
          if (t.includes(player) || t.includes(last)) {
            // click swap arrows if present
            const arrows = Array.from(root.querySelectorAll('button, [role=button], svg'));
            for (const a of arrows) {
              const al = (a.getAttribute('aria-label')||'') + (a.className||'');
              if (/swap|alternate|arrow|chevron|toggle/i.test(al) || /<|>|‹|›|⟷/.test(a.innerText||'')) {
                (a.closest('button')||a).click();
                return 'cycled arrow on ' + last;
              }
            }
          }
          root = root.parentElement;
        }
      }
      return 'not_found';
    }""",
        [player, line, prop_hints],
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
            print("ERROR: no board tab")
            return 1
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)

        # Confirm Schlittler still on slip
        slip0 = P.read_slip(frame, n_legs=1, ticket_type="power")
        raw0 = (slip0 or {}).get("raw_slip_section") or (slip0 or {}).get("raw_text") or ""
        print("[SLIP NOW]", raw0[:500])
        if "Schlittler" not in raw0:
            print("Schlittler missing — re-add first")
            P.switch_sport_tab(frame, page, "MLB", settle_ms=1500)
            frame = P.find_prizepicks_frame(page)
            ok = P.add_leg(
                frame,
                page,
                {
                    "sport": "MLB",
                    "player": "Cam Schlittler",
                    "prop_type": "Pitcher Strikeouts",
                    "direction": "OVER",
                    "line": 3.5,
                    "pick_type": "goblin",
                },
                strict_lines=True,
                require_line=True,
            )
            print("re-add Schlittler", ok)
            try:
                P.set_ticket_type(frame, "power")
            except Exception:
                pass

        active = ""
        for leg in REMAINING:
            sport = leg["sport"]
            print(f"\n=== {sport} {leg['player']} O{leg['line']} ===")
            switched = P.switch_sport_tab(frame, page, sport, settle_ms=2000)
            if switched is not None:
                frame = switched
                active = sport
            P.dismiss_modal(frame, page)
            P.ensure_popular_filter(frame, page)

            # Switch prop filter
            tab = P.prop_type_to_board_filter(leg["prop_type"])
            try:
                P._switch_board_filter(frame, page, tab)
            except Exception as e:
                print("filter err", e)
                for alt in leg.get("alt_props") or []:
                    try:
                        P._switch_board_filter(frame, page, alt)
                        print("alt filter", alt)
                        break
                    except Exception:
                        pass

            # React search
            sr = react_fill_search(page, leg["player"].split()[-1])
            print("[SEARCH]", sr)
            page.wait_for_timeout(2000)

            hits = find_name_anywhere(page, leg["player"])
            print(f"[HITS after search] {len(hits)}")
            for h in hits[:6]:
                print(" ", h)

            if not hits:
                deep_scroll(page, 10)
                page.wait_for_timeout(800)
                hits = find_name_anywhere(page, leg["player"])
                print(f"[HITS after scroll] {len(hits)}")
                for h in hits[:6]:
                    print(" ", h)

            # Try strict add_leg
            ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
            if not ok:
                # Clear search, scroll board hard, try again
                react_fill_search(page, "")
                page.wait_for_timeout(500)
                for _ in range(6):
                    deep_scroll(page, 3)
                    page.wait_for_timeout(400)
                    # Try cycle+click via JS
                    res = click_exact_goblin(
                        page,
                        leg["player"],
                        leg["line"],
                        [leg["prop_type"]] + list(leg.get("alt_props") or []),
                    )
                    print("[JS cycle/click]", res)
                    if str(res).startswith("clicked"):
                        ok = True
                        break
                    if "cycled" in str(res):
                        page.wait_for_timeout(500)
                        res2 = click_exact_goblin(
                            page,
                            leg["player"],
                            leg["line"],
                            [leg["prop_type"]] + list(leg.get("alt_props") or []),
                        )
                        print("[JS after cycle]", res2)
                        if str(res2).startswith("clicked"):
                            ok = True
                            break
                if not ok:
                    ok = P.add_leg(
                        frame, page, leg, strict_lines=True, require_line=True
                    )

            print("result", "OK" if ok else "FAIL")
            try:
                P.set_ticket_type(frame, "power")
            except Exception:
                pass
            page.wait_for_timeout(800)

        frame = P.find_prizepicks_frame(page)
        slip = P.read_slip(frame, n_legs=3, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or (slip or {}).get("raw_text") or ""
        print("\n[FINAL SLIP]")
        print(raw[:2000])
        n = int((slip or {}).get("n_selected") or 0)
        need = ("Schlittler" in raw) and ("Ogunbowale" in raw or "Arike" in raw) and (
            "Dampier" in raw
        )
        if n >= 3 and need:
            print("\nREADY — hit Submit in CDP Chrome.")
            return 0
        print("\nINCOMPLETE")
        # dump board filters for WNBA help
        filters = page.evaluate(
            """() => Array.from(document.querySelectorAll('button, [role=tab], [role=button]'))
              .map(b => (b.innerText||'').trim())
              .filter(t => t && t.length < 30)
              .slice(0, 80)"""
        )
        print("[FILTERS SAMPLE]", json.dumps(filters[:40]))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
