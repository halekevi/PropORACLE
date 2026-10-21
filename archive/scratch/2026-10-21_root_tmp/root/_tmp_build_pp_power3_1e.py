#!/usr/bin/env python3
"""Fix leg 3: remove Hammond, add Dampier Pass Yards 159.5 Goblin. No submit."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"


def react_fill_search(page, query: str) -> dict:
    return page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return {ok:false};
      const proto = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
      inp.dispatchEvent(new Event('change', {bubbles:true}));
      return {ok:true, value:inp.value};
    }""",
        query,
    )


def remove_slip_player(page, name: str) -> str:
    return page.evaluate(
        """(name) => {
      // Find slip section text containing name, click nearby remove/X
      const all = Array.from(document.querySelectorAll('button, [role=button], svg'));
      // Prefer buttons near Current Lineup
      const body = document.body.innerText || '';
      if (!body.includes(name)) return 'not_on_slip';
      // Walk slip cards
      const nodes = Array.from(document.querySelectorAll('div, li, article, section'));
      for (const n of nodes) {
        const t = (n.innerText || '');
        if (!t.includes(name) || t.length > 600) continue;
        if (!/Current Lineup|Players Selected|More|Less/.test(t) && !t.includes('CFB')) {
          // still ok if short slip card
        }
        // look for X / remove in this subtree
        const btns = Array.from(n.querySelectorAll('button, [role=button], svg, [aria-label]'));
        for (const b of btns) {
          const al = ((b.getAttribute && b.getAttribute('aria-label')) || '') + (b.className || '');
          const txt = (b.innerText || '').trim();
          if (/remove|delete|close|clear player/i.test(al) || txt === '×' || txt === 'X' || txt === '✕') {
            (b.closest('button') || b).click();
            return 'removed_via ' + (al || txt).slice(0,40);
          }
        }
      }
      // Fallback: click any aria-label remove near name text node
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
      let el;
      while ((el = walker.nextNode())) {
        if ((el.innerText || '').includes(name) && (el.innerText || '').length < 350) {
          const rem = el.querySelector("[aria-label*='emove'], [aria-label*='elete'], [aria-label*='lose']");
          if (rem) { rem.click(); return 'removed aria'; }
        }
      }
      return 'remove_fail';
    }""",
        name,
    )


def cycle_dampier_pass_goblin(page, max_swaps: int = 12) -> dict:
    """On Dampier Pass Yards card, swap until 159.5 then click More."""
    return page.evaluate(
        """(maxSwaps) => {
      const wantLine = '159.5';
      const findCard = () => {
        const moreBtns = Array.from(document.querySelectorAll('button')).filter(b => {
          const t = (b.innerText||'').trim();
          return t === 'More' || t === '+ More';
        });
        for (const btn of moreBtns) {
          let root = btn;
          for (let i=0;i<10 && root;i++) {
            const t = root.innerText || '';
            if (t.includes('Dampier') && /Pass\\s*Yards/i.test(t)) {
              return {root, btn, text: t.replace(/\\s+/g,' ').slice(0,240)};
            }
            root = root.parentElement;
          }
        }
        return null;
      };
      let card = findCard();
      if (!card) return {ok:false, reason:'no_dampier_pass_card'};
      const clickSwap = (root) => {
        // dual arrow / chevron buttons inside card
        const cands = Array.from(root.querySelectorAll('button, [role=button]'));
        for (const b of cands) {
          const t = (b.innerText||'').trim();
          const al = (b.getAttribute('aria-label')||'') + ' ' + (b.className||'');
          if (t === 'More' || t === 'Less' || t === '+ More') continue;
          if (/swap|alternate|toggle|next|prev|arrow|chevron|projection/i.test(al)) {
            b.click(); return 'aria';
          }
          // often empty icon buttons
          if (!t && b.querySelector('svg') && b.getBoundingClientRect().width < 48) {
            b.click(); return 'icon';
          }
        }
        // PP often has left/right triangle as siblings of the line
        for (const b of cands) {
          const t = (b.innerText||'').trim();
          if (t === '<' || t === '>' || t === '‹' || t === '›' || t === '←' || t === '→') {
            b.click(); return 'chevron-text';
          }
        }
        return null;
      };
      const steps = [];
      for (let s=0; s<maxSwaps; s++) {
        card = findCard();
        if (!card) return {ok:false, reason:'lost_card', steps};
        const t = card.root.innerText || '';
        steps.push(t.replace(/\\s+/g,' ').slice(0,120));
        if (t.includes(wantLine)) {
          // click More
          const locals = Array.from(card.root.querySelectorAll('button'));
          const more = locals.find(b => (b.innerText||'').trim() === 'More')
                    || locals.find(b => (b.innerText||'').trim() === '+ More');
          if (more) {
            more.click();
            return {ok:true, steps, how:'more'};
          }
          return {ok:false, reason:'no_more', steps};
        }
        const how = clickSwap(card.root);
        if (!how) return {ok:false, reason:'no_swap_control', steps};
        steps.push('swap:'+how);
      }
      return {ok:false, reason:'max_swaps', steps};
    }""",
        max_swaps,
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

        # Remove wrong Hammond leg
        r = remove_slip_player(page, "Hammond")
        print("[REMOVE Hammond]", r)
        page.wait_for_timeout(800)
        if r == "remove_fail" or r == "not_on_slip":
            # try Clear then rebuild Schlittler+Arike — avoid if possible
            print("trying slip Clear of just wrong player via UI Clear+rebuild not yet")

        slip = P.read_slip(frame, n_legs=2, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("[SLIP after remove]", raw[:700])

        if "Hammond" in raw:
            # Click X via Playwright locators near Hammond text
            try:
                loc = frame.get_by_text("Will Hammond", exact=False).first
                # parent card remove
                card = loc.locator("xpath=ancestor::*[contains(., 'Pass Yards') or contains(., 'Pass')][1]")
                for sel in [
                    "button[aria-label*='emove']",
                    "button[aria-label*='elete']",
                    "button[aria-label*='lose']",
                    "button:has-text('×')",
                ]:
                    try:
                        card.locator(sel).first.click(timeout=800, force=True)
                        print("removed via playwright", sel)
                        break
                    except Exception:
                        pass
            except Exception as e:
                print("pw remove err", e)
            page.wait_for_timeout(800)
            raw = (P.read_slip(frame, n_legs=2, ticket_type="power") or {}).get(
                "raw_slip_section"
            ) or ""
            print("[SLIP retry]", raw[:700])

        # Ensure CFB + search Dampier
        switched = P.switch_sport_tab(frame, page, "CFB", settle_ms=1500)
        if switched is not None:
            frame = switched
        P.dismiss_modal(frame, page)
        react_fill_search(page, "Dampier")
        page.wait_for_timeout(1800)

        # If Hammond still selected, clear entire slip and rebuild all 3
        raw = (P.read_slip(frame, n_legs=3, ticket_type="power") or {}).get(
            "raw_slip_section"
        ) or ""
        if "Hammond" in raw:
            print("Hammond still present — clear all and rebuild")
            P.clear_slip(frame)
            _, frame = P.verify_slip_empty(frame, page)
            P.set_ticket_type(frame, "power")
            for leg in [
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
            ]:
                P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1500)
                frame = P.find_prizepicks_frame(page)
                react_fill_search(page, leg["player"].split()[-1])
                page.wait_for_timeout(1200)
                ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
                print("rebuild", leg["player"], ok)
            P.switch_sport_tab(frame, page, "CFB", settle_ms=1500)
            frame = P.find_prizepicks_frame(page)
            react_fill_search(page, "Dampier")
            page.wait_for_timeout(1500)

        # Cycle Dampier Pass Yards to 159.5 Goblin and More-click
        # May need multiple evaluate rounds with waits between swaps
        ok = False
        last = None
        for attempt in range(14):
            last = cycle_dampier_pass_goblin(page, max_swaps=1)
            print(f"[CYCLE {attempt}]", last)
            if last.get("ok"):
                ok = True
                break
            if last.get("reason") == "no_dampier_pass_card":
                react_fill_search(page, "Dampier")
                page.wait_for_timeout(1000)
            page.wait_for_timeout(450)

        if not ok:
            # Try add_leg with player name forced via search cards
            # Patch: resolve using fuzzy name from card text
            print("fallback add_leg")
            ok = P.add_leg(
                frame,
                page,
                {
                    "sport": "CFB",
                    "player": "Devon Dampier",
                    "prop_type": "Pass Yards",
                    "direction": "OVER",
                    "line": 159.5,
                    "pick_type": "goblin",
                },
                strict_lines=True,
                require_line=True,
            )

        try:
            P.set_ticket_type(frame, "power")
        except Exception:
            pass
        # Click Power Play text if Flex was selected
        try:
            frame.get_by_text("Power Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        page.wait_for_timeout(800)
        slip = P.read_slip(frame, n_legs=3, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or (slip or {}).get("raw_text") or ""
        print("\n[FINAL]")
        print(raw[:1800])
        good = (
            "Schlittler" in raw
            and ("Ogunbowale" in raw or "Arike" in raw)
            and "Dampier" in raw
            and "Hammond" not in raw
            and "159.5" in raw
        )
        if good:
            print("\nREADY — Power 3 built. Hit Submit in CDP Chrome.")
            return 0
        print("\nINCOMPLETE")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
