#!/usr/bin/env python3
"""Fix Flex 6: remove wrong Bishop, add Goblin 69.5 + Gasser Ks 3.5. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"


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


def card_blob(frame, c):
    st = P._read_card_state_from_more(
        c.get("more_btn"), frame=frame, more_index=c.get("more_index")
    ) or {}
    blob = " ".join(str(x) for x in (st.get("text") or []))
    return st, blob


def find_card(frame, name: str, prop_ok) -> dict | None:
    for c in P.get_all_cards(frame):
        st, blob = card_blob(frame, c)
        pname = str(c.get("player") or "")
        if name.lower() not in blob.lower() and name.lower() not in pname.lower():
            continue
        if prop_ok(str(c.get("prop_type") or ""), blob):
            return c
    return None


def cycle_exact(frame, card, want_pick, want_line, max_swaps=20):
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen = set()
    for i in range(max_swaps):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        print(f"  face[{i}] {pt} {line}")
        key = (pt, line)
        if key in seen and i > 0:
            return None, btn, idx
        seen.add(key)
        try:
            if pt == want_pick and abs(float(line or -1) - float(want_line)) < 0.01:
                return st, btn, idx
        except Exception:
            pass
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            return None, btn, idx
        time.sleep(0.5)
    return None, btn, idx


def click_more(btn, idx, frame) -> bool:
    try:
        if btn is not None:
            try:
                btn.click(timeout=1200, no_wait_after=True)
            except Exception:
                btn.click(force=True, timeout=2000, no_wait_after=True)
            time.sleep(0.45)
            return True
        return bool(
            P._eval_more_js(
                "el => { el.click(); return true; }", None, frame=frame, more_index=idx
            )
        )
    except Exception as e:
        print("click", e)
        return False


def remove_bishop_114(page, frame) -> None:
    """Click Less on Bishop 114.5 to remove from slip, or X if present."""
    # Prefer Less on the Bishop slip row / board card
    res = page.evaluate(
        """() => {
      const less = Array.from(document.querySelectorAll('button')).filter(b =>
        ((b.innerText||'').trim() === 'Less'));
      for (const btn of less) {
        let root = btn;
        for (let i=0;i<10 && root;i++) {
          const t = (root.innerText || '');
          if (t.includes('Bishop') && (t.includes('114.5') || t.includes('Rush'))) {
            btn.click();
            return 'less ' + t.replace(/\\s+/g,' ').slice(0,120);
          }
          root = root.parentElement;
        }
      }
      // X / remove icons near Bishop
      const all = Array.from(document.querySelectorAll('button,[role=button],svg'));
      for (const el of all) {
        let root = el;
        for (let i=0;i<8 && root;i++) {
          const t = (root.innerText || '');
          if (t.includes('Bishop') && t.includes('114.5') && t.length < 400) {
            const x = root.querySelector('button[aria-label*="emove"], button[aria-label*="Close"], [data-testid*="remove"]');
            if (x) { x.click(); return 'x'; }
          }
          root = root.parentElement;
        }
      }
      return 'no';
    }"""
    )
    print("remove bishop", res)
    page.wait_for_timeout(800)


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

        slip0 = P.read_slip(frame, n_legs=6, ticket_type="flex")
        raw0 = (slip0 or {}).get("raw_slip_section") or ""
        print("[BEFORE]", raw0[:800])

        if "Bishop" in raw0 and "114.5" in raw0:
            print("\n=== Remove Bishop 114.5 ===")
            remove_bishop_114(page, frame)
            frame = P.find_prizepicks_frame(page)
            # also try Less via collect if still there
            slip1 = P.read_slip(frame, n_legs=6, ticket_type="flex")
            raw1 = (slip1 or {}).get("raw_slip_section") or ""
            if "114.5" in raw1 and "Bishop" in raw1:
                # Click Less on TENN RB board card after search
                P.switch_sport_tab(frame, page, "CFB", settle_ms=1200)
                frame = P.find_prizepicks_frame(page)
                react_search(page, "Bishop")
                page.wait_for_timeout(1500)
                page.evaluate(
                    """() => {
                  const less = Array.from(document.querySelectorAll('button')).filter(b =>
                    ((b.innerText||'').trim() === 'Less'));
                  for (const btn of less) {
                    let root = btn;
                    for (let i=0;i<8 && root;i++) {
                      const t = root.innerText || '';
                      if (t.includes('Bishop') || t.includes('TENN - RB')) {
                        if (t.includes('114.5') || t.includes('Rush')) {
                          btn.click(); return true;
                        }
                      }
                      root = root.parentElement;
                    }
                  }
                  return false;
                }"""
                )
                page.wait_for_timeout(700)

        # Add Bishop Goblin 69.5
        print("\n=== Bishop Rush Goblin 69.5 ===")
        P.switch_sport_tab(frame, page, "CFB", settle_ms=1400)
        frame = P.find_prizepicks_frame(page)
        react_search(page, "Bishop")
        page.wait_for_timeout(1800)
        card = find_card(
            frame,
            "Bishop",
            lambda prop, blob: ("Rush" in prop or "Rush Yards" in blob)
            and "Rec" not in prop
            and "Long" not in prop
            and "TD" not in prop,
        )
        if card is None:
            card = find_card(
                frame,
                "TENN - RB",
                lambda prop, blob: "Rush" in prop and "Long" not in prop and "TD" not in prop,
            )
        b_ok = False
        if card:
            print("bishop card", card.get("prop_type"), card.get("line"), card.get("pick_type"))
            # Prefer already-correct face from get_all_cards line if goblin 69.5
            if (
                str(card.get("pick_type") or "").lower() == "goblin"
                and abs(float(card.get("line") or -1) - 69.5) < 0.01
            ):
                b_ok = click_more(card.get("more_btn"), card.get("more_index"), frame)
            else:
                st, btn, idx = cycle_exact(frame, card, "goblin", 69.5)
                if st:
                    b_ok = click_more(btn, idx, frame)
        print("bishop", b_ok)

        # Add Gasser — click face if already goblin 3.5, no bad cycle
        print("\n=== Gasser Ks Goblin 3.5 ===")
        P.switch_sport_tab(frame, page, "MLB", settle_ms=1400)
        frame = P.find_prizepicks_frame(page)
        react_search(page, "Gasser")
        page.wait_for_timeout(1800)
        g_ok = False
        # Prefer exact parsed card
        for c in P.get_all_cards(frame):
            if "Gasser" not in str(c.get("player") or ""):
                continue
            prop = str(c.get("prop_type") or "")
            if prop not in ("Ks", "Strikeouts", "Pitcher Strikeouts") and "Ks" not in prop:
                continue
            if "Hit" in prop or "Walk" in prop:
                continue
            print("gasser card", prop, c.get("line"), c.get("pick_type"))
            if (
                str(c.get("pick_type") or "").lower() == "goblin"
                and abs(float(c.get("line") or -1) - 3.5) < 0.01
            ):
                g_ok = click_more(c.get("more_btn"), c.get("more_index"), frame)
                break
            st, btn, idx = cycle_exact(frame, c, "goblin", 3.5)
            if st:
                g_ok = click_more(btn, idx, frame)
            break
        if not g_ok:
            # JS click More on Gasser Ks 3.5
            res = page.evaluate(
                """() => {
              const more = Array.from(document.querySelectorAll('button')).filter(b =>
                ((b.innerText||'').trim() === 'More'));
              for (const btn of more) {
                let root = btn;
                for (let i=0;i<8 && root;i++) {
                  const t = root.innerText || '';
                  if (t.includes('Gasser') && /\\bKs\\b/.test(t) && t.includes('3.5')
                      && !t.includes('Hits') && !t.includes('Walk') && t.length < 350) {
                    btn.click(); return 'ok ' + t.replace(/\\s+/g,' ').slice(0,100);
                  }
                  root = root.parentElement;
                }
              }
              return 'no';
            }"""
            )
            print("js gasser", res)
            g_ok = str(res).startswith("ok")
        print("gasser", g_ok)

        try:
            P.set_ticket_type(frame, "flex")
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        frame = P.find_prizepicks_frame(page)
        slip = P.read_slip(frame, n_legs=6, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2500])
        n = int((slip or {}).get("n_selected") or 0)
        names_ok = (
            "Schlittler" in raw
            and "Gasser" in raw
            and "Dampier" in raw
            and "Bishop" in raw
            and "Malen" in raw
            and ("Ogunbowale" in raw or "Arike" in raw)
        )
        bishop_ok = "69.5" in raw and "Bishop" in raw and "114.5" not in raw
        malen_ok = "SOT" in raw or ("0.5" in raw and "Malen" in raw)
        if n >= 6 and names_ok and bishop_ok:
            print("\nREADY — Flex 6 built. Hit Submit in CDP Chrome.")
            if not malen_ok:
                print("NOTE: verify Malen is SOT 0.5")
            return 0
        print("\nINCOMPLETE", n, names_ok, "bishop69", bishop_ok, "g", g_ok, "b", b_ok)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
