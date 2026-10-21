#!/usr/bin/env python3
"""Full Power 3 #2 rebuild. No submit."""
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
        if name.lower() not in blob.lower() and name.lower() not in str(c.get("player") or "").lower():
            continue
        if prop_ok(str(c.get("prop_type") or ""), blob):
            return c
    return None


def cycle_exact(frame, card, want_pick, want_line, max_swaps=18):
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
        if pt == want_pick and abs(float(line or -1) - float(want_line)) < 0.01:
            return st, btn, idx
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            return None, btn, idx
        time.sleep(0.55)
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


def go_soccer(page, frame):
    page.evaluate(
        """() => {
      const row = Array.from(document.querySelectorAll('div,nav')).find(el => {
        const t = el.innerText || '';
        return t.includes('NFL') && t.includes('CFB') && t.includes('MLB') && el.scrollWidth > el.clientWidth;
      });
      if (row) row.scrollLeft = row.scrollWidth;
    }"""
    )
    page.wait_for_timeout(300)
    for label in ("Soccer", "SOCCER"):
        try:
            frame.get_by_text(label, exact=True).first.click(timeout=2500)
            page.wait_for_timeout(1600)
            return P.find_prizepicks_frame(page)
        except Exception:
            continue
    return frame


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

        P.navigate_board_for_sport(page, "MLB", settle_ms=2200)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        page.wait_for_timeout(500)

        # 1 Gasser
        print("=== Gasser ===")
        P.switch_sport_tab(frame, page, "MLB", settle_ms=1200)
        frame = P.find_prizepicks_frame(page)
        react_search(page, "Gasser")
        page.wait_for_timeout(1800)
        card = find_card(
            frame,
            "Gasser",
            lambda prop, blob: (
                (prop in ("Ks", "Strikeouts") or " Ks" in f" {blob}" or blob.endswith("Ks"))
                and "Hit" not in prop
                and "Walk" not in prop
                and "Earned" not in prop
            ),
        )
        g_ok = False
        if card:
            print("gasser card", card.get("prop_type"), card.get("line"), card.get("pick_type"))
            st, btn, idx = cycle_exact(frame, card, "goblin", 3.5)
            if st:
                g_ok = click_more(btn, idx, frame)
            else:
                # board may have moved — take closest goblin Ks under 5
                print("3.5 miss — dump faces already printed")
        print("gasser", g_ok)

        # 2 Freeman
        print("=== Freeman ===")
        P.switch_sport_tab(frame, page, "CFB", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)
        react_search(page, "Josiah Freeman")
        page.wait_for_timeout(2000)
        card = find_card(
            frame,
            "Josiah Freeman",
            lambda prop, blob: (
                ("Rec" in prop or "Receiv" in prop or "Rec Yards" in blob)
                and "TD" not in prop
                and "Anytime" not in blob
            ),
        )
        f_ok = False
        if card:
            print("freeman card", card.get("prop_type"), card.get("line"), card.get("pick_type"))
            st, btn, idx = cycle_exact(frame, card, "goblin", 19.5)
            if st:
                f_ok = click_more(btn, idx, frame)
        else:
            print("freeman missing on board")
        print("freeman", f_ok)

        # 3 Malen SOT — click the already-goblin 0.5 face
        print("=== Malen SOT ===")
        frame = go_soccer(page, frame)
        react_search(page, "Malen")
        page.wait_for_timeout(2000)
        card = find_card(
            frame,
            "Malen",
            lambda prop, blob: prop.upper() == "SOT" or " SOT" in f" {blob}" or "SOT" in blob,
        )
        m_ok = False
        if card:
            print("malen card", card.get("prop_type"), card.get("line"), card.get("pick_type"))
            st, _ = card_blob(frame, card)
            if not (
                str(st.get("pickType")).lower() == "goblin"
                and abs(float(st.get("line") or -1) - 0.5) < 0.01
            ):
                st2, btn, idx = cycle_exact(frame, card, "goblin", 0.5)
                if st2:
                    m_ok = click_more(btn, idx, frame)
            else:
                m_ok = click_more(card.get("more_btn"), card.get("more_index"), frame)
        else:
            # JS fallback
            res = page.evaluate(
                """() => {
              const more = Array.from(document.querySelectorAll('button')).filter(b =>
                ((b.innerText||'').trim() === 'More'));
              for (const btn of more) {
                let root = btn;
                for (let i=0;i<8 && root;i++) {
                  const t = root.innerText || '';
                  if (t.includes('Malen') && /\\bSOT\\b/.test(t) && t.includes('0.5') && t.length < 350) {
                    btn.click(); return 'ok ' + t.replace(/\\s+/g,' ').slice(0,100);
                  }
                  root = root.parentElement;
                }
              }
              return 'no';
            }"""
            )
            print("js", res)
            m_ok = str(res).startswith("ok")
        print("malen", m_ok)

        try:
            P.set_ticket_type(frame, "power")
            frame.get_by_text("Power Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=3, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2000])
        n = int((slip or {}).get("n_selected") or 0)
        if n >= 3 and "Gasser" in raw and "Freeman" in raw and "Malen" in raw and "SOT" in raw:
            print("\nREADY — Power 3 #2 built. Hit Submit in CDP Chrome.")
            return 0
        print("\nINCOMPLETE", g_ok, f_ok, m_ok)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
