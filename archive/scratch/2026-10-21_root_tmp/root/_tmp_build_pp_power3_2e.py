#!/usr/bin/env python3
"""Keep Malen SOT 0.5; add Gasser Ks 3.5 + Freeman Rec 19.5."""
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


def find_ks_gasser(frame):
    cards = P.get_all_cards(frame)
    for c in cards:
        st = P._read_card_state_from_more(
            c.get("more_btn"), frame=frame, more_index=c.get("more_index")
        ) or {}
        blob = " ".join(str(x) for x in (st.get("text") or []))
        prop = str(c.get("prop_type") or "")
        if "Gasser" not in blob and "Gasser" not in str(c.get("player") or ""):
            continue
        if prop in ("Ks", "Strikeouts") or "Ks" in blob or "Strikeout" in blob:
            # exclude Hits Allowed etc
            if "Hit" in prop or "Walk" in prop or "Earned" in prop or "ERA" in prop:
                continue
            if "Hit" in blob and "Ks" not in blob:
                continue
            return c
    return None


def find_freeman_rec(frame):
    cards = P.get_all_cards(frame)
    for c in cards:
        st = P._read_card_state_from_more(
            c.get("more_btn"), frame=frame, more_index=c.get("more_index")
        ) or {}
        blob = " ".join(str(x) for x in (st.get("text") or []))
        if "Josiah Freeman" not in blob:
            continue
        prop = str(c.get("prop_type") or "")
        if "Rec" in prop or "Receiv" in prop or "Rec Yards" in blob:
            if "TD" in prop or "Anytime" in blob:
                continue
            return c
    return None


def cycle_exact(frame, card, want_pick, want_line, max_swaps=16):
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen = set()
    for i in range(max_swaps):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        blob = " ".join(str(x) for x in (st.get("text") or []))
        print(f"  face[{i}] {pt} {line} | {blob[:90]}")
        key = (pt, line)
        if key in seen and i > 0:
            print("  wrapped")
            return None, btn, idx
        seen.add(key)
        if pt == want_pick and abs(float(line or -1) - float(want_line)) < 0.01:
            return st, btn, idx
        ok = P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop="")
        if not ok:
            print("  swap fail")
            return None, btn, idx
        time.sleep(0.55)
        # rebind by player from blob
    return None, btn, idx


def click_more(btn, idx, frame) -> bool:
    try:
        if btn is not None:
            try:
                btn.click(timeout=1200, no_wait_after=True)
            except Exception:
                btn.click(force=True, timeout=2000, no_wait_after=True)
            time.sleep(0.4)
            return True
        return bool(
            P._eval_more_js(
                "el => { el.click(); return true; }", None, frame=frame, more_index=idx
            )
        )
    except Exception as e:
        print("click", e)
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
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)

        slip = P.read_slip(frame, n_legs=1, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("[SLIP]", raw[:400])
        if "0.5SOT" not in raw.replace(" ", "") and not (
            "Malen" in raw and "SOT" in raw and "0.5" in raw
        ):
            print("Malen SOT missing — abort")
            return 2

        # Gasser
        print("\n=== Gasser Ks 3.5 ===")
        P.switch_sport_tab(frame, page, "MLB", settle_ms=1600)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        react_search(page, "Gasser")
        page.wait_for_timeout(1800)
        card = find_ks_gasser(frame)
        if card is None:
            print("no gasser ks card")
            g_ok = False
        else:
            print("card", card.get("player"), card.get("prop_type"), card.get("line"))
            st, btn, idx = cycle_exact(frame, card, "goblin", 3.5)
            g_ok = bool(st) and click_more(btn, idx, frame)
            # if 3.5 gone, try 4.5 as fallback? No — stick to card plan
        print("gasser", g_ok)

        # Freeman
        print("\n=== Freeman Rec 19.5 ===")
        P.switch_sport_tab(frame, page, "CFB", settle_ms=1600)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        react_search(page, "Josiah Freeman")
        page.wait_for_timeout(2000)
        probe = page.evaluate(
            """() => {
          const t = document.body.innerText || '';
          const i = t.indexOf('Josiah Freeman');
          return i < 0 ? 'missing' : t.slice(i, i+140).replace(/\\s+/g,' ');
            }"""
        )
        print("probe", probe)
        card = find_freeman_rec(frame)
        f_ok = False
        if card is None:
            print("no freeman rec")
        else:
            print("card", card.get("player"), card.get("prop_type"), card.get("line"))
            st, btn, idx = cycle_exact(frame, card, "goblin", 19.5)
            f_ok = bool(st) and click_more(btn, idx, frame)
        print("freeman", f_ok)

        try:
            P.set_ticket_type(frame, "power")
            frame.get_by_text("Power Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=3, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2000])
        if (
            int((slip or {}).get("n_selected") or 0) >= 3
            and "Gasser" in raw
            and "Freeman" in raw
            and "Malen" in raw
            and "SOT" in raw
        ):
            print("\nREADY — Power 3 #2 built. Hit Submit in CDP Chrome.")
            return 0
        print("\nINCOMPLETE")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
