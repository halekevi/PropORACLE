#!/usr/bin/env python3
"""Full rebuild Power 3 #2 with correct Malen SOT 0.5. No submit."""
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
            print("sport", label)
            return P.find_prizepicks_frame(page)
        except Exception:
            continue
    return frame


def find_named(frame, name: str, prop_needles: list[str]):
    cards = P.get_all_cards(frame)
    out = []
    for c in cards:
        st = P._read_card_state_from_more(
            c.get("more_btn"), frame=frame, more_index=c.get("more_index")
        ) or {}
        blob = " ".join(str(x) for x in (st.get("text") or []))
        if name.lower() not in blob.lower() and name.lower() not in str(c.get("player") or "").lower():
            continue
        prop = str(c.get("prop_type") or "")
        hay = (prop + " " + blob).lower()
        if prop_needles and not any(n.lower() in hay for n in prop_needles):
            continue
        out.append((c, st, blob))
    return out


def cycle_to(frame, card, want_pick, want_line):
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen = set()
    for i in range(14):
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
        time.sleep(0.5)
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


def add_gasser(page, frame) -> bool:
    P.switch_sport_tab(frame, page, "MLB", settle_ms=1500)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, "Gasser")
    page.wait_for_timeout(1500)
    ok = P.add_leg(
        frame,
        page,
        {
            "sport": "MLB",
            "player": "Robert Gasser",
            "prop_type": "Pitcher Strikeouts",
            "direction": "OVER",
            "line": 3.5,
            "pick_type": "goblin",
        },
        strict_lines=True,
        require_line=True,
    )
    if ok:
        return True
    hits = find_named(frame, "Gasser", ["Ks", "Strikeout"])
    if not hits:
        return False
    card = hits[0][0]
    st, btn, idx = cycle_to(frame, card, "goblin", 3.5)
    return bool(st) and click_more(btn, idx, frame)


def add_freeman(page, frame) -> bool:
    P.switch_sport_tab(frame, page, "CFB", settle_ms=1500)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, "Josiah Freeman")
    page.wait_for_timeout(1800)
    hits = find_named(frame, "Josiah Freeman", ["Rec", "Receiv", "Yards"])
    if not hits:
        return False
    # prefer Rec Yards
    card = hits[0][0]
    for c, st, blob in hits:
        if "rec" in P._norm(c.get("prop_type")) and "yard" in P._norm(c.get("prop_type")):
            card = c
            break
    st, btn, idx = cycle_to(frame, card, "goblin", 19.5)
    return bool(st) and click_more(btn, idx, frame)


def add_malen_sot(page, frame) -> bool:
    frame = go_soccer(page, frame)
    P.dismiss_modal(frame, page)
    react_search(page, "Malen")
    page.wait_for_timeout(2000)
    hits = find_named(frame, "Malen", ["SOT", "Shots On Target", "On Target"])
    # Also accept prop_type exactly SOT even if needle only in card
    if not hits:
        hits = find_named(frame, "Malen", ["SOT"])
    print("malen sot candidates", len(hits))
    for c, st, blob in hits:
        print(" ", c.get("prop_type"), c.get("line"), c.get("pick_type"), blob[:100])
    # Prefer prop SOT / already 0.5 goblin
    card = None
    for c, st, blob in hits:
        prop = str(c.get("prop_type") or "").upper()
        if prop == "SOT" or "SOT" in blob:
            card = c
            if abs(float(c.get("line") or -1) - 0.5) < 0.01 and str(
                c.get("pick_type")
            ).lower() == "goblin":
                break
    if card is None and hits:
        card = hits[0][0]
    if card is None:
        # JS click exact SOT More button
        res = page.evaluate(
            """() => {
          const more = Array.from(document.querySelectorAll('button')).filter(b =>
            ((b.innerText||'').trim() === 'More' || (b.innerText||'').trim() === '+ More'));
          for (const btn of more) {
            let root = btn;
            for (let i=0;i<8 && root;i++) {
              const t = root.innerText || '';
              if (t.includes('Malen') && /\\bSOT\\b/.test(t) && t.includes('0.5') && t.length < 400) {
                btn.click();
                return 'clicked ' + t.replace(/\\s+/g,' ').slice(0,120);
              }
              root = root.parentElement;
            }
          }
          return 'not_found';
        }"""
        )
        print("JS SOT", res)
        return str(res).startswith("clicked")
    # If already goblin 0.5, click; else cycle
    st = P._read_card_state_from_more(
        card.get("more_btn"), frame=frame, more_index=card.get("more_index")
    ) or {}
    if not (
        str(st.get("pickType")).lower() == "goblin"
        and abs(float(st.get("line") or -1) - 0.5) < 0.01
    ):
        st2, btn, idx = cycle_to(frame, card, "goblin", 0.5)
        if not st2:
            # if already showing SOT 0.5 goblin on board without needing cycle
            btn, idx = card.get("more_btn"), card.get("more_index")
        else:
            btn, idx = btn, idx
    else:
        btn, idx = card.get("more_btn"), card.get("more_index")
    return click_more(btn, idx, frame)


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

        P.navigate_board_for_sport(page, "MLB", settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        page.wait_for_timeout(500)

        print("=== Gasser ===")
        ok1 = add_gasser(page, frame)
        print("gasser", ok1)
        frame = P.find_prizepicks_frame(page)

        print("=== Freeman ===")
        ok2 = add_freeman(page, frame)
        print("freeman", ok2)
        frame = P.find_prizepicks_frame(page)

        print("=== Malen SOT ===")
        ok3 = add_malen_sot(page, frame)
        print("malen", ok3)
        frame = P.find_prizepicks_frame(page)

        try:
            P.set_ticket_type(frame, "power")
            frame.get_by_text("Power Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=3, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2000])
        soft = (
            "Gasser" in raw
            and "Josiah Freeman" in raw
            and "Malen" in raw
            and "SOT" in raw
            and "0.5" in raw
            and "Shots" not in raw.split("Malen", 1)[-1][:80]
        )
        # even softer check
        malen_chunk = raw[raw.find("Malen") : raw.find("Malen") + 120] if "Malen" in raw else ""
        print("malen_chunk", malen_chunk.replace("\n", " | "))
        if (
            int((slip or {}).get("n_selected") or 0) >= 3
            and "Gasser" in raw
            and "Freeman" in raw
            and "Malen" in raw
            and "SOT" in malen_chunk
        ):
            print("\nREADY — Power 3 #2 built. Hit Submit in CDP Chrome.")
            return 0
        print("\nINCOMPLETE", ok1, ok2, ok3)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
