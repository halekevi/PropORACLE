#!/usr/bin/env python3
"""Continue Power 3 #2: keep Gasser, add Freeman + Malen."""
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


def find_by_full_name(frame, must_include: str, prop_hints: list[str]):
    cards = P.get_all_cards(frame)
    hits = []
    for c in cards:
        st = P._read_card_state_from_more(
            c.get("more_btn"), frame=frame, more_index=c.get("more_index")
        ) or {}
        blob = " ".join(str(x) for x in (st.get("text") or []))
        pname = str(c.get("player") or "")
        if must_include.lower() not in blob.lower() and must_include.lower() not in pname.lower():
            continue
        cp = (c.get("prop_type") or "") + " " + blob
        if prop_hints and not any(h.lower() in cp.lower() for h in prop_hints):
            continue
        hits.append(c)
    return hits


def cycle_to(frame, card, want_pick, want_line, accept_nearest=True):
    btn = card.get("more_btn")
    idx = card.get("more_index")
    seen = set()
    goblins = []
    for i in range(14):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        print(f"  face[{i}] {pt} {line} text={st.get('text')}")
        key = (pt, line)
        if key in seen and i > 0:
            break
        seen.add(key)
        if pt == want_pick:
            goblins.append(st)
            if want_line is not None and abs(float(line or -1) - float(want_line)) < 0.01:
                return st, btn, idx
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.5)
    if accept_nearest and goblins:
        best = min(
            goblins,
            key=lambda s: abs(float(s.get("line") or 999) - float(want_line or 0)),
        )
        print("  nearest", best.get("line"))
        # cycle to best
        seen2 = set()
        for i in range(14):
            st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
            pt = str(st.get("pickType") or "").lower()
            line = st.get("line")
            if pt == want_pick and abs(float(line or -1) - float(best.get("line"))) < 0.01:
                return st, btn, idx
            key = (pt, line)
            if key in seen2 and i > 0:
                break
            seen2.add(key)
            P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop="")
            time.sleep(0.45)
        return best, btn, idx
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


def switch_soccer(page, frame):
    # Prefer in-app tab; fall back to league_id navigate (will keep slip? NO - goto drops slip)
    # So only use tab click. Scroll sports row if needed.
    page.evaluate(
        """() => {
      // scroll horizontal sport chips
      const row = Array.from(document.querySelectorAll('div,nav')).find(el => {
        const t = el.innerText || '';
        return t.includes('NFL') && t.includes('CFB') && t.includes('MLB') && el.scrollWidth > el.clientWidth;
      });
      if (row) row.scrollLeft = row.scrollWidth;
    }"""
    )
    page.wait_for_timeout(400)
    switched = P.switch_sport_tab(frame, page, "SOCCER", settle_ms=2000)
    if switched is not None:
        return switched
    # try exact Soccer text variants
    for label in ("Soccer", "SOCCER", "EPL", "MLS"):
        try:
            frame.get_by_text(label, exact=True).first.click(timeout=2000)
            page.wait_for_timeout(1500)
            print("sport via", label)
            return P.find_prizepicks_frame(page)
        except Exception:
            pass
    # last resort: league navigate — WARN drops slip so avoid if legs already selected
    return None


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
        print("[SLIP]", raw[:500])
        if "Gasser" not in raw:
            print("re-add Gasser")
            P.switch_sport_tab(frame, page, "MLB", settle_ms=1500)
            frame = P.find_prizepicks_frame(page)
            react_search(page, "Gasser")
            page.wait_for_timeout(1200)
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
            print("gasser", ok)
            if not ok:
                hits = find_by_full_name(frame, "Gasser", ["Ks", "Strikeout"])
                if hits:
                    st, btn, idx = cycle_to(frame, hits[0], "goblin", 3.5, False)
                    if st:
                        click_more(btn, idx, frame)

        # Freeman — full first name to avoid Gavin/Jayce
        print("\n=== Josiah Freeman Rec 19.5 ===")
        P.switch_sport_tab(frame, page, "CFB", settle_ms=1600)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        react_search(page, "Josiah Freeman")
        page.wait_for_timeout(2000)
        # probe
        probe = page.evaluate(
            """() => {
          const t = document.body.innerText || '';
          const idx = t.indexOf('Josiah');
          return idx < 0 ? 'no Josiah' : t.slice(Math.max(0,idx-20), idx+180).replace(/\\s+/g,' ');
            }"""
        )
        print("[PROBE]", probe)
        hits = find_by_full_name(
            frame, "Josiah Freeman", ["Rec", "Receiv", "Yards"]
        )
        if not hits:
            react_search(page, "Josiah")
            page.wait_for_timeout(1500)
            hits = find_by_full_name(frame, "Josiah", ["Rec", "Receiv", "Yards"])
        print("hits", len(hits))
        freeman_ok = False
        if hits:
            # prefer Rec Yards
            card = hits[0]
            for c in hits:
                if "rec" in P._norm(c.get("prop_type")) and "yard" in P._norm(
                    c.get("prop_type")
                ):
                    card = c
                    break
            print("card", card.get("player"), card.get("prop_type"), card.get("line"))
            st, btn, idx = cycle_to(frame, card, "goblin", 19.5, True)
            if st:
                freeman_ok = click_more(btn, idx, frame)
                print("freeman face", st.get("pickType"), st.get("line"), "ok", freeman_ok)
        else:
            print("Josiah Freeman not on live board")

        # Malen soccer
        print("\n=== Malen SOT 0.5 ===")
        frame2 = switch_soccer(page, frame)
        if frame2 is None:
            print("soccer tab miss — try navigate with league (risk slip wipe)")
            # Check league ids
            lid = getattr(P, "PP_BOARD_LEAGUE_IDS", {}).get("SOCCER") or getattr(
                P, "PP_BOARD_LEAGUE_IDS", {}
            ).get("SOC")
            print("league_id", lid)
            # Don't goto if we have legs — try clicking from filter sample
            labels = page.evaluate(
                """() => Array.from(document.querySelectorAll('button, [role=tab], a, div'))
                  .map(b => (b.innerText||'').trim())
                  .filter(t => t && t.length < 20 && /soccer|epl|mls|ucl|liga/i.test(t))
                  .slice(0,30)"""
            )
            print("soccer-ish labels", labels)
            for lab in labels:
                try:
                    page.get_by_text(lab, exact=True).first.click(timeout=1500)
                    page.wait_for_timeout(1500)
                    frame2 = P.find_prizepicks_frame(page)
                    print("clicked", lab)
                    break
                except Exception:
                    pass
        if frame2 is not None:
            frame = frame2
        P.dismiss_modal(frame, page)
        react_search(page, "Malen")
        page.wait_for_timeout(2000)
        probe = page.evaluate(
            """() => {
          const t = document.body.innerText || '';
          const idx = t.indexOf('Malen');
          return idx < 0 ? 'no Malen' : t.slice(Math.max(0,idx-30), idx+200).replace(/\\s+/g,' ');
            }"""
        )
        print("[PROBE Malen]", probe)
        hits = find_by_full_name(
            frame, "Malen", ["Shot", "SOT", "Target", "Shots"]
        )
        print("malen hits", len(hits))
        malen_ok = False
        if hits:
            card = hits[0]
            for c in hits:
                cp = P._norm(c.get("prop_type"))
                if "shot" in cp or "sot" in cp or "target" in cp:
                    card = c
                    break
            print("card", card.get("player"), card.get("prop_type"), card.get("line"))
            st, btn, idx = cycle_to(frame, card, "goblin", 0.5, True)
            if st:
                malen_ok = click_more(btn, idx, frame)
                print("malen face", st.get("pickType"), st.get("line"), "ok", malen_ok)

        try:
            P.set_ticket_type(frame, "power")
            frame.get_by_text("Power Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=3, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or ""
        print("\n[FINAL]")
        print(raw[:2000])
        ok = (
            "Gasser" in raw
            and "Freeman" in raw
            and "Josiah" in raw
            and "Malen" in raw
            and int((slip or {}).get("n_selected") or 0) >= 3
        )
        if ok:
            print("\nREADY — hit Submit in CDP Chrome.")
            return 0
        print("\nINCOMPLETE freeman", freeman_ok, "malen", malen_ok)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
