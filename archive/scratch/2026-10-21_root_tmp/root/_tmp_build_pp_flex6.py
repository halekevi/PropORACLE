#!/usr/bin/env python3
"""Build Flex 6: Schlittler / Gasser / Dampier / Bishop / Malen / Arike. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

NEED = ("Schlittler", "Gasser", "Dampier", "Bishop", "Malen", "Ogunbowale|Arike")


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
        if (
            name.lower() not in blob.lower()
            and name.lower() not in pname.lower()
        ):
            continue
        if prop_ok(str(c.get("prop_type") or ""), blob):
            return c
    return None


def cycle_exact(frame, card, want_pick, want_line, max_swaps=18, accept_nearest=False):
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen = set()
    goblins = []
    for i in range(max_swaps):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        print(f"  face[{i}] {pt} {line}")
        key = (pt, line)
        if key in seen and i > 0:
            break
        seen.add(key)
        if pt == want_pick:
            goblins.append(st)
            if abs(float(line or -1) - float(want_line)) < 0.01:
                return st, btn, idx
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.5)
    if accept_nearest and goblins:
        best = min(
            goblins,
            key=lambda s: abs(float(s.get("line") or 999) - float(want_line)),
        )
        print(f"  nearest {want_pick} -> {best.get('line')} (wanted {want_line})")
        seen2 = set()
        for i in range(max_swaps):
            st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
            pt = str(st.get("pickType") or "").lower()
            line = st.get("line")
            key = (pt, line)
            if (
                pt == want_pick
                and line is not None
                and abs(float(line) - float(best.get("line"))) < 0.01
            ):
                return st, btn, idx
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


def add_via_library(frame, page, *, sport, search, name, prop_ok, line, pick="goblin", nearest=False):
    switched = P.switch_sport_tab(frame, page, sport, settle_ms=1400)
    if switched is not None:
        frame = switched
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, search)
    page.wait_for_timeout(1800)
    # try strict add_leg first
    leg = {
        "sport": sport,
        "player": name,
        "prop_type": "",
        "direction": "OVER",
        "line": line,
        "pick_type": pick,
    }
    # Don't rely on prop_type for add_leg when empty — use card path
    card = find_card(frame, name.split()[-1] if " " in name else name, prop_ok)
    if card is None and " " in name:
        card = find_card(frame, name, prop_ok)
    if not card:
        print(f"  no card for {name}")
        return False, frame
    print(
        f"  card player={card.get('player')} prop={card.get('prop_type')} "
        f"line={card.get('line')} pick={card.get('pick_type')}"
    )
    st, btn, idx = cycle_exact(
        frame, card, pick, line, accept_nearest=nearest
    )
    if st is None:
        print("  cycle miss")
        return False, frame
    print(f"  final {st.get('pickType')} {st.get('line')}")
    return click_more(btn, idx, frame), frame


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

        P.navigate_board_for_sport(page, "MLB", settle_ms=2200)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "flex")
        page.wait_for_timeout(500)

        results = {}

        def ks_ok(prop, blob):
            return (
                (prop in ("Ks", "Strikeouts", "Pitcher Strikeouts") or "Ks" in blob)
                and "Hit" not in prop
                and "Walk" not in prop
                and "Earned" not in prop
            )

        # 1 Schlittler
        print("\n=== Schlittler Ks 3.5 ===")
        ok, frame = add_via_library(
            frame,
            page,
            sport="MLB",
            search="Schlittler",
            name="Schlittler",
            prop_ok=ks_ok,
            line=3.5,
        )
        results["Schlittler"] = ok
        print("  ->", ok)
        try:
            P.set_ticket_type(frame, "flex")
        except Exception:
            pass

        # 2 Gasser
        print("\n=== Gasser Ks 3.5 ===")
        ok, frame = add_via_library(
            frame,
            page,
            sport="MLB",
            search="Gasser",
            name="Gasser",
            prop_ok=ks_ok,
            line=3.5,
        )
        results["Gasser"] = ok
        print("  ->", ok)

        # 3 Dampier
        print("\n=== Dampier Pass 159.5 ===")
        ok, frame = add_via_library(
            frame,
            page,
            sport="CFB",
            search="Dampier",
            name="Dampier",
            prop_ok=lambda prop, blob: (
                "Pass" in prop or "Pass Yards" in blob
            )
            and "Rush" not in prop
            and "TD" not in prop,
            line=159.5,
            nearest=True,
        )
        results["Dampier"] = ok
        print("  ->", ok)

        # 4 Bishop Rush 69.5 — try goblin then standard
        print("\n=== Bishop Rush 69.5 ===")
        ok, frame = add_via_library(
            frame,
            page,
            sport="CFB",
            search="Bishop",
            name="Bishop",
            prop_ok=lambda prop, blob: (
                "Rush" in prop or "Rush Yards" in blob
            )
            and "Rec" not in prop
            and "TD" not in prop
            and "Long" not in prop,
            line=69.5,
            pick="goblin",
            nearest=True,
        )
        if not ok:
            print("  retry as standard")
            ok, frame = add_via_library(
                frame,
                page,
                sport="CFB",
                search="DeSean Bishop",
                name="Bishop",
                prop_ok=lambda prop, blob: (
                    "Rush" in prop or "Rush Yards" in blob
                )
                and "Rec" not in prop
                and "TD" not in prop,
                line=69.5,
                pick="standard",
                nearest=True,
            )
        results["Bishop"] = ok
        print("  ->", ok)

        # 5 Malen SOT
        print("\n=== Malen SOT 0.5 ===")
        frame = go_soccer(page, frame)
        react_search(page, "Malen")
        page.wait_for_timeout(2000)
        card = find_card(
            frame,
            "Malen",
            lambda prop, blob: prop.upper() == "SOT"
            or " SOT" in f" {blob}"
            or "On Target" in prop
            or "On Target" in blob,
        )
        m_ok = False
        if card:
            print(
                "malen card",
                card.get("prop_type"),
                card.get("line"),
                card.get("pick_type"),
            )
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
        results["Malen"] = m_ok
        print("  ->", m_ok)
        frame = P.find_prizepicks_frame(page)

        # 6 Arike
        print("\n=== Arike Rebs+Asts 4.5 ===")
        ok, frame = add_via_library(
            frame,
            page,
            sport="WNBA",
            search="Ogunbowale",
            name="Ogunbowale",
            prop_ok=lambda prop, blob: (
                "Rebs+Asts" in prop
                or "Rebs + Asts" in prop
                or "Rebs+Asts" in blob
                or ("Reb" in prop and "Ast" in prop)
            ),
            line=4.5,
        )
        results["Arike"] = ok
        print("  ->", ok)

        try:
            P.set_ticket_type(frame, "flex")
            frame.get_by_text("Flex Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=6, ticket_type="flex")
        raw = (slip or {}).get("raw_slip_section") or (slip or {}).get("raw_text") or ""
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
        print("\nresults", results)
        if n >= 6 and names_ok:
            print("\nREADY — Flex 6 built. Hit Submit in the PrizePicks Chrome window.")
            if "SOT" not in raw and "Malen" in raw:
                print("NOTE: check Malen is SOT not Shots.")
            if "159.5" not in raw and "Dampier" in raw:
                print("NOTE: Dampier line may have moved — check face.")
            return 0
        print("\nINCOMPLETE", n, names_ok)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
