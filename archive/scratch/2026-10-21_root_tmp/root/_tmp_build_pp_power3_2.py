#!/usr/bin/env python3
"""Build Power 3 #2 via CDP. Does NOT submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

LEGS = [
    {
        "sport": "MLB",
        "player": "Robert Gasser",
        "search": "Gasser",
        "prop_type": "Pitcher Strikeouts",
        "direction": "OVER",
        "line": 3.5,
        "pick_type": "goblin",
        "name_aliases": ["Robert Gasser", "Gasser"],
        "accept_nearest_goblin": False,
    },
    {
        "sport": "CFB",
        "player": "Josiah Freeman",
        "search": "Freeman",
        "prop_type": "Receiving Yards",
        "direction": "OVER",
        "line": 19.5,
        "pick_type": "goblin",
        "name_aliases": ["Josiah Freeman", "Freeman"],
        "accept_nearest_goblin": True,
    },
    {
        "sport": "SOCCER",
        "player": "Donyell Malen",
        "search": "Malen",
        "prop_type": "Shots On Target",
        "direction": "OVER",
        "line": 0.5,
        "pick_type": "goblin",
        "name_aliases": ["Donyell Malen", "Malen"],
        "accept_nearest_goblin": True,
    },
]


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


def find_cards(frame, aliases, prop_sub: str):
    cards = P.get_all_cards(frame)
    prop_n = P._norm(prop_sub)
    hits = []
    for c in cards:
        cp = P._norm(c.get("prop_type"))
        prop_ok = (not prop_n) or (prop_n == cp or prop_n in cp or cp in prop_n)
        # SOT aliases
        if not prop_ok and "shot" in prop_n:
            prop_ok = "shot" in cp or "sot" in cp
        if not prop_ok and "receiv" in prop_n:
            prop_ok = "rec" in cp or "receiv" in cp
        if not prop_ok and ("strike" in prop_n or prop_n == "ks"):
            prop_ok = cp in ("ks", "strikeouts") or "strike" in cp
        if not prop_ok:
            continue
        pname = str(c.get("player") or "")
        ok_name = any(a.lower() in pname.lower() or pname.lower() in a.lower() for a in aliases)
        if not ok_name and (
            c.get("more_btn") is not None or c.get("more_index") is not None
        ):
            st = P._read_card_state_from_more(
                c.get("more_btn"), frame=frame, more_index=c.get("more_index")
            ) or {}
            blob = " ".join(str(x) for x in (st.get("text") or []))
            ok_name = any(a.lower() in blob.lower() for a in aliases)
        if ok_name:
            hits.append(c)
    return hits


def cycle_to_target(frame, card, *, want_pick: str, want_line: float | None, accept_nearest_goblin: bool):
    btn = card.get("more_btn")
    idx = card.get("more_index")
    seen = set()
    goblin_faces = []
    for i in range(14):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        key = (pt, line)
        print(f"  face[{i}] {pt} {line}")
        if key in seen and i > 0:
            break
        seen.add(key)
        if pt == want_pick:
            goblin_faces.append(st)
            if want_line is not None and abs(float(line or -1) - float(want_line)) < 0.01:
                return st, btn, idx
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.5)
    if accept_nearest_goblin and goblin_faces:
        def dist(s):
            try:
                return abs(float(s.get("line")) - float(want_line))
            except Exception:
                return 999

        best = min(goblin_faces, key=dist)
        print(f"  nearest goblin -> {best.get('line')} (wanted {want_line})")
        seen2 = set()
        for i in range(14):
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


def click_more_on(frame, btn, idx, direction: str) -> bool:
    try:
        if direction.upper() in ("UNDER", "LESS"):
            return P.click_leg(
                frame,
                {
                    "player": "",
                    "prop_type": "",
                    "more_btn": btn,
                    "more_index": idx,
                    "pick_type": "goblin",
                },
                direction,
            )
        if btn is not None:
            try:
                btn.click(timeout=1200, no_wait_after=True)
            except Exception:
                btn.click(force=True, timeout=2000, no_wait_after=True)
            time.sleep(0.4)
            return True
        return bool(
            P._eval_more_js(
                "el => { el.click(); return true; }",
                None,
                frame=frame,
                more_index=idx,
            )
        )
    except Exception as e:
        print("click err", e)
        return False


def add_one(page, frame, leg) -> bool:
    sport = leg["sport"]
    switched = P.switch_sport_tab(frame, page, sport, settle_ms=1600)
    if switched is not None:
        frame = switched
    P.dismiss_modal(frame, page)
    react_search(page, leg["search"])
    page.wait_for_timeout(1600)

    ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
    if ok:
        return True

    print("  add_leg miss — manual cycle path")
    react_search(page, leg["search"])
    page.wait_for_timeout(1200)
    hits = find_cards(frame, leg["name_aliases"], leg["prop_type"])
    if not hits:
        cards = P.get_all_cards(frame)
        for c in cards:
            st = P._read_card_state_from_more(
                c.get("more_btn"), frame=frame, more_index=c.get("more_index")
            ) or {}
            blob = " ".join(str(x) for x in (st.get("text") or [])).lower()
            if leg["search"].lower() not in blob:
                continue
            prop_l = leg["prop_type"].lower()
            if any(
                x in blob
                for x in [
                    prop_l,
                    "pass yards",
                    "rec",
                    "receiving",
                    "ks",
                    "strikeout",
                    "shot",
                    "sot",
                ]
            ):
                hits.append(c)
    print(f"  candidate cards: {len(hits)}")
    if not hits:
        return False
    card = hits[0]
    for c in hits:
        if P._norm(leg["prop_type"]) in P._norm(c.get("prop_type")) or P._norm(
            c.get("prop_type")
        ) in P._norm(leg["prop_type"]):
            card = c
            break
        # SOT / Rec soft match
        cp = P._norm(c.get("prop_type"))
        if "shot" in P._norm(leg["prop_type"]) and "shot" in cp:
            card = c
            break
        if "receiv" in P._norm(leg["prop_type"]) and ("rec" in cp or "receiv" in cp):
            card = c
            break
    print(
        f"  using card player={card.get('player')} prop={card.get('prop_type')} "
        f"line={card.get('line')} pick={card.get('pick_type')}"
    )
    st, btn, idx = cycle_to_target(
        frame,
        card,
        want_pick=leg["pick_type"],
        want_line=leg["line"],
        accept_nearest_goblin=bool(leg.get("accept_nearest_goblin")),
    )
    if st is None:
        print("  cycle failed")
        return False
    print(f"  final face {st.get('pickType')} {st.get('line')}")
    return click_more_on(frame, btn, idx, leg["direction"])


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

        P.navigate_board_for_sport(page, "MLB", settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        page.wait_for_timeout(600)

        clicked = 0
        for leg in LEGS:
            print(f"\n=== {leg['sport']} {leg['player']} want {leg['line']} goblin ===")
            ok = add_one(page, frame, leg)
            frame = P.find_prizepicks_frame(page)
            print("  result", "OK" if ok else "FAIL")
            if ok:
                clicked += 1
            try:
                P.set_ticket_type(frame, "power")
            except Exception:
                pass
            page.wait_for_timeout(700)

        try:
            frame.get_by_text("Power Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass

        slip = P.read_slip(frame, n_legs=3, ticket_type="power")
        raw = (slip or {}).get("raw_slip_section") or (slip or {}).get("raw_text") or ""
        print("\n[FINAL]")
        print(raw[:2000])
        n = int((slip or {}).get("n_selected") or 0)
        names_ok = (
            "Gasser" in raw
            and "Freeman" in raw
            and "Malen" in raw
        )
        if n >= 3 and names_ok:
            print("\nREADY — Power Play #2 built. Hit Submit in the PrizePicks Chrome window.")
            return 0
        print("\nINCOMPLETE", f"clicked={clicked}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
