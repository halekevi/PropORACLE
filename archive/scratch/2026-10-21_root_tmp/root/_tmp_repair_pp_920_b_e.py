#!/usr/bin/env python3
"""Repair B–E on PrizePicks: correct props + Goblin faces. Leave A alone. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"

TICKETS = [
    {
        "id": "B",
        "mode": "power",
        "legs": [
            {
                "sport": "TENNIS",
                "player": "Janice Tjen",
                "search": "Tjen",
                "prop_type": "Total Games",
                "direction": "OVER",
                "line": 17.5,
                "pick_type": "goblin",
                "forbid": ["1st set", "first set", "games won"],
            },
            {
                "sport": "TENNIS",
                "player": "Polina Kudermetova",
                "search": "Kudermetova",
                "prop_type": "Games Won",
                "direction": "OVER",
                "line": 5.5,
                "pick_type": "goblin",
                "forbid": ["1st set", "first set", "total games"],
            },
        ],
    },
    {
        "id": "C",
        "mode": "power",
        "legs": [
            {
                "sport": "NFL",
                "player": "Woody Marks",
                "search": "Woody Marks",
                "prop_type": "Rush Yards",
                "direction": "OVER",
                "line": 25.5,
                "pick_type": "goblin",
            },
            {
                "sport": "NFL",
                "player": "Mack Hollins",
                "search": "Hollins",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 31.5,
                "pick_type": "goblin",
            },
            {
                "sport": "NFL",
                "player": "Devaughn Vele",
                "search": "Vele",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 36.5,
                "pick_type": "goblin",
            },
        ],
    },
    {
        "id": "D",
        "mode": "power",
        "legs": [
            {
                "sport": "NFL",
                "player": "Jacory Croskey-Merritt",
                "search": "Croskey",
                "prop_type": "Rush Yards",
                "direction": "OVER",
                "line": 49.5,
                "pick_type": "goblin",
            },
            {
                "sport": "NFL",
                "player": "Dallas Goedert",
                "search": "Goedert",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 32.5,
                "pick_type": "goblin",
            },
            {
                "sport": "NFL",
                "player": "Dontayvion Wicks",
                "search": "Wicks",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 29.5,
                "pick_type": "goblin",
            },
        ],
    },
    {
        "id": "E",
        "mode": "flex",
        "legs": [
            {
                "sport": "NFL",
                "player": "Woody Marks",
                "search": "Woody Marks",
                "prop_type": "Rush Yards",
                "direction": "OVER",
                "line": 25.5,
                "pick_type": "goblin",
            },
            {
                "sport": "NFL",
                "player": "Mack Hollins",
                "search": "Hollins",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 31.5,
                "pick_type": "goblin",
            },
            {
                "sport": "NFL",
                "player": "Dallas Goedert",
                "search": "Goedert",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 32.5,
                "pick_type": "goblin",
            },
            {
                "sport": "NFL",
                "player": "Kirk Cousins",
                "search": "Cousins",
                "prop_type": "Pass Yards",
                "direction": "UNDER",
                "line": 192.5,
                "pick_type": "standard",
            },
        ],
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


def prop_ok(card_prop: str, want: str, forbid: list[str]) -> bool:
    cp = P._norm(card_prop or "")
    wp = P._norm(want or "")
    if not cp or not wp:
        return False
    if forbid and any(P._norm(f) in cp for f in forbid):
        return False
    # Total Games must not match Total Games Won
    if wp == "total games":
        return cp == "total games" or (cp.startswith("total games") and "won" not in cp and "1st" not in cp)
    if wp in ("games won", "total games won"):
        return "games won" in cp and "1st" not in cp
    return wp == cp or wp in cp or cp in wp


def cycle_to_face(frame, card, *, want_pick: str, want_line: float):
    btn = card.get("more_btn")
    idx = card.get("more_index")
    seen = set()
    goblins = []
    for i in range(16):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        key = (pt, line)
        print(f"  face[{i}] {pt} {line}")
        if key in seen and i > 0:
            break
        seen.add(key)
        if pt == want_pick:
            goblins.append(st)
            try:
                if abs(float(line) - float(want_line)) < 0.01:
                    return st, btn, idx
            except Exception:
                pass
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.45)
    if goblins:
        def dist(s):
            try:
                return abs(float(s.get("line")) - float(want_line))
            except Exception:
                return 999

        best = min(goblins, key=dist)
        print(f"  nearest {want_pick} -> {best.get('line')} (wanted {want_line})")
        # cycle back
        seen2 = set()
        for i in range(16):
            st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
            pt = str(st.get("pickType") or "").lower()
            line = st.get("line")
            key = (pt, line)
            try:
                if pt == want_pick and abs(float(line) - float(best.get("line"))) < 0.01:
                    return st, btn, idx
            except Exception:
                pass
            if key in seen2 and i > 0:
                break
            seen2.add(key)
            P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop="")
            time.sleep(0.4)
        return best, btn, idx
    return None, btn, idx


def click_dir(frame, btn, idx, direction: str) -> bool:
    action = "Less" if direction.upper() in ("UNDER", "LESS") else "More"
    # Prefer direct More/Less near the card
    try:
        if btn is not None and action == "More":
            btn.click(timeout=1500, no_wait_after=True)
            time.sleep(0.4)
            return True
    except Exception:
        pass
    card = {
        "player": "",
        "prop_type": "",
        "line": None,
        "pick_type": "goblin",
        "more_btn": btn,
        "more_index": idx,
    }
    return bool(P.click_leg(frame, card, direction))


def add_one(page, frame, leg) -> bool:
    sport = leg["sport"]
    switched = P.switch_sport_tab(frame, page, sport, settle_ms=1400)
    if switched is not None:
        frame = switched
    P.dismiss_modal(frame, page)
    react_search(page, leg["search"])
    page.wait_for_timeout(1500)

    cards = P.get_all_cards(frame)
    forbid = leg.get("forbid") or []
    hits = []
    for c in cards:
        pname = str(c.get("player") or "")
        if leg["search"].lower() not in pname.lower() and leg["player"].split()[-1].lower() not in pname.lower():
            # also check state blob
            st = P._read_card_state_from_more(
                c.get("more_btn"), frame=frame, more_index=c.get("more_index")
            ) or {}
            blob = " ".join(str(x) for x in (st.get("text") or [])).lower()
            if leg["search"].lower() not in blob and leg["player"].split()[-1].lower() not in blob:
                continue
        if prop_ok(str(c.get("prop_type") or ""), leg["prop_type"], forbid):
            hits.append(c)
    print(f"  candidate cards: {len(hits)}")
    if not hits:
        # looser name, still prop_ok
        for c in cards:
            if prop_ok(str(c.get("prop_type") or ""), leg["prop_type"], forbid):
                st = P._read_card_state_from_more(
                    c.get("more_btn"), frame=frame, more_index=c.get("more_index")
                ) or {}
                blob = " ".join(str(x) for x in (st.get("text") or [])).lower()
                if leg["search"].lower() in blob:
                    hits.append(c)
        print(f"  loose candidates: {len(hits)}")
    if not hits:
        return False

    card = hits[0]
    print(
        f"  using {card.get('player')} | {card.get('prop_type')} | "
        f"{card.get('line')} | {card.get('pick_type')}"
    )
    st, btn, idx = cycle_to_face(
        frame,
        card,
        want_pick=leg["pick_type"],
        want_line=float(leg["line"]),
    )
    if st is None:
        print("  cycle failed")
        return False
    print(f"  final face {st.get('pickType')} {st.get('line')}")
    return click_dir(frame, btn, idx, leg["direction"])


def build_ticket(page, ticket: dict) -> dict:
    tid = ticket["id"]
    mode = ticket["mode"]
    n_want = len(ticket["legs"])
    first_sport = ticket["legs"][0]["sport"]
    print(f"\n======== REBUILD {tid} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, first_sport, settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    page.wait_for_timeout(500)

    fails = []
    clicked = 0
    for leg in ticket["legs"]:
        print(
            f"\n--- {tid} {leg['player']} {leg['prop_type']} "
            f"{leg['direction']} {leg['line']} {leg['pick_type']} ---"
        )
        ok = add_one(page, frame, leg)
        frame = P.find_prizepicks_frame(page)
        print("  result", "OK" if ok else "FAIL")
        if ok:
            clicked += 1
        else:
            fails.append(leg["player"])
        try:
            P.set_ticket_type(frame, mode)
        except Exception:
            pass
        page.wait_for_timeout(500)

    try:
        label = "Power Play" if mode == "power" else "Flex Play"
        frame.get_by_text(label, exact=True).first.click(timeout=1500)
    except Exception:
        pass

    slip = P.read_slip(frame, n_legs=n_want, ticket_type=mode)
    raw = (slip or {}).get("raw_slip_section") or (slip or {}).get("raw_text") or ""
    n = int((slip or {}).get("n_selected") or 0)
    print(f"\n[{tid} FINAL] n={n}/{n_want}\n{raw[:1600]}")
    return {"id": tid, "ready": n >= n_want and not fails, "n": n, "want": n_want, "fails": fails, "raw": raw}


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=30_000)
        ctx = browser.contexts[0]
        pages = []
        for t in TICKETS:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1800)
            pages.append(pg)
            print(f"tab {t['id']} opened")

        for page, ticket in zip(pages, TICKETS):
            try:
                results.append(build_ticket(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append(
                    {"id": ticket["id"], "ready": False, "n": 0, "want": len(ticket["legs"]), "fails": [str(e)], "raw": ""}
                )

        # Leave A alone; bring B front if ready else first
        for page, ticket in zip(pages, TICKETS):
            if ticket["id"] == "B":
                page.bring_to_front()
                break

    print("\n======== REPAIR SUMMARY ========")
    for r in results:
        print(f"  {r['id']}: {'READY' if r['ready'] else 'INCOMPLETE'} {r['n']}/{r['want']} fails={r['fails'] or '-'}")
    print("A (prior tab) left as-is if still intact. Submit manually. No C+E / D+E.")
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
