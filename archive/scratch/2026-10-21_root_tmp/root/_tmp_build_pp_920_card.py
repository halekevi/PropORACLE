#!/usr/bin/env python3
"""Build Sun 9/20 card A–E in separate PrizePicks Chrome tabs. No submit."""
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
        "id": "A",
        "label": "Tennis Power 3 (primary)",
        "mode": "power",
        "legs": [
            {
                "sport": "TENNIS",
                "player": "Anna Bondar",
                "search": "Bondar",
                "prop_type": "Total Games",
                "direction": "OVER",
                "line": 17.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
            {
                "sport": "TENNIS",
                "player": "Moyuka Uchijima",
                "search": "Uchijima",
                "prop_type": "Games Won",
                "direction": "OVER",
                "line": 5.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
            {
                "sport": "TENNIS",
                "player": "Maria Sakkari",
                "search": "Sakkari",
                "prop_type": "Total Games",
                "direction": "OVER",
                "line": 16.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
        ],
    },
    {
        "id": "B",
        "label": "Tennis Power 2",
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
                "accept_nearest_goblin": True,
            },
            {
                "sport": "TENNIS",
                "player": "Polina Kudermetova",
                "search": "Kudermetova",
                "prop_type": "Games Won",
                "direction": "OVER",
                "line": 5.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
        ],
    },
    {
        "id": "C",
        "label": "NFL Goblin Power 3 (hand-check n=1)",
        "mode": "power",
        "legs": [
            {
                "sport": "NFL",
                "player": "Woody Marks",
                "search": "Marks",
                "prop_type": "Rush Yards",
                "direction": "OVER",
                "line": 25.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
            {
                "sport": "NFL",
                "player": "Mack Hollins",
                "search": "Hollins",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 31.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
            {
                "sport": "NFL",
                "player": "Devaughn Vele",
                "search": "Vele",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 36.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
        ],
    },
    {
        "id": "D",
        "label": "NFL Goblin Power 3 #2 (hand-check n=1)",
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
                "accept_nearest_goblin": True,
            },
            {
                "sport": "NFL",
                "player": "Dallas Goedert",
                "search": "Goedert",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 32.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
            {
                "sport": "NFL",
                "player": "Dontayvion Wicks",
                "search": "Wicks",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 29.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
        ],
    },
    {
        "id": "E",
        "label": "NFL Flex 4 (hand-check; blocks C and D)",
        "mode": "flex",
        "legs": [
            {
                "sport": "NFL",
                "player": "Woody Marks",
                "search": "Marks",
                "prop_type": "Rush Yards",
                "direction": "OVER",
                "line": 25.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
            {
                "sport": "NFL",
                "player": "Mack Hollins",
                "search": "Hollins",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 31.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
            {
                "sport": "NFL",
                "player": "Dallas Goedert",
                "search": "Goedert",
                "prop_type": "Rec Yards",
                "direction": "OVER",
                "line": 32.5,
                "pick_type": "goblin",
                "accept_nearest_goblin": True,
            },
            {
                "sport": "NFL",
                "player": "Kirk Cousins",
                "search": "Cousins",
                "prop_type": "Pass Yards",
                "direction": "UNDER",
                "line": 192.5,
                "pick_type": "standard",
                "accept_nearest_goblin": False,
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


def add_one(page, frame, leg) -> bool:
    sport = leg["sport"]
    switched = P.switch_sport_tab(frame, page, sport, settle_ms=1400)
    if switched is not None:
        frame = switched
    P.dismiss_modal(frame, page)
    react_search(page, leg["search"])
    page.wait_for_timeout(1400)

    ok = P.add_leg(
        frame,
        page,
        leg,
        strict_lines=not bool(leg.get("accept_nearest_goblin")),
        require_line=not bool(leg.get("accept_nearest_goblin")),
    )
    if ok:
        return True

    # Fallback: looser line match
    print(f"  strict miss — retry accept_nearest for {leg['player']}")
    leg2 = dict(leg)
    ok = P.add_leg(frame, page, leg2, strict_lines=False, require_line=False)
    return bool(ok)


def build_ticket(page, ticket: dict) -> dict:
    tid = ticket["id"]
    mode = ticket["mode"]
    n_want = len(ticket["legs"])
    first_sport = ticket["legs"][0]["sport"]

    print(f"\n======== TICKET {tid}: {ticket['label']} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, first_sport, settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    page.wait_for_timeout(500)

    clicked = 0
    fails = []
    for leg in ticket["legs"]:
        print(
            f"\n--- {tid} {leg['sport']} {leg['player']} "
            f"{leg['prop_type']} {leg['direction']} {leg['line']} {leg['pick_type']} ---"
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
        page.wait_for_timeout(600)

    try:
        label = "Power Play" if mode == "power" else "Flex Play"
        frame.get_by_text(label, exact=True).first.click(timeout=1500)
    except Exception:
        pass

    slip = P.read_slip(frame, n_legs=n_want, ticket_type=mode)
    raw = (slip or {}).get("raw_slip_section") or (slip or {}).get("raw_text") or ""
    n = int((slip or {}).get("n_selected") or 0)
    print(f"\n[{tid} FINAL] n_selected={n} want={n_want} clicked={clicked}")
    print(raw[:1800])

    ready = n >= n_want and clicked >= n_want and not fails
    return {
        "id": tid,
        "ready": ready,
        "n": n,
        "want": n_want,
        "fails": fails,
        "raw": raw[:500],
        "url": page.url,
    }


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=30_000)
        ctx = browser.contexts[0] if browser.contexts else None
        if ctx is None:
            print("ERROR: no browser context")
            return 1

        # Prefer fresh tab per ticket so each slip stays intact for submit.
        pages = []
        for t in TICKETS:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(2000)
            pages.append(pg)
            print(f"opened tab for {t['id']}: {pg.url}")

        for page, ticket in zip(pages, TICKETS):
            try:
                results.append(build_ticket(page, ticket))
            except Exception as e:
                print(f"TICKET {ticket['id']} EXCEPTION: {type(e).__name__}: {e}")
                results.append(
                    {
                        "id": ticket["id"],
                        "ready": False,
                        "n": 0,
                        "want": len(ticket["legs"]),
                        "fails": [f"EXC:{e}"],
                        "raw": "",
                        "url": page.url,
                    }
                )

        # Leave tabs open; bring A to front last as primary.
        for page, ticket in zip(pages, TICKETS):
            if ticket["id"] == "A":
                page.bring_to_front()
                break

    print("\n================ SUMMARY ================")
    for r in results:
        status = "READY" if r["ready"] else "INCOMPLETE"
        print(
            f"  {r['id']}: {status}  legs={r['n']}/{r['want']}  "
            f"fails={r['fails'] or '-'}"
        )
    print(
        "\nDo NOT submit C+E or D+E together (overlap). "
        "C+D ok. A then B then one of {C,D,E}."
    )
    print("Submit manually in each Chrome tab — script did not click Submit.")
    ready_n = sum(1 for r in results if r["ready"])
    return 0 if ready_n == len(TICKETS) else 2


if __name__ == "__main__":
    raise SystemExit(main())
