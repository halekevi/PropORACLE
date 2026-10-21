#!/usr/bin/env python3
"""Build the three 9/23 G70 Power 3s on PrizePicks CDP. Does NOT submit."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
SHOT_DIR = _REPO / "data" / "reports"
SHOT_DIR.mkdir(parents=True, exist_ok=True)

TICKETS = [
    {
        "id": "T1",
        "name": "Power 3 · Lugo / Henderson / Kelly",
        "legs": [
            {
                "sport": "MLB",
                "player": "Seth Lugo",
                "search": "Seth Lugo",
                "prop_type": "Hits Allowed",
                "direction": "OVER",
                "line": 2.5,
                "pick_type": "goblin",
            },
            {
                "sport": "MLB",
                "player": "Logan Henderson",
                "search": "Logan Henderson",
                "prop_type": "Pitcher Strikeouts",
                "direction": "OVER",
                "line": 3.5,
                "pick_type": "goblin",
            },
            {
                "sport": "MLB",
                "player": "Merrill Kelly",
                "search": "Merrill Kelly",
                "prop_type": "Walks Allowed",
                "direction": "OVER",
                "line": 0.5,
                "pick_type": "goblin",
            },
        ],
    },
    {
        "id": "T2",
        "name": "Power 3 · Kirby / Sale / Springs",
        "legs": [
            {
                "sport": "MLB",
                "player": "George Kirby",
                "search": "George Kirby",
                "prop_type": "Hits Allowed",
                "direction": "OVER",
                "line": 2.5,
                "pick_type": "goblin",
            },
            {
                "sport": "MLB",
                "player": "Chris Sale",
                "search": "Chris Sale",
                "prop_type": "Pitcher Strikeouts",
                "direction": "OVER",
                "line": 4.5,
                "pick_type": "goblin",
            },
            {
                "sport": "MLB",
                "player": "Jeffrey Springs",
                "search": "Jeffrey Springs",
                "prop_type": "Walks Allowed",
                "direction": "OVER",
                "line": 0.5,
                "pick_type": "goblin",
            },
        ],
    },
    {
        "id": "T3",
        "name": "Power 3 · Scherzer / Malonga / Gibson",
        "legs": [
            {
                "sport": "MLB",
                "player": "Max Scherzer",
                "search": "Max Scherzer",
                "prop_type": "Hits Allowed",
                "direction": "OVER",
                "line": 2.5,
                "pick_type": "goblin",
            },
            {
                "sport": "WNBA",
                "player": "Dominique Malonga",
                "search": "Malonga",
                "prop_type": "Rebounds",
                "direction": "OVER",
                "line": 5.5,
                "pick_type": "goblin",
            },
            {
                "sport": "TENNIS",
                "player": "Talia Gibson",
                "search": "Talia Gibson",
                "prop_type": "Total Games",
                "direction": "OVER",
                "line": 16.5,
                "pick_type": "goblin",
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


def add_one(frame, page, leg) -> bool:
    switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1400)
    if switched is not None:
        frame = switched
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, "")
    page.wait_for_timeout(200)
    react_search(page, leg["search"])
    page.wait_for_timeout(1500)
    frame = P.find_prizepicks_frame(page)
    ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
    if not ok:
        ok = P.add_leg(frame, page, leg, strict_lines=False, require_line=True)
    try:
        P.set_ticket_type(P.find_prizepicks_frame(page), "power")
    except Exception:
        pass
    return ok


def build(page, ticket: dict) -> dict:
    tid = ticket["id"]
    legs = ticket["legs"]
    print(f"\n======== {tid} {ticket['name']} ========")
    page.bring_to_front()
    first = legs[0]["sport"]
    P.navigate_board_for_sport(page, first, settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    page.wait_for_timeout(500)

    fails = []
    for leg in legs:
        print(
            f"\n--- {leg['sport']} {leg['player']} Goblin {leg['direction']} "
            f"{leg['line']} {leg['prop_type']} ---"
        )
        ok = add_one(frame, page, leg)
        print("  result", "OK" if ok else "FAIL")
        if not ok:
            fails.append(leg["player"])
        frame = P.find_prizepicks_frame(page)
        page.wait_for_timeout(400)

    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
            timeout=1500
        )
    except Exception:
        pass

    slip = P.read_slip(
        P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power"
    ) or {}
    raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
    n = int(slip.get("n_selected") or 0)
    shot = SHOT_DIR / f"pp_g70_{tid.lower()}_2026-09-23.png"
    try:
        page.screenshot(path=str(shot), full_page=False)
    except Exception as exc:
        print(f"  screenshot fail {exc}")
        shot = None
    print(f"[{tid} FINAL] {n}/{len(legs)} fails={fails}")
    print(raw[:900])
    names_ok = all(leg["player"].split()[-1] in raw for leg in legs)
    ready = n >= len(legs) and not fails and names_ok
    return {
        "id": tid,
        "name": ticket["name"],
        "ready": ready,
        "n": n,
        "need": len(legs),
        "fails": fails,
        "raw": raw[:1200],
        "n_correct": slip.get("flex_correct_pays") or slip.get("min_guarantee"),
        "power_min": slip.get("power_min_x") or slip.get("min_guarantee"),
        "shot": str(shot) if shot else "",
    }


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        pages = []
        for _ in TICKETS:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(900)
            pages.append(pg)
        for page, ticket in zip(pages, TICKETS):
            try:
                results.append(build(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append(
                    {
                        "id": ticket["id"],
                        "name": ticket["name"],
                        "ready": False,
                        "n": 0,
                        "need": 3,
                        "fails": [str(e)],
                        "raw": "",
                        "shot": "",
                    }
                )
        if pages:
            pages[0].bring_to_front()

    print("\n======== SUMMARY ========")
    for r in results:
        tag = "READY" if r["ready"] else "INCOMPLETE"
        print(
            f"  {r['id']} {tag} {r['n']}/{r['need']} "
            f"fails={r['fails'] or '-'} payout={r.get('power_min') or r.get('n_correct')}"
        )
    print("No submit — confirm 3-correct 2x on each slip, then Submit in Chrome.")
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
