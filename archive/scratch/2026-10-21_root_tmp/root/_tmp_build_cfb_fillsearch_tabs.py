"""Build CFB L5=5 Goblin P3/F4 in separate tabs using Playwright fill-search."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board?league_id=15"
OUT = _REPO / "data" / "reports" / "cfb_sat_tabs_ready_2026-09-26.json"

TICKETS = [
    {
        "id": "P3-1",
        "mode": "power",
        "legs": [
            {
                "sport": "CFB",
                "player": "Arch Manning",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 174.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TEX",
            },
            {
                "sport": "CFB",
                "player": "Julian Sayin",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 209.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "OSU",
            },
            {
                "sport": "CFB",
                "player": "J'Koby Williams",
                "prop": "Rush Yards",
                "prop_type": "Rush Yards",
                "line": 49.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TTU",
            },
        ],
    },
    {
        "id": "P3-2",
        "mode": "power",
        "legs": [
            {
                "sport": "CFB",
                "player": "Avery Johnson",
                "prop": "Rush Yards",
                "prop_type": "Rush Yards",
                "line": 19.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "KSU",
            },
            {
                "sport": "CFB",
                "player": "Arch Manning",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 174.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TEX",
            },
            {
                "sport": "CFB",
                "player": "Julian Sayin",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 209.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "OSU",
            },
        ],
    },
    {
        "id": "F4-1",
        "mode": "flex",
        "legs": [
            {
                "sport": "CFB",
                "player": "Avery Johnson",
                "prop": "Rush Yards",
                "prop_type": "Rush Yards",
                "line": 19.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "KSU",
            },
            {
                "sport": "CFB",
                "player": "Arch Manning",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 174.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TEX",
            },
            {
                "sport": "CFB",
                "player": "Julian Sayin",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 209.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "OSU",
            },
            {
                "sport": "CFB",
                "player": "J'Koby Williams",
                "prop": "Rush Yards",
                "prop_type": "Rush Yards",
                "line": 49.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TTU",
            },
        ],
    },
]


def strong_search(frame, page, player: str) -> int:
    """Fill search so the board narrows. Returns card count after search."""
    for sel in [
        "input[placeholder*='Search']",
        "input[type='search']",
        "input[aria-label*='Search']",
        "input[aria-label='search']",
        "input[aria-label='search']",
    ]:
        box = frame.locator(sel).first
        try:
            if box.count() == 0:
                continue
            box.click(force=True, timeout=1200)
            box.fill("", force=True, timeout=1200)
            page.wait_for_timeout(150)
            box.fill(player, force=True, timeout=1500)
            try:
                box.press("Enter", timeout=500)
            except Exception:
                pass
            page.wait_for_timeout(1200)
            break
        except Exception:
            continue
    cards = P.get_all_cards(frame)
    print(f"[SEARCH] {player!r} -> {len(cards)} cards", flush=True)
    return len(cards)


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for leg in legs:
        print(f"  {leg['player']} {leg['prop']} {leg['line']}", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, leg["prop"])
        page.wait_for_timeout(500)
        frame = P.find_prizepicks_frame(page)
        n_cards = strong_search(frame, page, leg["player"])
        if n_cards > 40:
            # retry search once
            strong_search(frame, page, leg["player"].split()[-1])
            page.wait_for_timeout(400)
            strong_search(frame, page, leg["player"])
            frame = P.find_prizepicks_frame(page)
            n_cards = len(P.get_all_cards(frame))
            print(f"[SEARCH] retry count={n_cards}", flush=True)
        if n_cards > 40:
            print(
                f"DROPPED: SEARCH WIDE {leg['player']} cards={n_cards}",
                flush=True,
            )
            try:
                P.clear_slip(frame)
            except Exception:
                pass
            return {
                "id": tid,
                "ready": False,
                "n": 0,
                "px": None,
                "fails": [f"search:{leg['player']}"],
            }
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(frame)
            except Exception:
                pass
            return {
                "id": tid,
                "ready": False,
                "n": 0,
                "px": None,
                "fails": [leg["player"]],
            }
        time.sleep(0.2)
    try:
        label = "Flex Play" if mode == "flex" else "Power Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(
            timeout=2000
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
    print(raw[:450], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_cfb_{tid.lower()}_ready_2026-09-26.png"
    try:
        page.screenshot(path=str(shot), full_page=False)
    except Exception:
        pass
    return {
        "id": tid,
        "ready": ready,
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["player"], x["prop"], x["line"]) for x in legs],
        "raw": raw[:600],
    }


def main() -> int:
    results = []
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [
            pg
            for pg in ctx.pages
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
        ]
        if not boards:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            boards = [pg]
        while len(boards) < len(TICKETS):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(900)
            boards.append(pg)
            print(f"opened tab {len(boards)}", flush=True)
        for page, ticket in zip(boards, TICKETS):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]}
                )
        for page, r in zip(boards, results):
            if r.get("ready"):
                page.bring_to_front()
                print(f"front {r['id']}", flush=True)
                break
    OUT.write_text(
        json.dumps({"tickets": TICKETS, "results": results}, indent=2, default=str),
        encoding="utf-8",
    )
    print("\nSUMMARY", flush=True)
    for r in results:
        print(
            f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} "
            f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
            flush=True,
        )
    print(f"wrote {OUT}", flush=True)
    print("No submit.", flush=True)
    return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
