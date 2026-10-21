#!/usr/bin/env python3
"""Build 2026-09-25 G70 slips in separate PrizePicks tabs. Does not submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.pp_manual_slip import (
    add_cycled_leg,
    n_correct_power,
    order_legs_tennis_first,
    react_search,
)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"

TICKETS = [
    {
        "id": "P3-1",
        "mode": "power",
        "legs": [
            {"sport": "MLB", "player": "Robert Gasser", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "TENNIS", "player": "Daniil Medvedev", "prop_type": "Total Games", "direction": "OVER", "line": 18.5, "pick_type": "goblin"},
            {"sport": "SOCCER", "player": "Michael Olise", "prop_type": "Shots On Target", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "P3-2",
        "mode": "power",
        "legs": [
            {"sport": "MLB", "player": "Hunter Brown", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "SOCCER", "player": "Alan Vega", "prop_type": "Shots", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Freddy Peralta", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "P3-3",
        "mode": "power",
        "legs": [
            {"sport": "MLB", "player": "Jacob Lopez", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "SOCCER", "player": "Estéban Lepaul", "prop_type": "Shots On Target", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Yordan Alvarez", "prop_type": "Hits+Runs+RBIs", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "P3-4",
        "mode": "power",
        "legs": [
            {"sport": "MLB", "player": "Tomoyuki Sugano", "prop_type": "Earned Runs Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "SOCCER", "player": "Camilo Vargas", "prop_type": "Goalie Saves", "direction": "OVER", "line": 1.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Nick Lodolo", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "P3-5",
        "mode": "power",
        "legs": [
            {"sport": "MLB", "player": "Michael McGreevy", "prop_type": "Hits Allowed", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Tarik Skubal", "prop_type": "Hits Allowed", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Eury Pérez", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "F4-1",
        "mode": "flex",
        "legs": [
            {"sport": "MLB", "player": "Jackson Jobe", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Noah Cameron", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Alec Gamboa", "prop_type": "Hits Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Clay Holmes", "prop_type": "Hits Allowed", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "F4-2",
        "mode": "flex",
        "legs": [
            {"sport": "MLB", "player": "Gavin Williams", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 4.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Sean Burke", "prop_type": "Hits Allowed", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Cristopher Sánchez", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 3.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Bubba Chandler", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "F4-3",
        "mode": "flex",
        "legs": [
            {"sport": "MLB", "player": "Jacob deGrom", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Reid Detmers", "prop_type": "Pitches Thrown", "direction": "OVER", "line": 83.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Joe Ryan", "prop_type": "Walks Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Trevor Rogers", "prop_type": "Earned Runs Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
]


def _land_sport(page, frame, sport: str, *, allow_goto: bool):
    sp = str(sport or "").strip().upper()
    if allow_goto:
        P.navigate_board_for_sport(page, sp, settle_ms=1800)
        return P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, sp, settle_ms=1500)
    if switched is None:
        print(f"[SLIP] sport tab miss {sp} — not using goto (would drop slip)")
        return None
    return switched


def _abort_partial(frame, why: str) -> None:
    print(why)
    try:
        P.clear_slip(frame)
        print("[SLIP] cleared partial lineup after abort")
    except Exception as e:
        print(f"[SLIP] WARN: partial may remain ({e})")


def build_one(page, ticket: dict) -> dict:
    tid = ticket["id"]
    mode = ticket["mode"]
    legs = order_legs_tennis_first(
        [{**leg, "search": leg["player"]} for leg in ticket["legs"]]
    )
    expected = len(legs)
    print(f"\n======== {tid} {mode.upper()} n={expected} ========")
    page.bring_to_front()
    first = legs[0]
    P.navigate_board_for_sport(page, first["sport"], settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)

    added: list[str] = []
    for i, leg in enumerate(legs):
        print(f"\n=== {leg['sport']} {leg['player']} {leg['prop_type']} {leg['line']} ===")
        frame = P.find_prizepicks_frame(page)
        landed = _land_sport(page, frame, leg["sport"], allow_goto=(i == 0))
        if landed is None:
            _abort_partial(frame, f"DROPPED: SPORT TAB MISS {leg['player']}")
            return {"id": tid, "ready": False, "n": 0, "fails": [leg["player"]], "px": None}
        frame = landed
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(800)
        react_search(page, "")
        page.wait_for_timeout(150)
        react_search(page, leg["search"])
        page.wait_for_timeout(1400)
        frame = P.find_prizepicks_frame(page)
        result = add_cycled_leg(P, frame, page, leg)
        if not result.ok:
            _abort_partial(frame, result.loud_fail())
            return {"id": tid, "ready": False, "n": len(added), "fails": [leg["player"]], "px": None}
        added.append(leg["player"])
        time.sleep(0.3)

    try:
        label = "Flex Play" if mode == "flex" else "Power Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(timeout=2000)
    except Exception:
        pass
    slip = P.read_slip(
        P.find_prizepicks_frame(page), n_legs=expected, ticket_type=mode
    ) or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    selected = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, selected if selected else expected)
    print(f"[{tid} FINAL] n={selected} {expected}-correct={px}x")
    print(raw[:500])
    ready = selected == expected and all(name.split()[-1] in raw for name in added)
    if not ready:
        print(f"[{tid}] not ready selected={selected} expected={expected}")
    return {"id": tid, "ready": ready, "n": selected, "fails": [], "px": px, "raw": raw[:400]}


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        pages = []
        # Prefer existing board tab for first slip; open fresh boards for the rest.
        board = None
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                board = pg
                break
        if board is None:
            board = ctx.new_page()
            board.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            board.wait_for_timeout(1500)
        pages.append(board)
        for i in range(1, len(TICKETS)):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            pages.append(pg)
            print(f"opened tab {i + 1}/{len(TICKETS)}")

        for page, ticket in zip(pages, TICKETS):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append(
                    {"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)], "px": None}
                )

        try:
            pages[0].bring_to_front()
        except Exception:
            pass

    print("\n======== SUMMARY ========")
    for r in results:
        print(
            f"  {r['id']} ready={r.get('ready')} n={r.get('n')} "
            f"N-correct={r.get('px')} fails={r.get('fails')}"
        )
    print(f"Tabs open: {len(TICKETS)}. No submit.")
    return 0 if all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
