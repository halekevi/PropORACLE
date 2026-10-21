#!/usr/bin/env python3
"""Rebuild today's PrizePicks slips on CDP. Exact lines only. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
import utils.pp_manual_slip as M
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"

M.PROP_ALIASES.update(
    {
        "pitchingouts": ("pitchingouts", "po"),
        "po": ("pitchingouts", "po"),
        "hitsrunsrbis": ("hitsrunsrbis", "hrrbi"),
        "hrrbi": ("hitsrunsrbis", "hrrbi"),
        "earnedrunsallowed": ("earnedrunsallowed", "earnedruns"),
        "walksallowed": ("walksallowed", "walks"),
        "totalgames": ("totalgames",),
        "recyards": ("recyards", "receivingyards"),
        "receivingyards": ("recyards", "receivingyards"),
        "fgmade": ("fgmade",),
        "longestreception": ("longestreception", "longestrec"),
        "longestrec": ("longestreception", "longestrec"),
        "tacklesast": ("tacklesast",),
        "rushyardspercarry": ("rushyardspercarry",),
    }
)


def leg(sport, player, prop, line, pick):
    return {
        "sport": sport,
        "player": player,
        "search": player,
        "prop_type": prop,
        "prop": prop,
        "direction": "OVER",
        "side": "OVER",
        "line": float(line),
        "pick_type": pick,
    }


TICKETS = [
    (
        "S1",
        [
            leg("TENNIS", "James Duckworth", "Total Games", 17.5, "goblin"),
            leg("MLB", "Daniel Lynch IV", "Walks Allowed", 0.5, "goblin"),
            leg("MLB", "MacKenzie Gore", "Walks Allowed", 0.5, "goblin"),
        ],
    ),
    (
        "S2",
        [
            leg("MLB", "Shota Imanaga", "Earned Runs Allowed", 0.5, "goblin"),
            leg("MLB", "Andre Pallante", "Earned Runs Allowed", 0.5, "goblin"),
            leg("MLB", "Michael Soroka", "Earned Runs Allowed", 0.5, "goblin"),
        ],
    ),
    (
        "S3",
        [
            leg("MLB", "Jackson Chourio", "Hits+Runs+RBIs", 0.5, "goblin"),
            leg("MLB", "Yordan Alvarez", "Hits+Runs+RBIs", 0.5, "goblin"),
            leg("MLB", "Max Scherzer", "Pitching Outs", 8.5, "goblin"),
        ],
    ),
    (
        "N1",
        [
            leg("NFL", "Chase McLaughlin", "FG Made", 0.5, "goblin"),
            leg("NFL", "Juwan Johnson", "Rec Yards", 35.5, "standard"),
            leg("NFL", "Tre Tucker", "Longest Rec", 9.5, "goblin"),
        ],
    ),
    (
        "N2",
        [
            leg("NFL", "Ashton Jeanty", "Rush Yards Per Carry", 3.7, "standard"),
            leg("NFL", "Marlon Humphrey", "Tackles+Ast", 2.5, "goblin"),
            leg("NFL", "J.K. Dobbins", "Rush Yards Per Carry", 4.0, "standard"),
        ],
    ),
    (
        "N3",
        [
            leg("NFL", "Blake Corum", "Rush Yards Per Carry", 4.4, "standard"),
            leg("NFL", "Pat Surtain II", "Tackles+Ast", 2.5, "goblin"),
            leg("NFL", "Nate Landman", "Tackles+Ast", 4.5, "goblin"),
        ],
    ),
]


def add_one(page, item) -> object:
    P.navigate_board_for_sport(page, item["sport"], settle_ms=900)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, "")
    page.wait_for_timeout(120)
    react_search(page, item["player"])
    page.wait_for_timeout(1300)
    return add_cycled_leg(P, P.find_prizepicks_frame(page), page, item)


def lock_slip_mores(page) -> None:
    frame = P.find_prizepicks_frame(page)
    try:
        frame.locator("body").evaluate(
            """() => {
              const text = document.body.innerText || '';
              if (!text.includes('Choose more/less')) return 0;
              const root = [...document.querySelectorAll('div,aside,section')]
                .filter(e => (e.innerText || '').includes('Current Lineup')
                  && (e.innerText || '').includes('Choose more/less'))
                .sort((a, b) => a.innerText.length - b.innerText.length)[0];
              if (!root) return 0;
              let n = 0;
              for (const b of root.querySelectorAll('button')) {
                if ((b.innerText || '').trim() !== 'More') continue;
                const pressed = b.getAttribute('aria-pressed');
                if (pressed === 'true') continue;
                b.click();
                n += 1;
              }
              return n;
            }"""
        )
    except Exception as e:
        print(f"[MORE] {e}", flush=True)
    page.wait_for_timeout(500)


def build(page, tid, legs) -> dict:
    print(f"\n======== {tid} ========", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=900)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    P.set_ticket_type(frame, "power")
    for item in legs:
        result = add_one(page, item)
        if not result.ok:
            print(result.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {
                "id": tid,
                "ok": False,
                "reason": result.reason,
                "player": item["player"],
                "live": result.live_line,
            }
        time.sleep(0.15)
    P.set_ticket_type(P.find_prizepicks_frame(page), "power")
    lock_slip_mores(page)
    P.set_ticket_type(P.find_prizepicks_frame(page), "power")
    slip = (
        P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power")
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n or len(legs))
    print(f"[{tid}] n={n}/{len(legs)} N-correct={px}", flush=True)
    print(raw[:420], flush=True)
    return {"id": tid, "ok": n == len(legs), "n": n, "px": px}


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=60_000)
        ctx = browser.contexts[0]
        results = []
        # S1 is already on a live tab. Build the rest one new tab at a time.
        for tid, legs in TICKETS[1:]:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(600)
            try:
                results.append(build(pg, tid, legs))
            except Exception as e:
                print(f"{tid} EXC {e}", flush=True)
                results.append({"id": tid, "ok": False, "reason": str(e)})
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} ok={r.get('ok')} n={r.get('n')} "
                f"N-correct={r.get('px')} fail={r.get('player')} "
                f"{r.get('reason')} live={r.get('live')}",
                flush=True,
            )
        print("No submit.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
