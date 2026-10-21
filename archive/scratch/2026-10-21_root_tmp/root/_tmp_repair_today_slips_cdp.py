#!/usr/bin/env python3
"""Finish today's CDP slips. Skip any face whose line moved. No submit."""
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
        "hitsrunsrbis": ("hitsrunsrbis", "hrrbi", "hitsrunsribis"),
        "hrrbi": ("hitsrunsrbis", "hrrbi"),
        "earnedrunsallowed": ("earnedrunsallowed", "earnedruns"),
        "walksallowed": ("walksallowed", "walks"),
        "recyards": ("recyards", "receivingyards"),
        "receivingyards": ("recyards", "receivingyards"),
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


NEW = [
    (
        "S1",
        [
            leg("MLB", "Daniel Lynch IV", "Walks Allowed", 0.5, "goblin"),
            leg("MLB", "MacKenzie Gore", "Walks Allowed", 0.5, "goblin"),
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
            leg("NFL", "Juwan Johnson", "Rec Yards", 35.5, "standard"),
            leg("NFL", "Tre Tucker", "Longest Rec", 9.5, "goblin"),
        ],
    ),
    (
        "N2",
        [
            leg("NFL", "Marlon Humphrey", "Tackles+Ast", 2.5, "goblin"),
            leg("NFL", "J.K. Dobbins", "Rush Yards Per Carry", 4.0, "standard"),
        ],
    ),
    (
        "N3",
        [
            leg("NFL", "Pat Surtain II", "Tackles+Ast", 2.5, "goblin"),
            leg("NFL", "Nate Landman", "Tackles+Ast", 4.5, "goblin"),
        ],
    ),
]


def add_leg(page, item) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.navigate_board_for_sport(page, item["sport"], settle_ms=1200)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    chip = P.prop_type_to_board_filter(item["prop_type"])
    P._switch_board_filter(frame, page, chip)
    page.wait_for_timeout(400)
    react_search(page, "")
    page.wait_for_timeout(80)
    react_search(page, item["player"])
    page.wait_for_timeout(1100)
    result = add_cycled_leg(P, P.find_prizepicks_frame(page), page, item)
    if not result.ok:
        print(result.loud_fail(), flush=True)
        return False
    return True


def read(page, n) -> tuple[int, str, object]:
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=n, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    got = int(slip.get("n_selected") or 0)
    return got, raw, n_correct_power(raw, got or n)


def build_fresh(page, tid, legs) -> None:
    print(f"\n======== {tid} ========", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=1200)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    P.set_ticket_type(frame, "power")
    for item in legs:
        if not add_leg(page, item):
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            print(f"[{tid}] cleared after fail", flush=True)
            return
        time.sleep(0.2)
    got, raw, px = read(page, len(legs))
    print(f"[{tid}] n={got}/{len(legs)} N-correct={px}", flush=True)
    print(raw[:350], flush=True)


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        s2 = None
        for pg in ctx.pages:
            if "prizepicks.com" not in (pg.url or ""):
                continue
            try:
                frame = P.find_prizepicks_frame(pg)
                raw = frame.locator("body").inner_text(timeout=4000)
            except Exception:
                continue
            if "Shota Imanaga" in raw and "Michael Soroka" in raw and "Andre Pallante" not in raw:
                s2 = pg
                break
        if s2 is not None:
            print("S2 tab found — adding Pallante", flush=True)
            s2.bring_to_front()
            ok = add_leg(s2, leg("MLB", "Andre Pallante", "Earned Runs Allowed", 0.5, "goblin"))
            got, raw, px = read(s2, 3)
            print(f"[S2] add={ok} n={got} N-correct={px}", flush=True)
            print(raw[:400], flush=True)
        else:
            print("S2 tab not found — rebuilding", flush=True)
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            build_fresh(
                pg,
                "S2",
                [
                    leg("MLB", "Shota Imanaga", "Earned Runs Allowed", 0.5, "goblin"),
                    leg("MLB", "Andre Pallante", "Earned Runs Allowed", 0.5, "goblin"),
                    leg("MLB", "Michael Soroka", "Earned Runs Allowed", 0.5, "goblin"),
                ],
            )
        for tid, legs in NEW:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(600)
            build_fresh(pg, tid, legs)
    print("DONE no submit", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
