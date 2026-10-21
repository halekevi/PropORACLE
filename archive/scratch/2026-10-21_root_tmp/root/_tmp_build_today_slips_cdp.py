#!/usr/bin/env python3
"""Build today's listed slips on PrizePicks CDP tabs. No submit."""
from __future__ import annotations

import json
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
OUT = _REPO / "data" / "reports" / "pp_today_slips_2026-09-27.json"

M.PROP_ALIASES.update(
    {
        "pitchingouts": ("pitchingouts", "po"),
        "po": ("pitchingouts", "po"),
        "hitsrunsrbis": ("hitsrunsrbis", "hrrbi", "hitsrunsribis"),
        "hrrbi": ("hitsrunsrbis", "hrrbi"),
        "earnedrunsallowed": ("earnedrunsallowed", "earnedruns", "er"),
        "walksallowed": ("walksallowed", "walks", "bb"),
        "totalgames": ("totalgames",),
        "recyards": ("recyards", "receivingyards"),
        "receivingyards": ("recyards", "receivingyards"),
        "fgmade": ("fgmade", "fieldgoalsmade"),
        "longestreception": ("longestreception", "longestrec"),
        "longestrec": ("longestreception", "longestrec"),
        "tacklesast": ("tacklesast", "tacklesasts"),
        "rushyardspercarry": ("rushyardspercarry", "ypc"),
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
    {
        "id": "S1",
        "legs": [
            leg("TENNIS", "James Duckworth", "Total Games", 17.5, "goblin"),
            leg("MLB", "Daniel Lynch IV", "Walks Allowed", 0.5, "goblin"),
            leg("MLB", "MacKenzie Gore", "Walks Allowed", 0.5, "goblin"),
        ],
    },
    {
        "id": "S2",
        "legs": [
            leg("MLB", "Shota Imanaga", "Earned Runs Allowed", 0.5, "goblin"),
            leg("MLB", "Andre Pallante", "Earned Runs Allowed", 0.5, "goblin"),
            leg("MLB", "Michael Soroka", "Earned Runs Allowed", 0.5, "goblin"),
        ],
    },
    {
        "id": "S3",
        "legs": [
            leg("MLB", "Jackson Chourio", "Hits+Runs+RBIs", 0.5, "goblin"),
            leg("MLB", "Yordan Alvarez", "Hits+Runs+RBIs", 0.5, "goblin"),
            leg("MLB", "Max Scherzer", "Pitching Outs", 8.5, "goblin"),
        ],
    },
    {
        "id": "N1",
        "legs": [
            leg("NFL", "Chase McLaughlin", "FG Made", 0.5, "goblin"),
            leg("NFL", "Juwan Johnson", "Rec Yards", 35.5, "standard"),
            leg("NFL", "Tre Tucker", "Longest Rec", 9.5, "goblin"),
        ],
    },
    {
        "id": "N2",
        "legs": [
            leg("NFL", "Ashton Jeanty", "Rush Yards Per Carry", 3.7, "standard"),
            leg("NFL", "Marlon Humphrey", "Tackles+Ast", 2.5, "goblin"),
            leg("NFL", "J.K. Dobbins", "Rush Yards Per Carry", 4.0, "standard"),
        ],
    },
    {
        "id": "N3",
        "legs": [
            leg("NFL", "Blake Corum", "Rush Yards Per Carry", 4.4, "standard"),
            leg("NFL", "Pat Surtain II", "Tackles+Ast", 2.5, "goblin"),
            leg("NFL", "Nate Landman", "Tackles+Ast", 4.5, "goblin"),
        ],
    },
]


def land_sport(page, frame, sport: str, *, allow_goto: bool):
    sp = str(sport or "").strip().upper()
    if allow_goto:
        P.navigate_board_for_sport(page, sp, settle_ms=1500)
        return P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, sp, settle_ms=1200)
    return switched


def build_one(page, ticket: dict) -> dict:
    tid = ticket["id"]
    legs = ticket["legs"]
    print(f"\n======== {tid} ========", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=1500)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")

    for i, item in enumerate(legs):
        frame = P.find_prizepicks_frame(page)
        landed = land_sport(page, frame, item["sport"], allow_goto=(i == 0))
        if landed is None:
            print(f"DROPPED: SPORT TAB MISS {item['player']}", flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [item["player"]], "n": 0}
        frame = landed
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(item["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(500)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, item["player"])
        page.wait_for_timeout(1200)
        result = add_cycled_leg(P, P.find_prizepicks_frame(page), page, item)
        if not result.ok:
            print(result.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {
                "id": tid,
                "ok": False,
                "fails": [item["player"]],
                "reason": result.reason,
                "live_line": result.live_line,
                "n": 0,
            }
        time.sleep(0.2)

    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
            timeout=2000
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power"
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    print(f"[{tid}] slip_ok={n == len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    return {
        "id": tid,
        "ok": n == len(legs),
        "n": n,
        "px": px,
        "fails": [],
        "raw_head": raw[:500],
    }


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        pages = []
        for _ in TICKETS:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(700)
            pages.append(pg)
        results = []
        for pg, ticket in zip(pages, TICKETS):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ok": False, "fails": [str(e)], "n": 0}
                )
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(
            json.dumps({"results": results}, indent=2, default=str),
            encoding="utf-8",
        )
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} ok={r.get('ok')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} "
                f"reason={r.get('reason')} live={r.get('live_line')}",
                flush=True,
            )
        print(f"wrote {OUT}. No submit.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
