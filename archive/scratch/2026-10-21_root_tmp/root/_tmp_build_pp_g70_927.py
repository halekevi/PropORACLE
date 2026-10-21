#!/usr/bin/env python3
"""Build live 2026-09-27 Goblin-70 Power 3 slips on PrizePicks CDP. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
TICKETS = _REPO / "ui_runner" / "runtime" / "tickets_latest.json"
OUT = _REPO / "data" / "reports" / "pp_g70_built_2026-09-27.json"


def load_power3() -> list[dict]:
    d = json.loads(TICKETS.read_text(encoding="utf-8"))
    out = []
    for g in d.get("groups") or []:
        name = str(g.get("group_name") or "")
        if "Goblin-70 Power 3" not in name:
            continue
        for t in g.get("tickets") or []:
            legs = []
            for leg in t.get("legs") or []:
                legs.append(
                    {
                        "sport": str(leg.get("sport") or "").strip(),
                        "player": str(leg.get("player") or "").strip(),
                        "search": str(leg.get("player") or "").strip(),
                        "prop_type": str(leg.get("prop_type") or "").strip(),
                        "prop": str(leg.get("prop_type") or "").strip(),
                        "direction": str(leg.get("direction") or "OVER").upper(),
                        "side": str(leg.get("direction") or "OVER").upper(),
                        "line": float(leg["line"]),
                        "pick_type": str(leg.get("pick_type") or "goblin").lower(),
                    }
                )
            out.append(
                {
                    "id": t.get("ticket_id") or t.get("ticket_no"),
                    "mode": "power",
                    "legs": legs,
                }
            )
    return out


def land_sport(page, frame, sport: str, *, allow_goto: bool):
    sp = str(sport or "").strip().upper()
    if sp == "SOCCER":
        sp = "SOCCER"
    if allow_goto:
        P.navigate_board_for_sport(page, sp, settle_ms=1800)
        return P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, sp, settle_ms=1500)
    return switched


def build_one(page, ticket: dict) -> dict:
    tid = ticket["id"]
    legs = ticket["legs"]
    print(f"\n======== {tid} ========", flush=True)
    for x in legs:
        print(
            f"  {x['sport']} {x['player']} {x['prop_type']} "
            f"{x['direction']} {x['line']} ({x['pick_type']})",
            flush=True,
        )
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    first = legs[0]
    P.navigate_board_for_sport(page, first["sport"], settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")

    for i, leg in enumerate(legs):
        frame = P.find_prizepicks_frame(page)
        landed = land_sport(page, frame, leg["sport"], allow_goto=(i == 0))
        if landed is None:
            print(f"DROPPED: SPORT TAB MISS {leg['player']}", flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [leg["player"]], "n": 0}
        frame = landed
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(700)
        react_search(page, "")
        page.wait_for_timeout(120)
        react_search(page, leg["player"])
        page.wait_for_timeout(1300)
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [leg["player"]], "n": 0}
        time.sleep(0.25)

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
    print(raw[:450], flush=True)
    return {
        "id": tid,
        "ok": n == len(legs),
        "n": n,
        "px": px,
        "fails": [],
        "legs": [
            (x["sport"], x["player"], x["prop_type"], x["line"]) for x in legs
        ],
        "raw_head": raw[:500],
    }


def main() -> int:
    tickets = load_power3()
    print(f"Power 3 slips to build: {len(tickets)}", flush=True)
    if not tickets:
        print("No Goblin-70 Power 3 groups on tickets_latest", flush=True)
        return 2

    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page0 = next(
            (
                pg
                for pg in ctx.pages
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
            ),
            None,
        )
        if page0 is None:
            page0 = ctx.new_page()
            page0.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page0.wait_for_timeout(1200)

        pages = [page0]
        while len(pages) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(800)
            pages.append(pg)

        results = []
        for pg, ticket in zip(pages, tickets):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ok": False, "fails": [str(e)], "n": 0}
                )

        OUT.write_text(
            json.dumps({"tickets": tickets, "results": results}, indent=2, default=str),
            encoding="utf-8",
        )
        print("\nSUMMARY", flush=True)
        ok_n = 0
        for r in results:
            print(
                f"  {r.get('id')} ok={r.get('ok')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')}",
                flush=True,
            )
            if r.get("ok"):
                ok_n += 1
        print(f"wrote {OUT}. Built {ok_n}/{len(results)}. No submit.", flush=True)
        return 0 if ok_n == len(results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
