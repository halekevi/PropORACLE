#!/usr/bin/env python3
"""Retry failed NFL 9/27 PP tabs with Recs alias + leftover live yard legs. No submit."""
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
PREV = _REPO / "data" / "reports" / "nfl_pp_tabs_2026-09-27.json"
OUT = _REPO / "data" / "reports" / "nfl_pp_tabs_2026-09-27_retry.json"


def leg(player, prop, line, team, **extra):
    return {
        "player": player,
        "team": team,
        "prop": prop,
        "prop_type": prop,
        "line": float(line),
        "pick_type": "goblin",
        "direction": "OVER",
        "side": "OVER",
        "sport": "NFL",
        "search": player,
        **extra,
    }


def search_player(page, player: str) -> None:
    react_search(page, player)
    page.wait_for_timeout(1400)


# Prefer yard faces that probed LIVE; Recs for reception lines (board label).
TICKETS = [
    {
        "id": "P3-3b",
        "mode": "power",
        "legs": [
            leg("Emanuel Wilson", "Rush Yards", 10.5, "SEA"),
            leg("Mike Gesicki", "Rec Yards", 9.5, "CIN"),
            leg("Kenneth Walker III", "Rush Yards", 49.5, "KC"),
        ],
    },
    {
        "id": "P3-4b",
        "mode": "power",
        "legs": [
            leg("Cooper Kupp", "Recs", 1.5, "SEA"),
            leg("Darnell Mooney", "Rec Yards", 4.5, "NYG"),
            leg("Kyler Murray", "Pass Yards", 149.5, "MIN"),
        ],
    },
    {
        "id": "F4-2b",
        "mode": "flex",
        "legs": [
            leg("Malachi Fields", "Rec Yards", 4.5, "NYG"),
            leg("David Montgomery", "Rec Yards", 4.5, "HOU"),
            leg("Foster Moreau", "Recs", 0.5, "HOU"),
            leg("Erick All Jr.", "Recs", 0.5, "CIN"),
        ],
    },
]


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for x in legs:
        print(f"  {x['player']} {x['prop']} {x['line']} ({x['team']})", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, "NFL", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for x in legs:
        search_player(page, x["player"])
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, x)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [x["player"]], "n": 0}
        time.sleep(0.25)
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
    print(f"[{tid}] slip_ok={n == len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:500], flush=True)
    return {
        "id": tid,
        "ready": n == len(legs),
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["player"], x["prop"], x["line"], x["team"]) for x in legs],
        "raw_head": raw[:500],
        "note": "NFL Goblin engine off — slip built, not READY-labeled",
    }


def main() -> int:
    prev = json.loads(PREV.read_text(encoding="utf-8")) if PREV.is_file() else {}
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = next(
            (
                pg
                for pg in ctx.pages
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
            ),
            None,
        )
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1200)
        results = []
        for t in TICKETS:
            try:
                results.append(build_one(page, t))
            except Exception as e:
                print(f"{t['id']} EXC {e}", flush=True)
                results.append(
                    {"id": t["id"], "ready": False, "fails": [str(e)], "n": 0}
                )
        payload = {
            "prior_ok": [r for r in prev.get("results", []) if r.get("ready")],
            "retry_tickets": TICKETS,
            "results": results,
        }
        OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} slip_ok={r.get('ready')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
                flush=True,
            )
        print(f"wrote {OUT}", flush=True)
        return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
