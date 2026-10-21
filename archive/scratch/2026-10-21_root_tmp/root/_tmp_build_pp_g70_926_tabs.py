#!/usr/bin/env python3
"""Build 2026-09-26 G70 main card (P3 x5 + F4 x4) in separate PP tabs. No submit."""
from __future__ import annotations

import json
import re
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
G70 = Path(r"H:/PropORACLE_main_cp/data/reports/goblin70_tickets_2026-09-26.json")
OUT = _REPO / "data" / "reports" / "pp_g70_tabs_2026-09-26.json"

PROP_FIX = {
    "rushing_yards": "Rush Yards",
    "receiving_yards": "Rec Yards",
    "passing_yards": "Pass Yards",
    "pass yards": "Pass Yards",
    "rush yards": "Rush Yards",
    "rec yards": "Rec Yards",
    "receiving yards": "Rec Yards",
    "shots on target": "Shots On Target",
    "goalie saves": "Goalie Saves",
    "walks allowed": "Walks Allowed",
    "hits allowed": "Hits Allowed",
    "earned runs allowed": "Earned Runs Allowed",
}


def _prop(raw: str) -> str:
    s = str(raw or "").strip()
    key = re.sub(r"[_\s]+", " ", s).strip().lower()
    if key in PROP_FIX:
        return PROP_FIX[key]
    # title-ish
    if "_" in s:
        return " ".join(w.capitalize() for w in s.split("_"))
    return s


def load_main_tickets() -> list[dict]:
    d = json.loads(G70.read_text(encoding="utf-8"))
    out = []
    for t in d.get("tickets") or []:
        tid = str(t.get("id") or "")
        if not re.match(r"^(P3-|F4-)\d+$", tid):
            continue
        mode = "flex" if tid.startswith("F") else "power"
        legs = []
        for L in t.get("legs") or []:
            legs.append(
                {
                    "sport": str(L.get("sport") or "").upper(),
                    "player": str(L.get("player") or L.get("Player") or "").strip(),
                    "search": str(L.get("player") or L.get("Player") or "").strip(),
                    "prop_type": _prop(L.get("prop") or L.get("Prop") or L.get("prop_type") or ""),
                    "direction": str(
                        L.get("direction") or L.get("side") or L.get("Direction") or "OVER"
                    ).upper(),
                    "line": float(L.get("line") or L.get("Line")),
                    "pick_type": str(
                        L.get("pick_type") or L.get("Pick Type") or "goblin"
                    ).lower(),
                }
            )
        out.append({"id": tid, "mode": mode, "legs": legs, "web_group": t.get("web_group")})
    # stable order P3 then F4
    out.sort(key=lambda z: (0 if z["id"].startswith("P3") else 1, z["id"]))
    return out


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
    print(why, flush=True)
    try:
        P.clear_slip(frame)
        print("[SLIP] cleared partial lineup after abort", flush=True)
    except Exception as e:
        print(f"[SLIP] WARN: partial may remain ({e})", flush=True)


def build_one(page, ticket: dict) -> dict:
    tid = ticket["id"]
    mode = ticket["mode"]
    legs = order_legs_tennis_first(
        [{**leg, "search": leg["player"]} for leg in ticket["legs"]]
    )
    expected = len(legs)
    print(f"\n======== {tid} {mode.upper()} n={expected} ========", flush=True)
    for leg in legs:
        print(
            f"  {leg['sport']} {leg['player']} {leg['prop_type']} "
            f"{leg['direction']}{leg['line']} {leg['pick_type']}",
            flush=True,
        )
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    first = legs[0]
    P.navigate_board_for_sport(page, first["sport"], settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)

    added: list[str] = []
    for i, leg in enumerate(legs):
        print(
            f"\n=== {leg['sport']} {leg['player']} {leg['prop_type']} {leg['line']} ===",
            flush=True,
        )
        frame = P.find_prizepicks_frame(page)
        landed = _land_sport(page, frame, leg["sport"], allow_goto=(i == 0))
        if landed is None:
            _abort_partial(frame, f"DROPPED: SPORT TAB MISS {leg['player']}")
            return {
                "id": tid,
                "ready": False,
                "n": 0,
                "fails": [leg["player"]],
                "px": None,
            }
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
            return {
                "id": tid,
                "ready": False,
                "n": len(added),
                "fails": [leg["player"]],
                "px": None,
            }
        added.append(leg["player"])
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
            P.find_prizepicks_frame(page), n_legs=expected, ticket_type=mode
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    selected = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, selected if selected else expected)
    print(f"[{tid} FINAL] n={selected} {expected}-correct={px}x", flush=True)
    print(raw[:450], flush=True)
    ready = selected == expected
    return {
        "id": tid,
        "ready": ready,
        "n": selected,
        "fails": [],
        "px": px,
        "raw": raw[:500],
        "legs": [
            (x["player"], x["prop_type"], x["line"], x["sport"]) for x in legs
        ],
    }


def main() -> int:
    tickets = load_main_tickets()
    print(f"loaded {len(tickets)} main G70 tickets from {G70}", flush=True)
    if not tickets:
        return 2
    results = []
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        pages = []
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
        for i in range(1, len(tickets)):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1000)
            pages.append(pg)
            print(f"opened tab {i + 1}/{len(tickets)}", flush=True)

        for page, ticket in zip(pages, tickets):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {
                        "id": ticket["id"],
                        "ready": False,
                        "n": 0,
                        "fails": [str(e)],
                        "px": None,
                    }
                )

        for page, r in zip(pages, results):
            if r.get("ready"):
                try:
                    page.bring_to_front()
                except Exception:
                    pass
                break

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"results": results}, indent=2, default=str), encoding="utf-8")
    print("\n======== SUMMARY ========", flush=True)
    for r in results:
        print(
            f"  {r['id']} ready={r.get('ready')} n={r.get('n')} "
            f"N-correct={r.get('px')} fails={r.get('fails')}",
            flush=True,
        )
    print(f"Tabs open: {len(tickets)}. No submit. wrote {OUT}", flush=True)
    return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
