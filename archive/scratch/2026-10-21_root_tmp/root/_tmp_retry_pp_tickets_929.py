#!/usr/bin/env python3
"""Retry remaining 2026-09-29 PP slips with better Goblin cycling. No submit."""
from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import n_correct_power, prop_ok, react_search, _norm

spec = importlib.util.spec_from_file_location(
    "pp929", _REPO / "_tmp_build_pp_tickets_929.py"
)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = Path(r"H:/PropORACLE_main_cp/data/reports/pp_retry_live_tickets_2026-09-29.json")

# Skip the already-built Standard Power 3 (Norrie/Boston/Courtney).
TICKETS = [
    {
        "id": "S3-Ionescu-PRA-Boston-Courtney",
        "note": "Replace collapsed Ionescu triple with PRA + other standards",
        "legs": [
            {"sport": "WNBA", "player": "Sabrina Ionescu", "prop": "Pts+Rebs+Asts", "prop_type": "PRA", "direction": "UNDER", "line": 25.0, "pick_type": "standard"},
            {"sport": "WNBA", "player": "Aliyah Boston", "prop": "Defensive Rebounds", "prop_type": "Defensive Rebounds", "direction": "UNDER", "line": 6.5, "pick_type": "standard"},
            {"sport": "WNBA", "player": "Courtney Williams", "prop": "Pts+Rebs", "prop_type": "Pts+Rebs", "direction": "UNDER", "line": 16.5, "pick_type": "standard"},
        ],
    },
    {
        "id": "M2-Tiafoe-IonescuPRA",
        "legs": [
            {"sport": "TENNIS", "player": "Frances Tiafoe", "prop": "Total Games", "prop_type": "Total Games", "direction": "OVER", "line": 19.5, "pick_type": "goblin"},
            {"sport": "WNBA", "player": "Sabrina Ionescu", "prop": "Pts+Rebs+Asts", "prop_type": "PRA", "direction": "UNDER", "line": 25.0, "pick_type": "standard"},
        ],
    },
    {
        "id": "G2-Bondar-BoydOuts",
        "legs": [
            {"sport": "TENNIS", "player": "Anna Bondar", "prop": "Total Games", "prop_type": "Total Games", "direction": "OVER", "line": 19.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Matthew Boyd", "prop": "Pitching Outs", "prop_type": "Pitching Outs", "direction": "OVER", "line": 12.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "G2-Zverev-Paul",
        "legs": [
            {"sport": "TENNIS", "player": "Alexander Zverev", "prop": "Total Games", "prop_type": "Total Games", "direction": "OVER", "line": 18.5, "pick_type": "goblin"},
            {"sport": "TENNIS", "player": "Tommy Paul", "prop": "Total Games", "prop_type": "Total Games", "direction": "OVER", "line": 21.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "G2-Baturina-Sucic",
        "legs": [
            {"sport": "Soccer", "player": "Martin Baturina", "prop": "Shots", "prop_type": "Shots", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "Soccer", "player": "Luka Sucic", "prop": "Shots", "prop_type": "Shots", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
]


def probe_tennis(page, player: str) -> None:
    P.navigate_board_for_sport(page, "TENNIS", settle_ms=1500)
    frame = P.find_prizepicks_frame(page)
    try:
        P._switch_board_filter(frame, page, "Total Games")
    except Exception:
        pass
    react_search(page, player)
    page.wait_for_timeout(1200)
    frame = P.find_prizepicks_frame(page)
    cards = [
        c
        for c in P.get_all_cards(frame)
        if _norm(player) in _norm(c.get("player")) or _norm(c.get("player")) in _norm(player)
    ]
    print(
        f"PROBE {player}: "
        f"{[(c.get('prop_type'), c.get('line'), c.get('pick_type')) for c in cards[:10]]}",
        flush=True,
    )


def add_with_goblin_cycle(page, leg: dict) -> tuple[bool, str, dict]:
    ok, why, live = mod.add_leg_now(page, leg)
    if ok:
        return ok, why, live
    # If goblin missing, try any same-prop card then cycle harder
    frame = P.find_prizepicks_frame(page)
    P.navigate_board_for_sport(page, leg["sport"], settle_ms=1400)
    frame = P.find_prizepicks_frame(page)
    chip = P.prop_type_to_board_filter(leg["prop_type"])
    try:
        P._switch_board_filter(frame, page, chip)
    except Exception:
        pass
    react_search(page, leg["player"])
    page.wait_for_timeout(1400)
    frame = P.find_prizepicks_frame(page)
    cards = [
        c
        for c in P.get_all_cards(frame)
        if (
            _norm(leg["player"]) in _norm(c.get("player"))
            or _norm(c.get("player")) in _norm(leg["player"])
        )
        and prop_ok(leg["prop"], str(c.get("prop_type") or ""))
    ]
    if not cards:
        cards = [
            c
            for c in P.get_all_cards(frame)
            if _norm(leg["player"]) in _norm(c.get("player"))
            or _norm(c.get("player")) in _norm(leg["player"])
        ]
    if not cards:
        return False, why, leg
    start = cards[0]
    print(
        f"  cycle from {start.get('prop_type')} {start.get('line')} {start.get('pick_type')}",
        flush=True,
    )
    P.cycle_card_to_pick_type(
        frame,
        start.get("more_btn"),
        player=leg["player"],
        prop=start.get("prop_type") or leg["prop"],
        want_pick=leg["pick_type"],
        want_line=leg["line"],
        require_line=False,
        more_index=start.get("more_index"),
        max_clicks=18,
    )
    page.wait_for_timeout(400)
    react_search(page, leg["player"])
    page.wait_for_timeout(1100)
    return mod.add_leg_now(page, leg)


def build_one(page, ticket: dict) -> dict:
    tid = ticket["id"]
    legs = ticket["legs"]
    print(f"\n======== {tid} ========", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=1700)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")

    used = []
    for i, leg in enumerate(legs):
        if i > 0:
            switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1300)
            if switched is None:
                P.navigate_board_for_sport(page, leg["sport"], settle_ms=1500)
            frame = P.find_prizepicks_frame(page)
        ok, why, live = add_with_goblin_cycle(page, leg)
        if not ok:
            print(f"DROPPED: {why}", flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [why], "n": 0, "legs": used}
        used.append(live)
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power") or {}
        print(f"  slip n={slip.get('n_selected')} after {live.get('player')}", flush=True)
        time.sleep(0.2)

    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception:
        pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    print(f"[{tid}] slip_ok={n == len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:300], flush=True)
    return {"id": tid, "ok": n == len(legs), "n": n, "want_n": len(legs), "px": px, "fails": [], "legs": used, "raw_head": raw[:400]}


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page0 = next(
            (p for p in ctx.pages if "prizepicks.com" in (p.url or "") and "board" in (p.url or "")),
            None,
        )
        if page0 is None:
            page0 = ctx.new_page()
            page0.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
        for name in ("Frances Tiafoe", "Anna Bondar", "Alexander Zverev", "Tommy Paul"):
            probe_tennis(page0, name)

        results = []
        for i, ticket in enumerate(TICKETS):
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(700)
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append({"id": ticket["id"], "ok": False, "fails": [str(e)], "n": 0})
            time.sleep(0.3)

        OUT.write_text(json.dumps({"results": results}, indent=2, default=str), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        ok_n = 0
        for r in results:
            print(
                f"  {r.get('id')} ok={r.get('ok')} n={r.get('n')}/{r.get('want_n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')}",
                flush=True,
            )
            if r.get("ok"):
                ok_n += 1
        print(f"wrote {OUT}. Built {ok_n}/{len(results)}. No submit.", flush=True)
        print("Also keep the earlier Norrie/Boston/Courtney tab if still open.", flush=True)
        return 0 if ok_n else 2


if __name__ == "__main__":
    raise SystemExit(main())
