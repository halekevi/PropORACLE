"""Fresh tab P3-1 (Sayin) + set existing Avery/Arch/J'Koby slip to Power."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, prop_ok, _norm

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board?league_id=15"
OUT = _REPO / "data" / "reports" / "cfb_sat_tabs_ready_2026-09-26.json"

P3_1 = {
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
}


def yard_ok(want: str, got: str) -> bool:
    if prop_ok(want, got):
        return True
    w, g = _norm(want), _norm(got)
    for a in ("pass", "rush", "rec"):
        if a in w and "yard" in w and a in g and "yard" in g:
            return True
    return False


def popular_search(frame, page, player: str) -> list[dict]:
    try:
        P._switch_board_filter(frame, page, "Popular")
    except Exception:
        pass
    page.wait_for_timeout(400)
    frame = P.find_prizepicks_frame(page)
    for root in (page, frame):
        for sel in [
            "input[placeholder*='Search']",
            "input[type='search']",
            "input[aria-label*='Search']",
            "input[aria-label='search']",
        ]:
            box = root.locator(sel).first
            try:
                if box.count() == 0:
                    continue
                box.click(force=True, timeout=2000)
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.wait_for_timeout(120)
                page.keyboard.type(player, delay=35)
                page.wait_for_timeout(1500)
                frame = P.find_prizepicks_frame(page)
                cards = P.get_all_cards(frame)
                pn = _norm(player)
                hits = [
                    c
                    for c in cards
                    if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
                ]
                print(
                    f"[SEARCH] {player!r} total={len(cards)} hits={len(hits)}",
                    flush=True,
                )
                if hits:
                    return hits
            except Exception as e:
                print(f"[SEARCH] {sel}: {e}", flush=True)
    return []


def add_leg(page, leg: dict) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    hits = popular_search(frame, page, leg["player"])
    if not hits:
        hits = popular_search(frame, page, leg["player"].split()[-1])
    if not hits:
        print(f"NO HITS {leg['player']}", flush=True)
        return False
    start = next(
        (c for c in hits if yard_ok(leg["prop"], str(c.get("prop_type") or ""))),
        hits[0],
    )
    already = None
    for c in hits:
        if not yard_ok(leg["prop"], str(c.get("prop_type") or "")):
            continue
        if str(c.get("pick_type") or "").lower() != "goblin":
            continue
        if P._line_key(c.get("line")) == P._line_key(leg["line"]):
            already = c
            break
    if already is None:
        P.cycle_card_to_pick_type(
            frame,
            start.get("more_btn"),
            player=leg["player"],
            prop=start.get("prop_type") or leg["prop"],
            want_pick="goblin",
            want_line=leg["line"],
            require_line=True,
            more_index=start.get("more_index"),
        )
        page.wait_for_timeout(400)
        frame = P.find_prizepicks_frame(page)
        popular_search(frame, page, leg["player"])
    r = add_cycled_leg(P, frame, page, leg)
    if not r.ok:
        print(r.loud_fail(), flush=True)
        return False
    return True


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} ========", flush=True)
    page.bring_to_front()
    page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(1500)
    P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        print(f"  + {leg['player']} {leg['prop']} {leg['line']}", flush=True)
        if not add_leg(page, leg):
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": 0}
        time.sleep(0.2)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
            timeout=2500
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
    ready = n == len(legs)
    print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
    print(raw[:500], flush=True)
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
        "raw": raw[:700],
    }


def force_power(page) -> dict:
    page.bring_to_front()
    frame = P.find_prizepicks_frame(page)
    try:
        P.set_ticket_type(frame, "power")
    except Exception:
        pass
    try:
        frame.get_by_text("Power Play", exact=True).first.click(timeout=2500)
    except Exception:
        pass
    slip = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    return {"n": n, "px": n_correct_power(raw, n), "raw": raw[:500]}


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [
            pg
            for pg in ctx.pages
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
        ]
        # Identify existing Avery slip
        existing = None
        for pg in boards:
            try:
                raw = str(
                    (
                        P.read_slip(
                            P.find_prizepicks_frame(pg), n_legs=3, ticket_type="power"
                        )
                        or {}
                    ).get("raw_slip_section")
                    or (
                        P.read_slip(
                            P.find_prizepicks_frame(pg), n_legs=3, ticket_type="flex"
                        )
                        or {}
                    ).get("raw_slip_section")
                    or ""
                )
                if "Avery Johnson" in raw and "Arch Manning" in raw:
                    existing = pg
                    break
            except Exception:
                continue
        p32 = None
        if existing is not None:
            print("Found existing P3-2 tab — forcing Power Play", flush=True)
            p32 = force_power(existing)
            print(f"P3-2 after power: n={p32['n']} N-correct={p32['px']}", flush=True)
            print(p32["raw"][:400], flush=True)

        # Fresh page for P3-1
        page = ctx.new_page()
        print("opened fresh tab for P3-1", flush=True)
        r1 = build_one(page, P3_1)
        if r1.get("ready"):
            page.bring_to_front()
        elif existing is not None:
            existing.bring_to_front()

        payload = {
            "P3-1": r1,
            "P3-2": {
                "id": "P3-2",
                "ready": bool(p32 and p32.get("n") == 3),
                "n": None if not p32 else p32.get("n"),
                "px": None if not p32 else p32.get("px"),
                "legs": [
                    ("Avery Johnson", "Rush Yards", 19.5),
                    ("Arch Manning", "Pass Yards", 174.5),
                    ("J'Koby Williams", "Rush Yards", 49.5),
                ],
                "raw": None if not p32 else p32.get("raw"),
            },
        }
        OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        print(
            f"  P3-1 ready={r1.get('ready')} n={r1.get('n')} N-correct={r1.get('px')} "
            f"fails={r1.get('fails')} legs={r1.get('legs')}",
            flush=True,
        )
        print(
            f"  P3-2 ready={payload['P3-2']['ready']} n={payload['P3-2']['n']} "
            f"N-correct={payload['P3-2']['px']} legs={payload['P3-2']['legs']}",
            flush=True,
        )
        print(f"wrote {OUT}", flush=True)
        print("No submit.", flush=True)
        ok = bool(r1.get("ready")) and payload["P3-2"]["ready"]
        return 0 if ok else (0 if payload["P3-2"]["ready"] or r1.get("ready") else 2)


if __name__ == "__main__":
    raise SystemExit(main())
