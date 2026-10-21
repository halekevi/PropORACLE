"""Finish CFB tab card: READY P3-1 (Sayin) + verify P3-2 still up."""
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
    page.wait_for_timeout(350)
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
                box.click(force=True, timeout=1500)
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.wait_for_timeout(100)
                page.keyboard.type(player, delay=40)
                page.wait_for_timeout(1600)
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
                print(f"[SEARCH] try fail {sel}: {e}", flush=True)
                continue
    return []


def add_leg(page, leg: dict) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    hits = popular_search(frame, page, leg["player"])
    if not hits:
        # last-name fallback
        last = leg["player"].split()[-1]
        hits = popular_search(frame, page, last)
    if not hits:
        print(f"NO HITS {leg['player']}", flush=True)
        return False
    start = next(
        (c for c in hits if yard_ok(leg["prop"], str(c.get("prop_type") or ""))),
        hits[0],
    )
    idx = start.get("more_index")
    if idx is not None:
        try:
            frame.locator("body").evaluate(
                """(i) => {
                  const btns = Array.from(document.querySelectorAll('button')).filter(b => {
                    const t = (b.innerText || '').trim();
                    return t === 'More' || t === '+ More';
                  });
                  const el = btns[i];
                  if (el) el.scrollIntoView({block:'center'});
                  return !!el;
                }""",
                int(idx),
            )
            page.wait_for_timeout(250)
        except Exception:
            pass
    frame = P.find_prizepicks_frame(page)
    # If already on exact goblin, skip cycle noise
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
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
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
    print(raw[:400], flush=True)
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
        # Prefer a tab that is NOT already holding a filled slip if possible —
        # use first board for P3-1 rebuild.
        page = boards[0]
        r1 = build_one(page, P3_1)
        # Peek other tabs for existing ready slips
        others = []
        for pg in boards[1:]:
            try:
                slip = P.read_slip(P.find_prizepicks_frame(pg), n_legs=3, ticket_type="power") or {}
                n = int(slip.get("n_selected") or 0)
                raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
                others.append({"n": n, "px": n_correct_power(raw, n), "raw": raw[:300]})
            except Exception as e:
                others.append({"err": str(e)})
        payload = {"P3-1": r1, "other_tabs": others}
        OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        print(
            f"  P3-1 ready={r1.get('ready')} n={r1.get('n')} N-correct={r1.get('px')} "
            f"fails={r1.get('fails')} legs={r1.get('legs')}",
            flush=True,
        )
        for i, o in enumerate(others, 1):
            print(f"  tab{i+1} {o}", flush=True)
        if r1.get("ready"):
            page.bring_to_front()
        print(f"wrote {OUT}", flush=True)
        print("No submit.", flush=True)
        return 0 if r1.get("ready") else 2


if __name__ == "__main__":
    raise SystemExit(main())
