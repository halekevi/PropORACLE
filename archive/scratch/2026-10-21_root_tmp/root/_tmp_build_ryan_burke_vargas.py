"""Build Joe Ryan ERA / Sean Burke Walks / Miguel Vargas H+R+RBI L5=5 Power 3. No submit."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
legs = [
    {"sport":"MLB","player":"Joe Ryan","prop_type":"Earned Runs Allowed","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Joe Ryan"},
    {"sport":"MLB","player":"Sean Burke","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin","search":"Sean Burke"},
    {"sport":"MLB","player":"Miguel Vargas","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin","search":"Miguel Vargas"},
]

def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                    # prefer empty slip
                    try:
                        slip = P.read_slip(P.find_prizepicks_frame(pg), n_legs=6, ticket_type="power") or {}
                        n = int(slip.get("n_selected") or 0)
                    except Exception:
                        n = -1
                    if n == 0:
                        page = pg
                        break
                    if page is None:
                        page = pg
            if page and n == 0:
                break
        if page is None:
            page = browser.contexts[0].new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1500)
        page.bring_to_front()
        P.navigate_board_for_sport(page, "MLB", settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        for leg in legs:
            print(f"+ L5=5 {leg['player']} {leg['prop_type']} O{leg['line']}", flush=True)
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            P._switch_board_filter(frame, page, P.prop_type_to_board_filter(leg["prop_type"]))
            page.wait_for_timeout(600)
            react_search(page, "")
            page.wait_for_timeout(80)
            react_search(page, leg["search"])
            page.wait_for_timeout(1300)
            frame = P.find_prizepicks_frame(page)
            r = add_cycled_leg(P, frame, page, leg)
            if not r.ok:
                print(r.loud_fail(), flush=True)
                try:
                    P.clear_slip(P.find_prizepicks_frame(page))
                except Exception:
                    pass
                return 2
            time.sleep(0.25)
        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
        except Exception:
            pass
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        n = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, 3)
        print(f"READY={n==3} n={n} N-correct={px}", flush=True)
        print(raw[:400], flush=True)
        page.bring_to_front()
        shot = _REPO / "data" / "reports" / "pp_ryan_burke_vargas_l5eq5_2026-09-25.png"
        try:
            page.screenshot(path=str(shot), full_page=False)
            print(f"shot {shot}", flush=True)
        except Exception as e:
            print(f"shot fail {e}", flush=True)
        if n != 3:
            return 2
        print("On the board. I did not submit.", flush=True)
        return 0

raise SystemExit(main())
