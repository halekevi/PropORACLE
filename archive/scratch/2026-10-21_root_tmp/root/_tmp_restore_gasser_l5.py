"""Restore Gasser/Alvarez/Tatis L5=5 Power 3 on an empty tab."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

legs = [
 {"sport":"MLB","player":"Robert Gasser","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin","search":"Robert Gasser"},
 {"sport":"MLB","player":"Yordan Alvarez","prop_type":"Hits","direction":"OVER","line":0.5,"pick_type":"goblin","search":"Yordan Alvarez"},
 {"sport":"MLB","player":"Fernando Tatis Jr.","prop_type":"Hits","direction":"OVER","line":0.5,"pick_type":"goblin","search":"Fernando Tatis"},
]
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=120_000)
    page=None
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" not in (pg.url or "") or "board" not in (pg.url or ""): continue
            try:
                slip=P.read_slip(P.find_prizepicks_frame(pg), n_legs=6, ticket_type="power") or {}
                n=int(slip.get("n_selected") or 0)
            except Exception:
                n=-1
            if n == 0:
                page=pg; break
        if page: break
    if page is None:
        page=browser.contexts[0].new_page()
        page.goto("https://app.prizepicks.com/board", wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(1200)
    page.bring_to_front()
    P.navigate_board_for_sport(page, "MLB", settle_ms=1600)
    frame=P.find_prizepicks_frame(page)
    P.clear_slip(frame); _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    for leg in legs:
        print("+", leg["player"], leg["prop_type"], leg["line"])
        frame=P.find_prizepicks_frame(page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(leg["prop_type"]))
        page.wait_for_timeout(500)
        react_search(page,""); page.wait_for_timeout(50); react_search(page, leg["search"]); page.wait_for_timeout(1100)
        r=add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail()); raise SystemExit(2)
        time.sleep(0.2)
    try: P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip=P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    raw=str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n=int(slip.get("n_selected") or 0)
    print("ready", n==3, "n", n, "N-correct", n_correct_power(raw,3))
    print(raw[:300])
    page.bring_to_front()
