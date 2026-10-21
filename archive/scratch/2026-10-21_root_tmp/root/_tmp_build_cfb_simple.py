"""Simple CFB Power 3 via add_leg + cycle. Maiava Pass TDs + 2 Recs Goblins."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import react_search, n_correct_power
import utils.pp_manual_slip as M
M.PROP_ALIASES["recs"] = ("recs", "receptions")
M.PROP_ALIASES["passtds"] = ("passtds", "passingtds", "passingtd")

CDP = "http://127.0.0.1:9222"
legs = [
    {"sport":"CFB","player":"Jayden Maiava","prop_type":"Pass TDs","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Maiava"},
    {"sport":"CFB","player":"Zacharyus Williams","prop_type":"Recs","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Zacharyus"},
    {"sport":"CFB","player":"Mark Bowman","prop_type":"Recs","direction":"OVER","line":3.5,"pick_type":"goblin","search":"Bowman"},
]

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    page = [pg for ctx in browser.contexts for pg in ctx.pages if "board" in (pg.url or "") and "prizepicks" in (pg.url or "")][0]
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    for leg in legs:
        print("+", leg["player"], leg["prop_type"], leg["line"], flush=True)
        frame = P.find_prizepicks_frame(page)
        chip = "Pass TDs" if "TD" in leg["prop_type"] else "Receptions"
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(600)
        react_search(page,""); page.wait_for_timeout(50); react_search(page, leg["search"]); page.wait_for_timeout(1300)
        frame = P.find_prizepicks_frame(page)
        ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
        print("  add_leg", ok, flush=True)
        if not ok:
            P.clear_slip(P.find_prizepicks_frame(page))
            raise SystemExit(2)
        time.sleep(0.3)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    print(f"READY={n==3} n={n} N-correct={n_correct_power(raw,3)}", flush=True)
    print(raw[:400], flush=True)
    page.bring_to_front()
    print("No submit.", flush=True)
    raise SystemExit(0 if n==3 else 2)
