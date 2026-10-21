"""Build one CFB Power 3 using board prop names Recs / Pass TDs."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, find_card, _norm

CDP = "http://127.0.0.1:9222"
legs = [
    {"sport":"CFB","player":"Kayden Dixon-Wyatt","prop_type":"Recs","direction":"OVER","line":2.5,"pick_type":"goblin","search":"Dixon-Wyatt"},
    {"sport":"CFB","player":"Zacharyus Williams","prop_type":"Recs","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Zacharyus"},
    {"sport":"CFB","player":"Mark Bowman","prop_type":"Recs","direction":"OVER","line":3.5,"pick_type":"goblin","search":"Bowman"},
]

# extend prop aliases temporarily via matching Recs
import utils.pp_manual_slip as M
M.PROP_ALIASES["recs"] = ("recs", "receptions")
M.PROP_ALIASES["receptions"] = ("recs", "receptions")

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    page = None
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                page = pg; break
        if page: break
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    for leg in legs:
        print("+", leg["player"], leg["prop_type"], leg["line"], flush=True)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, "Receptions")
        page.wait_for_timeout(600)
        react_search(page,""); page.wait_for_timeout(50); react_search(page, leg["search"]); page.wait_for_timeout(1300)
        frame = P.find_prizepicks_frame(page)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            # manual fallback: find goblin card and click More
            card = find_card(P, frame, leg["player"], "Recs", line=leg["line"], pick="goblin", line_key=P._line_key)
            if card is None:
                card = find_card(P, frame, leg["player"], "Receptions", line=leg["line"], pick="goblin", line_key=P._line_key)
            if card is None:
                # cycle any then click
                anyc = find_card(P, frame, leg["player"], "Recs") or find_card(P, frame, leg["player"], "Receptions")
                if anyc and anyc.get("more_btn"):
                    P.cycle_card_to_pick_type(frame, anyc.get("more_btn"), player=leg["player"], prop=anyc.get("prop_type") or "Recs", want_pick="goblin", want_line=leg["line"], require_line=False, more_index=anyc.get("more_index"))
                    page.wait_for_timeout(500)
                    frame = P.find_prizepicks_frame(page)
                    card = find_card(P, frame, leg["player"], "Recs", pick="goblin", line_key=P._line_key) or find_card(P, frame, leg["player"], "Receptions", pick="goblin", line_key=P._line_key)
            if card and card.get("more_btn"):
                print(f"  manual click {card.get('player')} {card.get('line')} {card.get('prop_type')} {card.get('pick_type')}", flush=True)
                try:
                    card["more_btn"].click(timeout=1500, no_wait_after=True)
                except Exception:
                    card["more_btn"].click(force=True, timeout=2500, no_wait_after=True)
                page.wait_for_timeout(600)
            else:
                P.clear_slip(frame); raise SystemExit(2)
        time.sleep(0.3)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, 3)
    print(f"READY={n==3} n={n} N-correct={px}", flush=True)
    print(raw[:500], flush=True)
    page.bring_to_front()
    try:
        page.screenshot(path=str(_REPO / "data/reports/pp_cfb_recs_p3_2026-09-26.png"), full_page=False)
    except Exception: pass
    print("No submit.", flush=True)
    raise SystemExit(0 if n == 3 else 2)
