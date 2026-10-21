from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, _norm

CDP = "http://127.0.0.1:9222"
# Replace Peralta Ks 2.5 (gone) with Peralta live goblin if any; else Joe Ryan already on F4B — use Sean Burke alternative... use Freddy live faces
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=60_000)
    ctx = browser.contexts[0]
    page = None
    for pg in ctx.pages:
        if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
            page = pg
            break
    page.bring_to_front()
    P.navigate_board_for_sport(page, "MLB", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    react_search(page, "Freddy Peralta")
    page.wait_for_timeout(1400)
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    print("Peralta faces:")
    for c in cards:
        if "peralta" in _norm(c.get("player")):
            print(f"  {c.get('pick_type'):8} {c.get('line')} {c.get('prop_type')}")
    # Pick best goblin for 3rd leg of P3A: prefer walks/HA/ERA
    pick = None
    for want in ("Walks Allowed", "Hits Allowed", "Earned Runs Allowed", "Pitching Outs", "PO"):
        for c in cards:
            if "peralta" not in _norm(c.get("player")): continue
            if str(c.get("pick_type")).lower() != "goblin": continue
            if want.lower().replace(" ","") in _norm(c.get("prop_type")) or _norm(c.get("prop_type")) in _norm(want):
                pick = {"sport":"MLB","player":"Freddy Peralta","prop_type":c.get("prop_type"),"direction":"OVER","line":float(c.get("line")),"pick_type":"goblin","search":"Freddy Peralta"}
                break
        if pick: break
    print("chosen", pick)
    if not pick:
        raise SystemExit(2)
    legs = [
        {"sport":"MLB","player":"Hunter Brown","prop_type":"Walks Allowed","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Hunter Brown"},
        {"sport":"MLB","player":"Robert Gasser","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin","search":"Robert Gasser"},
        pick,
    ]
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    for i, leg in enumerate(legs):
        print("===", leg["player"], leg["prop_type"], leg["line"])
        frame = P.find_prizepicks_frame(page)
        if i == 0:
            P.navigate_board_for_sport(page, "MLB", settle_ms=1200)
            frame = P.find_prizepicks_frame(page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(leg["prop_type"]))
        page.wait_for_timeout(600)
        react_search(page,""); page.wait_for_timeout(80); react_search(page, leg["search"]); page.wait_for_timeout(1200)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail()); raise SystemExit(2)
        time.sleep(0.2)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, 3)
    print(f"P3A FIXED n={n} N-correct={px}")
    print(raw[:500])
