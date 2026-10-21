"""Build CFB Power 3 from stable live Goblin Recs + Maiava Pass TDs."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
TICKETS = [
  {"id": "CFB-P3-RECS", "mode": "power", "legs": [
    {"sport":"CFB","player":"Kayden Dixon-Wyatt","prop_type":"Receptions","direction":"OVER","line":2.5,"pick_type":"goblin","search":"Dixon-Wyatt"},
    {"sport":"CFB","player":"Zacharyus Williams","prop_type":"Receptions","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Zacharyus"},
    {"sport":"CFB","player":"Mark Bowman","prop_type":"Receptions","direction":"OVER","line":2.5,"pick_type":"goblin","search":"Mark Bowman"},
  ]},
  {"id": "CFB-P3-MAIAVA", "mode": "power", "legs": [
    {"sport":"CFB","player":"Jayden Maiava","prop_type":"Pass TDs","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Maiava"},
    {"sport":"CFB","player":"Kayden Dixon-Wyatt","prop_type":"Receptions","direction":"OVER","line":2.5,"pick_type":"goblin","search":"Dixon-Wyatt"},
    {"sport":"CFB","player":"Mark Bowman","prop_type":"Receptions","direction":"OVER","line":3.5,"pick_type":"goblin","search":"Mark Bowman"},
  ]},
]

def build(page, ticket):
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
        print(f"  + {leg['player']} {leg['prop_type']} O{leg['line']}", flush=True)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        # Receptions chip may be Recs
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(700)
        react_search(page,""); page.wait_for_timeout(80); react_search(page, leg["search"]); page.wait_for_timeout(1400)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            # try prop alias Recs
            if leg["prop_type"] == "Receptions":
                alt = {**leg, "prop_type": "Recs", "prop": "Recs"}
                r = add_cycled_leg(P, frame, page, alt)
            if not r.ok:
                print(r.loud_fail(), flush=True)
                try: P.clear_slip(P.find_prizepicks_frame(page))
                except Exception: pass
                return {"id": tid, "ready": False, "fails": [leg["player"]]}
        time.sleep(0.3)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode) or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    print(f"[{tid}] ready={n==len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:350], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_{tid.lower()}_2026-09-26.png"
    try: page.screenshot(path=str(shot), full_page=False)
    except Exception: pass
    return {"id": tid, "ready": n==len(legs), "n": n, "px": px, "fails": []}

results=[]
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    ctx = browser.contexts[0]
    boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")]
    while len(boards) < 2:
        pg = ctx.new_page(); pg.goto("https://app.prizepicks.com/board", wait_until="domcontentloaded", timeout=60_000); pg.wait_for_timeout(1200); boards.append(pg)
    for page, ticket in zip(boards[:2], TICKETS):
        try: results.append(build(page, ticket))
        except Exception as e:
            print("EXC", e); results.append({"id": ticket["id"], "ready": False, "fails":[str(e)]})
    for page, r in zip(boards, results):
        if r.get("ready"):
            page.bring_to_front(); print("Brought", r["id"]); break
print("SUMMARY")
for r in results: print(r)
print("No submit. Yard Goblins mostly off the live board.")
