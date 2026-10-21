"""Fill more tabs with verified L5=5 live faces (avoid flaky hitter-K goblins)."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
# All L5=5 vs today's line from step8 + confirmed/likely live (non-Ks)
TICKETS = [
  {"id":"L5=5-A","mode":"power","legs":[
    {"sport":"MLB","player":"Joe Ryan","prop_type":"Earned Runs Allowed","direction":"OVER","line":1.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Sean Burke","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Miguel Vargas","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
  ]},
  {"id":"L5=5-B","mode":"power","legs":[
    {"sport":"MLB","player":"Zack Gelof","prop_type":"Hits","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Bobby Witt Jr.","prop_type":"Hits","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Brooks Lee","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
  ]},
  {"id":"L5=5-C","mode":"power","legs":[
    {"sport":"MLB","player":"Oneil Cruz","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Alan Roden","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Walker Jenkins","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
  ]},
  {"id":"L5=5-D","mode":"flex","legs":[
    {"sport":"MLB","player":"David Fry","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Braden Montgomery","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Munetaka Murakami","prop_type":"Hitter Strikeouts","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
    {"sport":"MLB","player":"Salvador Perez","prop_type":"Hitter Strikeouts","direction":"OVER","line":0.5,"pick_type":"goblin","l5":5},
  ]},
]

def build_one(page, ticket):
    tid, mode = ticket["id"], ticket["mode"]
    legs = [{**l, "search": l["player"], "prop": l["prop_type"]} for l in ticket["legs"]]
    print(f"\n======== {tid} ========", flush=True)
    for leg in legs:
        print(f"  L5={leg['l5']} {leg['player']} {leg['prop_type']} {leg['line']}", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, "MLB", settle_ms=1600)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(leg["prop_type"]))
        page.wait_for_timeout(550)
        react_search(page,""); page.wait_for_timeout(60); react_search(page, leg["search"]); page.wait_for_timeout(1100)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try: P.clear_slip(P.find_prizepicks_frame(page))
            except Exception: pass
            return {"id": tid, "ready": False, "fails": [leg["player"]]}
        time.sleep(0.2)
    try:
        P.find_prizepicks_frame(page).get_by_text("Flex Play" if mode=="flex" else "Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode) or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    print(f"[{tid}] ready={n==len(legs)} n={n} N-correct={px}", flush=True)
    return {"id": tid, "ready": n==len(legs), "n": n, "px": px, "fails": []}

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    boards=[]
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                boards.append(pg)
    # skip tab0 if it already has the first L5=5 slip (Gasser/Alvarez/Tatis)
    # use empty tabs
    targets=[]
    for pg in boards:
        try:
            slip=P.read_slip(P.find_prizepicks_frame(pg), n_legs=6, ticket_type="power") or {}
            n=int(slip.get("n_selected") or 0)
        except Exception:
            n=0
        if n == 0:
            targets.append(pg)
    print(f"empty board tabs: {len(targets)}", flush=True)
    while len(targets) < len(TICKETS):
        pg=browser.contexts[0].new_page()
        pg.goto("https://app.prizepicks.com/board", wait_until="domcontentloaded", timeout=60_000)
        pg.wait_for_timeout(1000); targets.append(pg)
    results=[]
    for pg, ticket in zip(targets, TICKETS):
        try: results.append(build_one(pg, ticket))
        except Exception as e:
            print("EXC", e); results.append({"id": ticket["id"], "ready": False, "fails":[str(e)]})
    for pg,r in zip(targets, results):
        if r.get("ready"):
            pg.bring_to_front(); break
print("SUMMARY")
for r in results: print(r)
