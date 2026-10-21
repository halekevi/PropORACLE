"""Rebuild live-face MLB-heavy Power3/Flex slips in separate tabs."""
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

# Live goblin faces verified / rebuilt from probe (9/25 ~9:56 ET).
# Dropped: Medvedev TG goblin gone; soccer tab flaky in multi-sport; Gasser Ks goblin gone.
TICKETS = [
  {"id":"P3A","mode":"power","legs":[
    {"sport":"MLB","player":"Hunter Brown","prop_type":"Walks Allowed","direction":"OVER","line":1.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Robert Gasser","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Freddy Peralta","prop_type":"Pitcher Strikeouts","direction":"OVER","line":2.5,"pick_type":"goblin"},
  ]},
  {"id":"P3B","mode":"power","legs":[
    {"sport":"MLB","player":"Jacob Lopez","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Yordan Alvarez","prop_type":"Hits+Runs+RBIs","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Tomoyuki Sugano","prop_type":"Earned Runs Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
  ]},
  {"id":"P3C","mode":"power","legs":[
    {"sport":"MLB","player":"Nick Lodolo","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Michael McGreevy","prop_type":"Hits Allowed","direction":"OVER","line":2.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Tarik Skubal","prop_type":"Hits Allowed","direction":"OVER","line":2.5,"pick_type":"goblin"},
  ]},
  {"id":"P3D","mode":"power","legs":[
    {"sport":"MLB","player":"Gavin Williams","prop_type":"Hits Allowed","direction":"OVER","line":3.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Clay Holmes","prop_type":"Hits Allowed","direction":"OVER","line":2.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Bubba Chandler","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
  ]},
  {"id":"F4A","mode":"flex","legs":[
    {"sport":"MLB","player":"Jackson Jobe","prop_type":"Pitcher Strikeouts","direction":"OVER","line":2.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Noah Cameron","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Alec Gamboa","prop_type":"Hits Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Jacob deGrom","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
  ]},
  {"id":"F4B","mode":"flex","legs":[
    {"sport":"MLB","player":"Sean Burke","prop_type":"Hits Allowed","direction":"OVER","line":2.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Cristopher Sánchez","prop_type":"Pitcher Strikeouts","direction":"OVER","line":3.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Joe Ryan","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Reid Detmers","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
  ]},
]

def land(page, frame, sport, allow_goto):
    if allow_goto:
        P.navigate_board_for_sport(page, sport, settle_ms=1800)
        return P.find_prizepicks_frame(page)
    sw = P.switch_sport_tab(frame, page, sport, settle_ms=1500)
    return sw

def build_one(page, ticket):
    tid, mode, legs = ticket["id"], ticket["mode"], [{**l,"search":l["player"]} for l in ticket["legs"]]
    print(f"\n======== {tid} {mode} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    added=[]
    for i, leg in enumerate(legs):
        print(f"=== {leg['player']} {leg['prop_type']} {leg['line']} ===")
        frame = P.find_prizepicks_frame(page)
        landed = land(page, frame, leg["sport"], allow_goto=(i==0))
        if landed is None:
            print("SPORT MISS", leg["player"]); P.clear_slip(frame); return {"id":tid,"ready":False,"n":0,"fails":[leg["player"]]}
        frame = landed
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(leg["prop_type"]))
        page.wait_for_timeout(700)
        react_search(page,""); page.wait_for_timeout(100); react_search(page, leg["search"]); page.wait_for_timeout(1300)
        frame = P.find_prizepicks_frame(page)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail()); P.clear_slip(frame); return {"id":tid,"ready":False,"n":len(added),"fails":[leg["player"]]}
        added.append(leg["player"]); time.sleep(0.25)
    try:
        P.find_prizepicks_frame(page).get_by_text("Flex Play" if mode=="flex" else "Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode) or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] n={n} N-correct={px} ready={ready}")
    print(raw[:400])
    return {"id":tid,"ready":ready,"n":n,"px":px,"fails":[]}

def main():
    results=[]
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        pages=[]
        board=None
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                board=pg; break
        if board is None:
            board=ctx.new_page(); board.goto(BOARD, wait_until="domcontentloaded", timeout=60_000); board.wait_for_timeout(1200)
        pages.append(board)
        for i in range(1,len(TICKETS)):
            pg=ctx.new_page(); pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000); pg.wait_for_timeout(900); pages.append(pg)
            print("tab", i+1)
        for page, ticket in zip(pages, TICKETS):
            try: results.append(build_one(page, ticket))
            except Exception as e:
                print(ticket["id"], "EXC", e); results.append({"id":ticket["id"],"ready":False,"n":0,"fails":[str(e)]})
    print("\nSUMMARY")
    for r in results:
        print(f"  {r['id']} ready={r.get('ready')} n={r.get('n')} N-correct={r.get('px')} fails={r.get('fails')}")
    print("No submit.")
    return 0 if all(r.get("ready") for r in results) else 2
raise SystemExit(main())
