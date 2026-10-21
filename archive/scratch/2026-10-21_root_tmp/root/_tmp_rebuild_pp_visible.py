"""Build 4 MLB Power/Flex slips on existing empty board tabs. Leave them filled. No submit."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
# Live goblin faces confirmed on board this afternoon
TICKETS = [
  {"id":"TAB1-P3","mode":"power","legs":[
    {"sport":"MLB","player":"Freddy Peralta","prop_type":"Walks Allowed","direction":"OVER","line":1.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Cristopher Sánchez","prop_type":"Pitcher Strikeouts","direction":"OVER","line":3.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Joe Ryan","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
  ]},
  {"id":"TAB2-P3","mode":"power","legs":[
    {"sport":"MLB","player":"Reid Detmers","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Cristopher Sánchez","prop_type":"Hits Allowed","direction":"OVER","line":4.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Joe Ryan","prop_type":"Earned Runs Allowed","direction":"OVER","line":1.5,"pick_type":"goblin"},
  ]},
  {"id":"TAB3-F4","mode":"flex","legs":[
    {"sport":"MLB","player":"Reid Detmers","prop_type":"Hits Allowed","direction":"OVER","line":3.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Cristopher Sánchez","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Freddy Peralta","prop_type":"Walks Allowed","direction":"OVER","line":1.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Joe Ryan","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
  ]},
  {"id":"TAB4-P3","mode":"power","legs":[
    {"sport":"MLB","player":"Reid Detmers","prop_type":"Hits Allowed","direction":"OVER","line":3.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Cristopher Sánchez","prop_type":"Pitcher Strikeouts","direction":"OVER","line":3.5,"pick_type":"goblin"},
    {"sport":"MLB","player":"Joe Ryan","prop_type":"Walks Allowed","direction":"OVER","line":0.5,"pick_type":"goblin"},
  ]},
]

def build_one(page, ticket):
    tid, mode = ticket["id"], ticket["mode"]
    legs = [{**l, "search": l["player"]} for l in ticket["legs"]]
    print(f"\n======== BUILD {tid} ========", flush=True)
    page.bring_to_front()
    page.wait_for_timeout(500)
    P.navigate_board_for_sport(page, "MLB", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for i, leg in enumerate(legs):
        # Map prop filter: Ks -> Pitcher Strikeouts
        prop = leg["prop_type"]
        if prop in ("Ks", "Strikeouts"):
            prop = "Pitcher Strikeouts"
            leg = {**leg, "prop_type": prop}
        print(f"  + {leg['player']} {prop} {leg['line']}", flush=True)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(prop)
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(700)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, leg["search"])
        page.wait_for_timeout(1300)
        frame = P.find_prizepicks_frame(page)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try: P.clear_slip(frame)
            except Exception: pass
            return {"id": tid, "ready": False, "n": i, "fails": [leg["player"]], "raw": ""}
        time.sleep(0.3)
    try:
        label = "Flex Play" if mode == "flex" else "Power Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(timeout=2000)
    except Exception:
        pass
    page.wait_for_timeout(600)
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode) or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] READY={ready} n={n} N-correct={px}x", flush=True)
    print(raw[:450], flush=True)
    # screenshot for proof
    shot = _REPO / "data" / "reports" / f"pp_{tid.lower()}_2026-09-25.png"
    try:
        page.screenshot(path=str(shot), full_page=False)
        print(f"shot {shot}", flush=True)
    except Exception as e:
        print(f"shot fail {e}", flush=True)
    return {"id": tid, "ready": ready, "n": n, "px": px, "fails": [], "raw": raw[:300]}

def main():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
        boards = []
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                    boards.append(pg)
        print(f"board tabs available: {len(boards)}", flush=True)
        if len(boards) < len(TICKETS):
            ctx = browser.contexts[0]
            while len(boards) < len(TICKETS):
                pg = ctx.new_page()
                pg.goto("https://app.prizepicks.com/board", wait_until="domcontentloaded", timeout=60_000)
                pg.wait_for_timeout(1500)
                boards.append(pg)
                print(f"opened extra board tab ({len(boards)})", flush=True)
        for page, ticket in zip(boards[:len(TICKETS)], TICKETS):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append({"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]})
        # leave first ready tab in front
        for page, r in zip(boards, results):
            if r.get("ready"):
                page.bring_to_front()
                print(f"\nBrought {r['id']} to front — look at that Chrome tab.", flush=True)
                break
    print("\n======== SUMMARY ========", flush=True)
    for r in results:
        print(f"  {r['id']} ready={r.get('ready')} n={r.get('n')} N-correct={r.get('px')} fails={r.get('fails')}", flush=True)
    print("Slips left on tabs. No submit.", flush=True)
    return 0 if any(r.get("ready") for r in results) else 2

raise SystemExit(main())
