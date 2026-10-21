"""Build CFB Power 3 with 2-team diversity: USC L5=4 + ORE L5=3. No submit."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, find_card
import utils.pp_manual_slip as M
M.PROP_ALIASES["recs"] = ("recs", "receptions")
M.PROP_ALIASES["receptions"] = ("recs", "receptions")
M.PROP_ALIASES["passtds"] = ("passtds", "passingtds", "passingtd")
M.PROP_ALIASES["receivingyards"] = ("receivingyards", "recyards", "recyds")

CDP = "http://127.0.0.1:9222"
# Two tickets, each USC + ORE (PrizePicks 2-team rule)
TICKETS = [
  {
    "id": "CFB-2T-1",
    "mode": "power",
    "legs": [
      # USC L5=4
      {"sport":"CFB","player":"Jayden Maiava","prop_type":"Pass TDs","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Maiava","team":"USC"},
      # ORE L5=3
      {"sport":"CFB","player":"Evan Stewart","prop_type":"Receptions","direction":"OVER","line":2.5,"pick_type":"goblin","search":"Evan Stewart","team":"ORE"},
      # USC L5=4-ish recs (live may move)
      {"sport":"CFB","player":"Kayden Dixon-Wyatt","prop_type":"Receptions","direction":"OVER","line":2.5,"pick_type":"goblin","search":"Dixon-Wyatt","team":"USC"},
    ],
  },
  {
    "id": "CFB-2T-2",
    "mode": "power",
    "legs": [
      {"sport":"CFB","player":"Zacharyus Williams","prop_type":"Receptions","direction":"OVER","line":1.5,"pick_type":"goblin","search":"Zacharyus","team":"USC"},
      {"sport":"CFB","player":"Dakorien Moore","prop_type":"Receiving Yards","direction":"OVER","line":24.5,"pick_type":"goblin","search":"Dakorien","team":"ORE"},
      {"sport":"CFB","player":"Mark Bowman","prop_type":"Receptions","direction":"OVER","line":2.5,"pick_type":"goblin","search":"Bowman","team":"USC"},
    ],
  },
]

def teams_ok(legs):
    return len({l.get("team") for l in legs}) >= 2

def add_with_fallback(page, leg):
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    prop = leg["prop_type"]
    chip = "Pass TDs" if "TD" in prop else ("Receiving Yards" if "Yard" in prop else "Receptions")
    P._switch_board_filter(frame, page, chip)
    page.wait_for_timeout(600)
    react_search(page, ""); page.wait_for_timeout(50); react_search(page, leg["search"]); page.wait_for_timeout(1300)
    frame = P.find_prizepicks_frame(page)
    # try planned line, then any goblin for prop
    for attempt_prop in [prop, "Recs", "Receptions", "Rec Yards", "Receiving Yards", "Pass TDs"]:
        use = {**leg, "prop_type": attempt_prop, "prop": attempt_prop}
        r = add_cycled_leg(P, frame, page, use)
        if r.ok:
            return True, attempt_prop, leg["line"]
        # if line moved, take live goblin same prop family
        card = None
        for c in P.get_all_cards(frame):
            if leg["player"].split()[-1].lower() not in str(c.get("player") or "").lower():
                continue
            if str(c.get("pick_type") or "").lower() != "goblin":
                continue
            card = c
            break
        if card and card.get("more_btn"):
            live_line = float(card.get("line"))
            live_prop = str(card.get("prop_type") or attempt_prop)
            print(f"  live-face fallback {leg['player']} {live_prop} O{live_line}", flush=True)
            use2 = {**leg, "prop_type": live_prop, "prop": live_prop, "line": live_line}
            r2 = add_cycled_leg(P, frame, page, use2)
            if r2.ok:
                return True, live_prop, live_line
            try:
                card["more_btn"].click(timeout=1500, no_wait_after=True)
                page.wait_for_timeout(600)
                return True, live_prop, live_line
            except Exception:
                pass
        frame = P.find_prizepicks_frame(page)
    return False, prop, leg["line"]

def build(page, ticket):
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    assert teams_ok(legs), "ticket must have 2 teams"
    print(f"\n======== {tid} teams={sorted({l['team'] for l in legs})} ========", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    used = []
    for leg in legs:
        print(f"+ {leg['team']} {leg['player']} {leg['prop_type']} O{leg['line']}", flush=True)
        ok, pused, lused = add_with_fallback(page, leg)
        if not ok:
            print("FAIL", leg["player"], flush=True)
            try: P.clear_slip(P.find_prizepicks_frame(page))
            except Exception: pass
            return {"id": tid, "ready": False, "fails": [leg["player"]]}
        used.append(f"{leg['team']} {leg['player']} {pused} O{lused}")
        time.sleep(0.25)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    # verify 2 teams in slip text roughly
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode) or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    has_usc = "USC" in raw
    has_ore = "ORE" in raw or "Oregon" in raw
    print(f"[{tid}] ready={n==len(legs)} n={n} N-correct={px} usc={has_usc} ore={has_ore}", flush=True)
    print(raw[:400], flush=True)
    if "two different teams" in raw.lower() or "at least two" in raw.lower():
        print("WARN: PP still complaining about teams", flush=True)
    page.screenshot(path=str(_REPO / f"data/reports/pp_{tid.lower()}.png"), full_page=False)
    return {"id": tid, "ready": n==len(legs) and has_usc and has_ore, "n": n, "px": px, "legs": used, "fails": []}

results=[]
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    ctx = browser.contexts[0]
    boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")]
    while len(boards) < len(TICKETS):
        pg = ctx.new_page(); pg.goto("https://app.prizepicks.com/board", wait_until="domcontentloaded", timeout=60_000); pg.wait_for_timeout(1200); boards.append(pg)
    # clear any all-USC slip on first tab first
    for page, ticket in zip(boards, TICKETS):
        try: results.append(build(page, ticket))
        except Exception as e:
            print("EXC", e); results.append({"id": ticket["id"], "ready": False, "fails":[str(e)]})
    for page, r in zip(boards, results):
        if r.get("ready"):
            page.bring_to_front(); print("Brought", r["id"]); break

print("\nSUMMARY")
for r in results: print(r)
print("No submit.")
