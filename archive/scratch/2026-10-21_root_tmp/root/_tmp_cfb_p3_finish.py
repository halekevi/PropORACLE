"""Finish P3-1 from leftover live CFB L5=5 after F4-1. No submit."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, prop_ok, _norm

CDP="http://127.0.0.1:9222"
BOARD="https://app.prizepicks.com/board?league_id=15"
LEGS=[
 {"player":"Keshaun Singleton","prop":"Rec Yards","prop_type":"Rec Yards","line":39.5,"pick_type":"goblin","direction":"OVER","sport":"CFB","search":"Keshaun Singleton"},
 {"player":"Rodney Fields Jr.","prop":"Rush Yards","prop_type":"Rush Yards","line":34.5,"pick_type":"goblin","direction":"OVER","sport":"CFB","search":"Rodney Fields"},
 {"player":"Junior Sherrill","prop":"Rec Yards","prop_type":"Rec Yards","line":39.5,"pick_type":"goblin","direction":"OVER","sport":"CFB","search":"Junior Sherrill"},
]

def yard_ok(want, got):
    if prop_ok(want, got): return True
    w,g=_norm(want),_norm(got)
    for a in ("pass","rush","rec"):
        if a in w and "yard" in w and a in g and "yard" in g: return True
    return False

def search(page, player):
    page.evaluate("""(q)=>{const inp=document.querySelector("input[aria-label='search'], input[placeholder*='Search']");if(!inp)return false;const s=Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set;s.call(inp,'');inp.dispatchEvent(new Event('input',{bubbles:true}));s.call(inp,q);inp.dispatchEvent(new Event('input',{bubbles:true}));return true;}""", player)
    page.wait_for_timeout(1300)
    frame=P.find_prizepicks_frame(page)
    cards=P.get_all_cards(frame)
    pn=_norm(player)
    return [c for c in cards if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn]

with sync_playwright() as p:
    b=p.chromium.connect_over_cdp(CDP, timeout=60_000)
    ctx=b.contexts[0]
    page=next(pg for pg in ctx.pages if "prizepicks" in (pg.url or "") and "board" in (pg.url or ""))
    # new tab so we don't wipe F4-1
    pg=ctx.new_page()
    pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
    pg.wait_for_timeout(1200)
    pg.bring_to_front()
    P.navigate_board_for_sport(pg,"CFB", settle_ms=1800)
    frame=P.find_prizepicks_frame(pg)
    P.clear_slip(frame)
    P.set_ticket_type(frame,"power")
    for leg in LEGS:
        print("===", leg["player"], leg["prop"], leg["line"], flush=True)
        hits=search(pg, leg["search"])
        if not hits: hits=search(pg, leg["player"].split()[-1])
        if not hits:
            print("NO HITS", leg["player"]); raise SystemExit(2)
        start=next((c for c in hits if yard_ok(leg["prop"], str(c.get("prop_type") or ""))), hits[0])
        exact=any(yard_ok(leg["prop"], str(c.get("prop_type") or "")) and str(c.get("pick_type") or "").lower()=="goblin" and P._line_key(c.get("line"))==P._line_key(leg["line"]) for c in hits)
        if not exact:
            P.cycle_card_to_pick_type(P.find_prizepicks_frame(pg), start.get("more_btn"), player=leg["player"], prop=start.get("prop_type") or leg["prop"], want_pick="goblin", want_line=leg["line"], require_line=True, more_index=start.get("more_index"), max_clicks=14)
            pg.wait_for_timeout(350); search(pg, leg["search"])
        r=add_cycled_leg(P, P.find_prizepicks_frame(pg), pg, leg)
        if not r.ok:
            print(r.loud_fail()); raise SystemExit(2)
        time.sleep(0.2)
    try: P.find_prizepicks_frame(pg).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip=P.read_slip(P.find_prizepicks_frame(pg), n_legs=3, ticket_type="power") or {}
    raw=str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n=int(slip.get("n_selected") or 0)
    px=n_correct_power(raw,3)
    print(f"[P3-1] ready={n==3} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
