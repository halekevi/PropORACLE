from __future__ import annotations
import sys
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import _norm, prop_ok

CDP="http://127.0.0.1:9222"
TARGETS=[159.5,174.5,199.5,209.5,229.5]

with sync_playwright() as p:
    b=p.chromium.connect_over_cdp(CDP, timeout=60_000)
    page=next(pg for pg in b.contexts[0].pages if "prizepicks" in (pg.url or "") and "board" in (pg.url or ""))
    page.bring_to_front()
    P.navigate_board_for_sport(page,"CFB", settle_ms=1800)
    page.evaluate("""(q)=>{const inp=document.querySelector("input[aria-label='search'], input[placeholder*='Search']");if(!inp)return false;const s=Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set;s.call(inp,'');inp.dispatchEvent(new Event('input',{bubbles:true}));s.call(inp,q);inp.dispatchEvent(new Event('input',{bubbles:true}));return true;}""","Sam Leavitt")
    page.wait_for_timeout(1400)
    frame=P.find_prizepicks_frame(page)
    cards=P.get_all_cards(frame)
    pn=_norm("Sam Leavitt")
    hits=[c for c in cards if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn]
    start=next((c for c in hits if prop_ok("Pass Yards", str(c.get("prop_type") or ""))), None)
    print("start", start and (start.get("line"), start.get("pick_type"), start.get("prop_type")))
    if not start:
        raise SystemExit("no pass yards card")
    # screenshot for flash badge
    page.screenshot(path=str(_REPO/"data/reports/pp_leavitt_flash_2026-09-26.png"), full_page=False)
    for want in TARGETS:
        frame=P.find_prizepicks_frame(page)
        hits=P.get_all_cards(frame)
        start=next((c for c in hits if pn in _norm(c.get("player")) and prop_ok("Pass Yards", str(c.get("prop_type") or ""))), start)
        r=P.cycle_card_to_pick_type(frame, start.get("more_btn"), player="Sam Leavitt", prop=start.get("prop_type") or "Pass Yards", want_pick="goblin", want_line=want, require_line=True, more_index=start.get("more_index"), max_clicks=16)
        print(f"want={want} ->", r and (r.get("line"), r.get("pick_type")))
    # also rush goblin 19.5
    hits=P.get_all_cards(P.find_prizepicks_frame(page))
    rstart=next((c for c in hits if pn in _norm(c.get("player")) and prop_ok("Rush Yards", str(c.get("prop_type") or ""))), None)
    if rstart:
        r=P.cycle_card_to_pick_type(P.find_prizepicks_frame(page), rstart.get("more_btn"), player="Sam Leavitt", prop=rstart.get("prop_type") or "Rush Yards", want_pick="goblin", want_line=19.5, require_line=True, more_index=rstart.get("more_index"), max_clicks=14)
        print("rush 19.5 ->", r and (r.get("line"), r.get("pick_type")))
    # page text for Flash Sale near Leavitt
    info=page.evaluate("""() => {
      const t=(document.body.innerText||'');
      const i=t.toLowerCase().indexOf('leavitt');
      const slice=t.slice(Math.max(0,i-200), i+600);
      const flashes=[...t.matchAll(/flash[^\\n]{0,80}/gi)].map(m=>m[0]).slice(0,15);
      return {slice, flashes};
    }""")
    print("flashes", info.get("flashes"))
    print("context:", repr(info.get("slice")[:500]))
