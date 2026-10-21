"""Probe Sam Leavitt live CFB faces + flash-sale text on PP."""
from __future__ import annotations
import sys
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import _norm

CDP = "http://127.0.0.1:9222"

def dump_leavitt(page):
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    page.evaluate("""(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return false;
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
      setter.call(inp, '');
      inp.dispatchEvent(new Event('input', {bubbles:true}));
      setter.call(inp, q);
      inp.dispatchEvent(new Event('input', {bubbles:true}));
      return true;
    }""", "Sam Leavitt")
    page.wait_for_timeout(1500)
    frame = P.find_prizepicks_frame(page)
    # raw text scan for flash
    blob = page.evaluate("() => (document.body && document.body.innerText) ? document.body.innerText.slice(0,8000) : ''")
    for line in str(blob).splitlines():
        if "leavitt" in line.lower() or "flash" in line.lower():
            print("TXT:", line.strip()[:160])
    cards = P.get_all_cards(frame)
    pn = _norm("Sam Leavitt")
    hits = [c for c in cards if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn]
    print(f"cards={len(hits)}")
    for c in hits:
        print(
            f"  {c.get('player')} | {c.get('line')} {c.get('prop_type')} | {c.get('pick_type')} | "
            f"more={c.get('more_index')}"
        )
    # try cycle goblin pass yards faces
    pass_cards = [c for c in hits if "pass" in _norm(c.get("prop_type") or "") and "yard" in _norm(c.get("prop_type") or "") and "rush" not in _norm(c.get("prop_type") or "")]
    if pass_cards:
        start = pass_cards[0]
        print("cycling Pass Yards goblin faces from", start.get("line"), start.get("pick_type"))
        frame = P.find_prizepicks_frame(page)
        # dump a few swaps
        for i in range(6):
            P.cycle_card_to_pick_type(
                frame, start.get("more_btn"), player="Sam Leavitt",
                prop=start.get("prop_type") or "Pass Yards",
                want_pick="goblin", want_line=None, require_line=False,
                more_index=start.get("more_index"),
            )
            page.wait_for_timeout(400)
            hits2 = [c for c in P.get_all_cards(P.find_prizepicks_frame(page))
                     if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn]
            for c in hits2:
                if "pass" in _norm(c.get("prop_type") or "") and "yard" in _norm(c.get("prop_type") or ""):
                    print(f"  swap{i}: {c.get('line')} {c.get('prop_type')} {c.get('pick_type')}")
            # also evaluate flash badges near card
        flash = page.evaluate("""() => {
          const nodes = [...document.querySelectorAll('*')];
          const out = [];
          for (const n of nodes) {
            const t = (n.innerText || '').trim();
            if (!t) continue;
            if (/flash/i.test(t) && /leavitt|sale|%|off/i.test(t) && t.length < 120)
              out.push(t.replace(/\\s+/g,' ').slice(0,120));
          }
          return [...new Set(out)].slice(0,20);
        }""")
        print("FLASH nodes:", flash)

with sync_playwright() as p:
    b = p.chromium.connect_over_cdp(CDP, timeout=60_000)
    page = next(pg for pg in b.contexts[0].pages if "prizepicks" in (pg.url or ""))
    page.bring_to_front()
    dump_leavitt(page)
