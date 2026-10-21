"""Probe live CFB faces for USC-ORE L5=4 players."""
from __future__ import annotations
import sys
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import react_search, _norm

PLAYERS = [
    "Jayden Maiava", "Kayden Dixon-Wyatt", "King Miller",
    "Waymond Jordan", "Zacharyus Williams", "Mark Bowman",
    "Maiava", "Dixon-Wyatt", "Miller",
]
CDP = "http://127.0.0.1:9222"
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    page = None
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                page = pg
                break
        if page: break
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2500)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    # dump sport tabs / board state
    try:
        tabs = page.evaluate("""() => {
          return Array.from(document.querySelectorAll('button, [role=tab]'))
            .map(e => (e.innerText||'').trim().split('\\n')[0])
            .filter(t => t && t.length < 20)
            .slice(0, 40);
        }""")
        print("header chips sample:", tabs)
    except Exception as e:
        print("chips err", e)
    for player in PLAYERS[:6]:
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, player)
        page.wait_for_timeout(1400)
        frame = P.find_prizepicks_frame(page)
        try:
            P._switch_board_filter(frame, page, "Popular")
            page.wait_for_timeout(500)
        except Exception:
            pass
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        print(f"\n== {player} cards={len(cards)} ==")
        hits = [c for c in cards if _norm(player.split()[-1]) in _norm(c.get("player")) or _norm(player) in _norm(c.get("player"))]
        if not hits:
            print("  no player hits; sample:", [(c.get("player"), c.get("line"), c.get("prop_type"), c.get("pick_type")) for c in cards[:8]])
        for c in hits[:12]:
            print(f"  {c.get('pick_type'):8} {c.get('line')} {c.get('prop_type')} | {c.get('player')}")
