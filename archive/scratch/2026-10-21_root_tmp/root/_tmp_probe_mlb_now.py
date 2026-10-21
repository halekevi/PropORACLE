"""Probe MLB goblin faces still on the live board right now."""
from __future__ import annotations
import sys
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import react_search

CDP = "http://127.0.0.1:9222"
PLAYERS = [
    "Hunter Brown","Robert Gasser","Jacob Lopez","Yordan Alvarez","Tomoyuki Sugano",
    "Nick Lodolo","Michael McGreevy","Tarik Skubal","Gavin Williams","Clay Holmes",
    "Bubba Chandler","Jackson Jobe","Noah Cameron","Alec Gamboa","Jacob deGrom",
    "Sean Burke","Cristopher Sánchez","Joe Ryan","Reid Detmers","Freddy Peralta",
]
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
    P.navigate_board_for_sport(page, "MLB", settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    goblins = []
    for player in PLAYERS:
        react_search(page, "")
        page.wait_for_timeout(80)
        react_search(page, player)
        page.wait_for_timeout(900)
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        hits = [c for c in cards if player.split()[-1].lower() in str(c.get("player") or "").lower()
                and str(c.get("pick_type") or "").lower() == "goblin"]
        if hits:
            for c in hits:
                row = (player, c.get("prop_type"), float(c.get("line")), c.get("pick_type"))
                goblins.append(row)
                print(f"OK  {player:22} {c.get('pick_type'):7} {c.get('line')} {c.get('prop_type')}")
        else:
            print(f"--  {player:22} no goblin")
    print(f"\nTOTAL goblin faces={len(goblins)}")
