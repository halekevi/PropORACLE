"""Probe live goblin faces for failed 9/25 legs."""
from __future__ import annotations
import sys
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import react_search, prop_ok, _norm

TARGETS = [
    ("TENNIS", "Daniil Medvedev", "Total Games"),
    ("SOCCER", "Michael Olise", "Shots On Target"),
    ("SOCCER", "Alan Vega", "Shots"),
    ("SOCCER", "Estéban Lepaul", "Shots On Target"),
    ("SOCCER", "Camilo Vargas", "Goalie Saves"),
    ("MLB", "Eury Pérez", "Pitcher Strikeouts"),
    ("MLB", "Gavin Williams", "Pitcher Strikeouts"),
    ("MLB", "Reid Detmers", "Pitches Thrown"),
    ("MLB", "Robert Gasser", "Pitcher Strikeouts"),
    ("MLB", "Hunter Brown", "Walks Allowed"),
]

CDP = "http://127.0.0.1:9222"
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=60_000)
    page = None
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                page = pg
                break
        if page:
            break
    if not page:
        page = browser.contexts[0].pages[0]
    page.bring_to_front()
    last = None
    for sport, player, prop in TARGETS:
        if sport != last:
            P.navigate_board_for_sport(page, sport, settle_ms=2000)
            last = sport
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(prop)
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(600)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, player)
        page.wait_for_timeout(1200)
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        pn = _norm(player)
        hits = []
        for c in cards:
            cn = _norm(c.get("player"))
            if pn not in cn and cn not in pn:
                continue
            if prop_ok(prop, str(c.get("prop_type") or "")) or _norm(prop) in _norm(c.get("prop_type")) or _norm(c.get("prop_type")) in _norm(prop):
                hits.append(c)
            elif "ks" in _norm(prop) and ("ks" in _norm(c.get("prop_type")) or "strike" in _norm(c.get("prop_type"))):
                hits.append(c)
        print(f"\n== {sport} {player} {prop} ==")
        if not hits:
            print("  NO HITS; sample:", [(c.get("player"), c.get("line"), c.get("prop_type"), c.get("pick_type")) for c in cards[:6]])
        for c in hits:
            print(f"  {c.get('pick_type'):8} {c.get('line')} {c.get('prop_type')}")
