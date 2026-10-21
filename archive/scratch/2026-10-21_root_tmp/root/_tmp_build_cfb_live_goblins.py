"""Cycle USC-ORE cards to goblin; rebuild tickets from whatever goblin faces remain."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, _norm

CDP = "http://127.0.0.1:9222"
# Targets: (player, prop chip / prop_type, preferred goblin lines from L5=4 list)
TARGETS = [
    ("Jayden Maiava", "Pass Yards", [189.5, 199.5, 259.5]),
    ("Kayden Dixon-Wyatt", "Receiving Yards", [29.5]),
    ("Kayden Dixon-Wyatt", "Receptions", [2.5]),
    ("King Miller", "Rush Yards", [39.5]),
    ("Waymond Jordan", "Rush Yards", [14.5, 19.5]),
    ("Zacharyus Williams", "Receiving Yards", [9.5, 14.5, 19.5]),
    ("Zacharyus Williams", "Receptions", [2.5, 1.5]),
    ("Mark Bowman", "Receiving Yards", [14.5]),
    ("Mark Bowman", "Receptions", [2.5, 3.5]),
]

def find_card(frame, player, prop):
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    hits = []
    for c in cards:
        cn = _norm(c.get("player"))
        if pn not in cn and cn not in pn and _norm(player.split()[-1]) not in cn:
            continue
        cp = _norm(c.get("prop_type"))
        wp = _norm(prop)
        ok = False
        if wp in cp or cp in wp: ok = True
        if "pass" in wp and "yard" in wp and "pass" in cp and "yard" in cp: ok = True
        if "rush" in wp and "yard" in wp and "rush" in cp and "yard" in cp: ok = True
        if "receiv" in wp and "yard" in wp and (("rec" in cp and "yard" in cp) or cp == "recyards"): ok = True
        if wp in ("receptions", "recs") and cp in ("receptions", "recs", "rec"): ok = True
        if ok: hits.append(c)
    return hits[0] if hits else None

live = []
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    page = None
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                page = pg; break
        if page: break
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
    for player, prop, wants in TARGETS:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(prop)
        try:
            P._switch_board_filter(frame, page, chip)
        except Exception:
            pass
        page.wait_for_timeout(600)
        react_search(page, ""); page.wait_for_timeout(60); react_search(page, player); page.wait_for_timeout(1300)
        frame = P.find_prizepicks_frame(page)
        card = find_card(frame, player, prop)
        if card is None:
            print(f"MISS card {player} {prop}")
            continue
        print(f"FOUND {player} {card.get('prop_type')} {card.get('line')} {card.get('pick_type')}")
        # try each preferred line as goblin
        matched = None
        for want in wants + [None]:
            cycled = P.cycle_card_to_pick_type(
                frame,
                card.get("more_btn"),
                player=player,
                prop=card.get("prop_type") or prop,
                want_pick="goblin",
                want_line=want,
                require_line=(want is not None),
                more_index=card.get("more_index"),
            )
            page.wait_for_timeout(400)
            frame = P.find_prizepicks_frame(page)
            # re-find
            cards = P.get_all_cards(frame)
            for c in cards:
                if _norm(player.split()[-1]) not in _norm(c.get("player")): continue
                if str(c.get("pick_type") or "").lower() != "goblin": continue
                cp = _norm(c.get("prop_type")); wp = _norm(prop)
                if not (wp in cp or cp in wp or ("yard" in wp and "yard" in cp) or (wp.startswith("rec") and "rec" in cp)):
                    continue
                matched = c
                break
            if matched:
                break
            # refresh card for next cycle attempt
            card = find_card(frame, player, prop) or card
        if matched:
            print(f"  GOBLIN LIVE {matched.get('player')} {matched.get('prop_type')} O{matched.get('line')}")
            live.append({
                "sport": "CFB", "player": player, "prop_type": matched.get("prop_type") or prop,
                "prop": matched.get("prop_type") or prop, "direction": "OVER",
                "line": float(matched.get("line")), "pick_type": "goblin", "search": player,
            })
        else:
            print(f"  no goblin after cycle {player} {prop}")

print(f"\nLive goblins usable: {len(live)}")
for x in live:
    print(f"  {x['player']} {x['prop_type']} O{x['line']}")

# Pack unique-player Power 3 / Flex from live
seen=set(); uniq=[]
for x in live:
    if x["player"] in seen: continue
    seen.add(x["player"]); uniq.append(x)
print(f"unique players: {len(uniq)}")

if len(uniq) < 2:
    print("Not enough live goblins to build a ticket")
    raise SystemExit(2)

tickets=[]
rem=list(uniq)
# Power 3 if >=3 else Power 2 not allowed on PP usually need 2+ - Power needs 2 min? PP Power is usually 2-6. 2-pick is OK (user had Gasser/Alvarez 2-pick)
while rem:
    n = 3 if len(rem) >= 3 else (2 if len(rem) >= 2 else 0)
    if n == 0: break
    tickets.append({"id": f"CFB-L5={4}-{len(tickets)+1}", "mode": "power", "legs": rem[:n]})
    rem = rem[n:]

def build(page, ticket):
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== BUILD {tid} ========", flush=True)
    for leg in legs:
        print(f"  + {leg['player']} {leg['prop_type']} O{leg['line']}", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for i, leg in enumerate(legs):
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(leg["prop_type"]))
        page.wait_for_timeout(550)
        react_search(page,""); page.wait_for_timeout(50); react_search(page, leg["search"]); page.wait_for_timeout(1200)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try: P.clear_slip(P.find_prizepicks_frame(page))
            except Exception: pass
            return {"id": tid, "ready": False, "fails": [leg["player"]]}
        time.sleep(0.25)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode) or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    print(f"[{tid}] ready={n==len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:300], flush=True)
    return {"id": tid, "ready": n==len(legs), "n": n, "px": px, "legs": [f"{x['player']} {x['prop_type']} O{x['line']}" for x in legs], "fails": []}

results=[]
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    # reuse connection - need new context from same browser after first with block ended
    # Actually we're still inside first with for probe - wait I closed it. Reconnect.
    
# rebuild connection for tickets
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    ctx = browser.contexts[0]
    boards=[]
    for pg in ctx.pages:
        if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
            boards.append(pg)
    while len(boards) < len(tickets):
        pg = ctx.new_page(); pg.goto("https://app.prizepicks.com/board", wait_until="domcontentloaded", timeout=60_000); pg.wait_for_timeout(1000); boards.append(pg)
    for page, ticket in zip(boards, tickets):
        try: results.append(build(page, ticket))
        except Exception as e:
            print("EXC", e); results.append({"id": ticket["id"], "ready": False, "fails":[str(e)]})
    for page, r in zip(boards, results):
        if r.get("ready"):
            page.bring_to_front(); print("Brought", r["id"]); break

print("\nSUMMARY")
for r in results: print(r)
print("Note: live goblins may differ from morning L5=4 yard lines.")
