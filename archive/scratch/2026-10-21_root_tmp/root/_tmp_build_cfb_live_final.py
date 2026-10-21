"""Probe current goblin lines then build Power 3 with exact live faces."""
from __future__ import annotations
import sys, time
from pathlib import Path
import pandas as pd
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, find_card
import utils.pp_manual_slip as M
M.PROP_ALIASES["recs"] = ("recs", "receptions")
M.PROP_ALIASES["receptions"] = ("recs", "receptions")
M.PROP_ALIASES["passtds"] = ("passtds", "passingtds")

CDP = "http://127.0.0.1:9222"
# check L5 for live lines in step8
s8 = pd.read_excel(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx")

def l5_for(player, prop_substr, line):
    sub = s8[s8["Player"].astype(str).str.contains(player.split()[-1], case=False, na=False)]
    sub = sub[sub["Prop"].astype(str).str.contains(prop_substr, case=False, na=False)]
    sub = sub[pd.to_numeric(sub["Line"], errors="coerce") == float(line)]
    if sub.empty: return None
    r = sub.iloc[0]
    return float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0), str(r.get("Pick Type")), float(pd.to_numeric(r.get("L10 Over"), errors="coerce") or 0)

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    page = [pg for ctx in browser.contexts for pg in ctx.pages if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")][0]
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=1800)
    probes = [
        ("Kayden Dixon-Wyatt", "Dixon-Wyatt", "Receptions", "Rec"),
        ("Zacharyus Williams", "Zacharyus", "Receptions", "Rec"),
        ("Mark Bowman", "Bowman", "Receptions", "Rec"),
        ("Jayden Maiava", "Maiava", "Pass TDs", "Pass TD"),
    ]
    live_legs = []
    for player, search, prop, prop_key in probes:
        frame = P.find_prizepicks_frame(page)
        P._switch_board_filter(frame, page, "Receptions" if "Rec" in prop else "Pass TDs")
        page.wait_for_timeout(500)
        react_search(page,""); page.wait_for_timeout(40); react_search(page, search); page.wait_for_timeout(1100)
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        gobs = []
        for c in cards:
            if search.lower().split()[-1] not in str(c.get("player") or "").lower() and player.split()[-1].lower() not in str(c.get("player") or "").lower():
                continue
            if str(c.get("pick_type") or "").lower() != "goblin":
                continue
            gobs.append(c)
        if not gobs:
            # try cycle from any card
            anyc = None
            for c in cards:
                if player.split()[-1].lower() in str(c.get("player") or "").lower():
                    anyc = c; break
            if anyc and anyc.get("more_btn"):
                P.cycle_card_to_pick_type(frame, anyc.get("more_btn"), player=player, prop=anyc.get("prop_type") or prop, want_pick="goblin", want_line=None, require_line=False, more_index=anyc.get("more_index"))
                page.wait_for_timeout(400)
                frame = P.find_prizepicks_frame(page)
                for c in P.get_all_cards(frame):
                    if player.split()[-1].lower() in str(c.get("player") or "").lower() and str(c.get("pick_type") or "").lower()=="goblin":
                        gobs.append(c)
        if not gobs:
            print(f"NO GOBLIN {player}")
            continue
        # deepest = lowest line
        gobs.sort(key=lambda c: float(c.get("line") or 999))
        c = gobs[0]
        line = float(c.get("line"))
        bprop = str(c.get("prop_type") or prop)
        info = l5_for(player, prop_key.replace(" ", ""), line) or l5_for(player, bprop[:3], line)
        print(f"LIVE {player} {bprop} O{line} goblin  step8_L5={info}")
        live_legs.append({"sport":"CFB","player":player,"prop_type":bprop,"prop":bprop,"direction":"OVER","line":line,"pick_type":"goblin","search":search,"l5": None if not info else info[0]})

    # Prefer legs that still have L5>=4 in step8; else take live goblins and warn
    good = [x for x in live_legs if x.get("l5") is not None and x["l5"] >= 4]
    pool = good if len(good) >= 2 else live_legs
    print("pool", [(x["player"], x["prop_type"], x["line"], x.get("l5")) for x in pool])
    if len(pool) < 2:
        print("not enough"); raise SystemExit(2)
    legs = pool[:3] if len(pool) >= 3 else pool[:2]
    print("BUILDING", [(x["player"], x["line"]) for x in legs])

    P.clear_slip(P.find_prizepicks_frame(page))
    _, frame = P.verify_slip_empty(P.find_prizepicks_frame(page), page)
    P.set_ticket_type(frame, "power")
    for leg in legs:
        print("+", leg["player"], leg["prop_type"], leg["line"], flush=True)
        frame = P.find_prizepicks_frame(page)
        chip = "Receptions" if "rec" in leg["prop_type"].lower() else "Pass TDs"
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(500)
        react_search(page,""); page.wait_for_timeout(40); react_search(page, leg["search"]); page.wait_for_timeout(1200)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            # click whatever goblin is showing for player
            frame = P.find_prizepicks_frame(page)
            card = None
            for c in P.get_all_cards(frame):
                if leg["player"].split()[-1].lower() in str(c.get("player") or "").lower() and str(c.get("pick_type")).lower()=="goblin":
                    card = c; break
            if not card or not card.get("more_btn"):
                P.clear_slip(frame); raise SystemExit(2)
            print("manual", card.get("line"), card.get("prop_type"), flush=True)
            try: card["more_btn"].click(timeout=1500, no_wait_after=True)
            except Exception: card["more_btn"].click(force=True, timeout=2500, no_wait_after=True)
            page.wait_for_timeout(600)
        time.sleep(0.25)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception: pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    print(f"READY={n==len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:450], flush=True)
    page.bring_to_front()
    page.screenshot(path=str(_REPO/"data/reports/pp_cfb_usc_live_p3.png"), full_page=False)
    print("No submit.")
    raise SystemExit(0 if n == len(legs) else 2)
