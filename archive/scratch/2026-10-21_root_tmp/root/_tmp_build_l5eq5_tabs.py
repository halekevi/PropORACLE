"""Build PrizePicks slips using ONLY L5=5 keep-gated MLB Goblins still live."""
from __future__ import annotations
import json, sys, time
from pathlib import Path
import pandas as pd
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.mlb_keep_gates import mlb_goblin_keep_eligible
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, prop_ok, _norm

p = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/mlb/step8_mlb_direction_clean.xlsx")
df = pd.read_excel(p)

cands = []
for _, r in df.iterrows():
    if "goblin" not in str(r.get("Pick Type") or "").lower(): continue
    if str(r.get("Direction") or "").upper() != "OVER": continue
    l5 = float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0)
    if l5 < 5: continue
    rec = {
        "sport": "MLB", "side": "OVER", "direction": "OVER", "pick_type": "Goblin",
        "player": str(r["Player"]), "prop": str(r["Prop"]), "prop_type": str(r["Prop"]),
        "line": float(r["Line"]), "l5_over": l5,
        "l10_over": float(pd.to_numeric(r.get("L10 Over"), errors="coerce") or 0),
        "def_tier": r.get("Def Tier"), "team": r.get("Team"), "opp": r.get("Opp"),
        "search": str(r["Player"]),
    }
    for c in df.columns:
        rec[str(c)] = r[c]
        rec[str(c).lower().replace(" ", "_")] = r[c]
    if mlb_goblin_keep_eligible(rec):
        cands.append(rec)

# unique player: best L10 face
by_player = {}
for rec in cands:
    pl = rec["player"]
    if pl not in by_player or rec["l10_over"] > by_player[pl]["l10_over"]:
        by_player[pl] = rec
uniq = sorted(by_player.values(), key=lambda x: -x["l10_over"])
print(f"unique L5=5 keep players: {len(uniq)}")

live = []
with sync_playwright() as pwy:
    browser = pwy.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=120_000)
    page = None
    boards = []
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                boards.append(pg)
    page = boards[0]
    page.bring_to_front()
    P.navigate_board_for_sport(page, "MLB", settle_ms=2200)
    for rec in uniq:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(rec["prop"]))
        page.wait_for_timeout(450)
        react_search(page, ""); page.wait_for_timeout(50); react_search(page, rec["player"]); page.wait_for_timeout(950)
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        hit = None
        for c in cards:
            cn = _norm(c.get("player"))
            pn = _norm(rec["player"])
            if pn not in cn and cn not in pn: continue
            if str(c.get("pick_type") or "").lower() != "goblin": continue
            if not prop_ok(rec["prop"], str(c.get("prop_type") or "")):
                if "strike" in _norm(rec["prop"]) and ("ks" in _norm(c.get("prop_type")) or "strike" in _norm(c.get("prop_type"))):
                    pass
                else:
                    continue
            if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                hit = c; break
        if hit:
            print(f"LIVE L5=5 {rec['player']:22} {rec['prop']:22} {rec['line']} L10={rec['l10_over']:.0f}")
            live.append({**rec, "pick_type": "goblin"})  # add_cycled wants lower
        else:
            print(f"MISS     {rec['player']:22} {rec['prop']:22} {rec['line']}")

    print(f"\nLive L5=5 pool: {len(live)}")
    if len(live) < 3:
        print("Not enough L5=5 live faces to build Power 3")
        raise SystemExit(2)

    # Pack: Power 3s then Flex 4 if enough leftovers; unique players across all tickets
    remaining = list(live)
    tickets = []
    while len(remaining) >= 3:
        mode = "power" if len(remaining) < 7 else ("flex" if len(tickets) >= 2 and len(remaining) >= 4 else "power")
        n = 4 if mode == "flex" and len(remaining) >= 4 else 3
        if n == 4 and len(remaining) < 4:
            n = 3; mode = "power"
        chunk = remaining[:n]
        remaining = remaining[n:]
        tickets.append({"id": f"L5EQ5-{len(tickets)+1}-{mode[0].upper()}{n}", "mode": mode, "legs": chunk})

    print(f"Building {len(tickets)} tickets on separate tabs...")

    def build_one(pg, ticket):
        tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
        print(f"\n======== {tid} ========", flush=True)
        for leg in legs:
            print(f"  L5={leg['l5_over']} {leg['player']} {leg['prop']} {leg['line']}", flush=True)
        pg.bring_to_front()
        P.navigate_board_for_sport(pg, "MLB", settle_ms=1800)
        frame = P.find_prizepicks_frame(pg)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, pg)
        P.set_ticket_type(frame, mode)
        for leg in legs:
            frame = P.find_prizepicks_frame(pg)
            P.dismiss_modal(frame, pg)
            P._switch_board_filter(frame, pg, P.prop_type_to_board_filter(leg["prop"]))
            pg.wait_for_timeout(600)
            react_search(pg, ""); pg.wait_for_timeout(80); react_search(pg, leg["player"]); pg.wait_for_timeout(1200)
            frame = P.find_prizepicks_frame(pg)
            r = add_cycled_leg(P, frame, pg, leg)
            if not r.ok:
                print(r.loud_fail(), flush=True)
                try: P.clear_slip(frame)
                except Exception: pass
                return {"id": tid, "ready": False, "fails": [leg["player"]], "n": 0}
            time.sleep(0.25)
        try:
            P.find_prizepicks_frame(pg).get_by_text("Flex Play" if mode=="flex" else "Power Play", exact=True).first.click(timeout=2000)
        except Exception: pass
        slip = P.read_slip(P.find_prizepicks_frame(pg), n_legs=len(legs), ticket_type=mode) or {}
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        n = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, n if n else len(legs))
        ready = n == len(legs)
        print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
        print(raw[:350], flush=True)
        shot = _REPO / "data" / "reports" / f"pp_{tid.lower()}_2026-09-25.png"
        try: pg.screenshot(path=str(shot), full_page=False)
        except Exception: pass
        return {"id": tid, "ready": ready, "n": n, "px": px, "fails": [], "legs": [(x["player"], x["prop"], x["line"], x["l5_over"]) for x in legs]}

    # ensure enough board tabs
    while len(boards) < len(tickets):
        ctx = browser.contexts[0]
        pg = ctx.new_page()
        pg.goto("https://app.prizepicks.com/board", wait_until="domcontentloaded", timeout=60_000)
        pg.wait_for_timeout(1200)
        boards.append(pg)

    results = []
    for pg, ticket in zip(boards, tickets):
        try:
            results.append(build_one(pg, ticket))
        except Exception as e:
            print(f"{ticket['id']} EXC {e}", flush=True)
            results.append({"id": ticket["id"], "ready": False, "fails": [str(e)]})

    for pg, r in zip(boards, results):
        if r.get("ready"):
            pg.bring_to_front()
            print(f"\nBrought {r['id']} to front", flush=True)
            break

print("\nSUMMARY")
for r in results:
    print(f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} N-correct={r.get('px')} legs={r.get('legs')} fails={r.get('fails')}")
print("All legs L5=5. No submit.")
