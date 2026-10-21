"""For each L5=5-capable player, match LIVE goblin line to step8 L5 counts; keep if L5>=5 + keep gate."""
from __future__ import annotations
import sys, time, json
from pathlib import Path
import pandas as pd
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.mlb_keep_gates import mlb_goblin_keep_eligible
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, prop_ok, _norm
from utils.prop_norm import canon_prop

p = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/mlb/step8_mlb_direction_clean.xlsx")
df = pd.read_excel(p)

# index step8 goblin overs by player
by_player = {}
for _, r in df.iterrows():
    if "goblin" not in str(r.get("Pick Type") or "").lower(): continue
    if str(r.get("Direction") or "").upper() != "OVER": continue
    pl = str(r["Player"])
    by_player.setdefault(pl, []).append(r)

players = sorted(by_player.keys())
# Focus on players who had ANY L5=5 goblin today (broader than keep) OR appear on evening slate
focus = [pl for pl, rows in by_player.items()
         if any(float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0) >= 5 for r in rows)]
print(f"players with any L5=5 goblin face in step8: {len(focus)}")

def row_to_rec(r, pick_lower=False):
    rec = {
        "sport": "MLB", "side": "OVER", "direction": "OVER",
        "pick_type": "goblin" if pick_lower else "Goblin",
        "player": str(r["Player"]), "prop": str(r["Prop"]), "prop_type": str(r["Prop"]),
        "line": float(r["Line"]),
        "l5_over": float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0),
        "l10_over": float(pd.to_numeric(r.get("L10 Over"), errors="coerce") or 0),
        "search": str(r["Player"]),
    }
    for c in df.columns:
        rec[str(c)] = r[c]
        rec[str(c).lower().replace(" ", "_")] = r[c]
    return rec

live_l5eq5 = []
with sync_playwright() as pwy:
    browser = pwy.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=120_000)
    boards = []
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                boards.append(pg)
    page = boards[0]
    page.bring_to_front()
    P.navigate_board_for_sport(page, "MLB", settle_ms=2000)

    for player in focus:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        try:
            P._switch_board_filter(frame, page, "Popular")
        except Exception:
            pass
        page.wait_for_timeout(300)
        react_search(page, ""); page.wait_for_timeout(40); react_search(page, player); page.wait_for_timeout(900)
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        goblins = []
        for c in cards:
            cn=_norm(c.get("player")); pn=_norm(player)
            if pn not in cn and cn not in pn: continue
            if str(c.get("pick_type") or "").lower() != "goblin": continue
            goblins.append(c)
        if not goblins:
            continue
        best = None
        for c in goblins:
            for r in by_player[player]:
                if not prop_ok(str(r["Prop"]), str(c.get("prop_type") or "")):
                    if canon_prop("MLB", str(r["Prop"])) != canon_prop("MLB", str(c.get("prop_type") or "")):
                        continue
                if P._line_key(r["Line"]) != P._line_key(c.get("line")):
                    continue
                l5 = float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0)
                if l5 < 5:
                    continue
                rec = row_to_rec(r)
                # require keep for ticket quality
                if not mlb_goblin_keep_eligible({**rec, "pick_type": "Goblin"}):
                    continue
                cand = {**rec, "pick_type": "goblin", "board_prop": c.get("prop_type"), "board_line": c.get("line")}
                if best is None or cand["l10_over"] > best["l10_over"]:
                    best = cand
        if best:
            print(f"OK L5=5 {best['player']:22} {best['prop']:22} {best['line']} L10={best['l10_over']:.0f}")
            live_l5eq5.append(best)
        else:
            # show why: live lines vs L5
            info=[]
            for c in goblins:
                for r in by_player[player]:
                    if canon_prop("MLB", str(r["Prop"])) == canon_prop("MLB", str(c.get("prop_type") or "")) and P._line_key(r["Line"])==P._line_key(c.get("line")):
                        info.append((c.get("prop_type"), c.get("line"), float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0)))
            if info:
                print(f"no-keep/L5 {player:22} live_matched={info[:4]}")

    print(f"\nUsable live L5=5 keep: {len(live_l5eq5)}")
    if len(live_l5eq5) < 3:
        # Fallback: L5=5 only (no keep) for visibility? User asked L5=5 — use L5=5 even if keep fails? 
        # Prefer keep; if <3, widen to L5=5 any keep-prop without full D gate
        print("Widening to L5=5 on keep props even if D/L10 gate fails (still L5=5)...")
        live2=[]
        for player in focus:
            frame = P.find_prizepicks_frame(page)
            react_search(page, ""); page.wait_for_timeout(40); react_search(page, player); page.wait_for_timeout(800)
            cards = P.get_all_cards(P.find_prizepicks_frame(page))
            for c in cards:
                cn=_norm(c.get("player")); pn=_norm(player)
                if pn not in cn and cn not in pn: continue
                if str(c.get("pick_type") or "").lower() != "goblin": continue
                for r in by_player[player]:
                    if canon_prop("MLB", str(r["Prop"])) != canon_prop("MLB", str(c.get("prop_type") or "")):
                        continue
                    if P._line_key(r["Line"]) != P._line_key(c.get("line")): continue
                    l5 = float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0)
                    if l5 < 5: continue
                    prop = canon_prop("MLB", str(r["Prop"]))
                    from utils.mlb_keep_gates import KEEP_PROPS
                    if prop not in KEEP_PROPS: continue
                    rec = row_to_rec(r, pick_lower=True)
                    live2.append(rec)
                    print(f"WIDE L5=5 {rec['player']:22} {rec['prop']:22} {rec['line']} L5={l5} L10={rec['l10_over']:.0f}")
                    break
        # unique players
        seen=set(); live_l5eq5=[]
        for r in sorted(live2, key=lambda x: -x["l10_over"]):
            if r["player"] in seen: continue
            seen.add(r["player"]); live_l5eq5.append(r)
        print(f"Wide pool unique: {len(live_l5eq5)}")

    if len(live_l5eq5) < 3:
        print("STILL not enough — cannot build L5=5-only tickets from live board")
        raise SystemExit(2)

    # unique already
    remaining = list(live_l5eq5)
    tickets=[]
    while len(remaining) >= 3:
        n = 3
        mode = "power"
        if len(remaining) >= 7 and len(tickets) >= 1:
            n=4; mode="flex"
        if len(remaining) < n: break
        tickets.append({"id": f"L5={5}-{len(tickets)+1}", "mode": mode, "legs": remaining[:n]})
        remaining = remaining[n:]

    print(f"Tickets to build: {len(tickets)}")

    def build_one(pg, ticket):
        tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
        print(f"\n======== {tid} {mode} ========", flush=True)
        for leg in legs:
            print(f"  L5={int(leg['l5_over'])} {leg['player']} {leg['prop']} O{leg['line']}", flush=True)
        pg.bring_to_front()
        P.navigate_board_for_sport(pg, "MLB", settle_ms=1600)
        frame = P.find_prizepicks_frame(pg)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, pg)
        P.set_ticket_type(frame, mode)
        for leg in legs:
            frame = P.find_prizepicks_frame(pg)
            P.dismiss_modal(frame, pg)
            P._switch_board_filter(frame, pg, P.prop_type_to_board_filter(leg["prop"]))
            pg.wait_for_timeout(550)
            react_search(pg,""); pg.wait_for_timeout(60); react_search(pg, leg["player"]); pg.wait_for_timeout(1100)
            r = add_cycled_leg(P, frame, pg, leg)
            if not r.ok:
                print(r.loud_fail(), flush=True)
                try: P.clear_slip(P.find_prizepicks_frame(pg))
                except Exception: pass
                return {"id": tid, "ready": False, "fails": [leg["player"]]}
            time.sleep(0.2)
        try:
            P.find_prizepicks_frame(pg).get_by_text("Flex Play" if mode=="flex" else "Power Play", exact=True).first.click(timeout=2000)
        except Exception: pass
        slip = P.read_slip(P.find_prizepicks_frame(pg), n_legs=len(legs), ticket_type=mode) or {}
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        n = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, n if n else len(legs))
        print(f"[{tid}] ready={n==len(legs)} n={n} N-correct={px}", flush=True)
        print(raw[:300], flush=True)
        return {"id": tid, "ready": n==len(legs), "n": n, "px": px, "legs": [(x["player"], x["prop"], x["line"], int(x["l5_over"])) for x in legs], "fails":[]}

    while len(boards) < len(tickets):
        pg = browser.contexts[0].new_page()
        pg.goto("https://app.prizepicks.com/board", wait_until="domcontentloaded", timeout=60_000)
        pg.wait_for_timeout(1000); boards.append(pg)

    # clear other board tabs first so user only sees L5=5 slips
    for pg in boards[len(tickets):]:
        try:
            frame=P.find_prizepicks_frame(pg); P.clear_slip(frame)
        except Exception: pass

    results=[]
    for pg, ticket in zip(boards, tickets):
        try: results.append(build_one(pg, ticket))
        except Exception as e:
            print("EXC", e); results.append({"id": ticket["id"], "ready": False, "fails":[str(e)]})
    for pg,r in zip(boards, results):
        if r.get("ready"):
            pg.bring_to_front(); print("Brought", r["id"], "to front"); break

print("\nSUMMARY")
for r in results:
    print(r)
