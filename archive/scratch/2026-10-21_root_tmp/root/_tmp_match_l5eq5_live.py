"""Match live goblin faces to step8 L5=5 keep rows (any line that still clears)."""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.mlb_keep_gates import mlb_goblin_keep_eligible
from utils.pp_manual_slip import react_search, prop_ok, _norm
from utils.prop_norm import canon_prop

p = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/mlb/step8_mlb_direction_clean.xlsx")
df = pd.read_excel(p)

def make_rec(r):
    rec = {
        "sport": "MLB", "side": "OVER", "direction": "OVER", "pick_type": "Goblin",
        "player": str(r["Player"]), "prop": str(r["Prop"]), "prop_type": str(r["Prop"]),
        "line": float(r["Line"]),
        "l5_over": float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0),
        "l10_over": float(pd.to_numeric(r.get("L10 Over"), errors="coerce") or 0),
        "def_tier": r.get("Def Tier"), "team": r.get("Team"), "opp": r.get("Opp"),
        "search": str(r["Player"]),
    }
    for c in df.columns:
        rec[str(c)] = r[c]
        rec[str(c).lower().replace(" ", "_")] = r[c]
    return rec

# All L5=5 keep faces (may be multiple per player)
l5eq5 = []
for _, r in df.iterrows():
    if "goblin" not in str(r.get("Pick Type") or "").lower(): continue
    if str(r.get("Direction") or "").upper() != "OVER": continue
    if float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0) < 5: continue
    rec = make_rec(r)
    if mlb_goblin_keep_eligible(rec):
        l5eq5.append(rec)

players = sorted({r["player"] for r in l5eq5})
print(f"L5=5 keep faces={len(l5eq5)} players={len(players)}")

live = []
with sync_playwright() as pwy:
    browser = pwy.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=120_000)
    page = None
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                page = pg; break
        if page: break
    page.bring_to_front()
    P.navigate_board_for_sport(page, "MLB", settle_ms=2200)
    for player in players:
        faces = [r for r in l5eq5 if r["player"] == player]
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        react_search(page, ""); page.wait_for_timeout(50); react_search(page, player); page.wait_for_timeout(1100)
        # Popular filter to see all goblins
        try:
            P._switch_board_filter(frame, page, "Popular")
            page.wait_for_timeout(400)
        except Exception:
            pass
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        goblins = []
        for c in cards:
            cn = _norm(c.get("player")); pn = _norm(player)
            if pn not in cn and cn not in pn: continue
            if str(c.get("pick_type") or "").lower() != "goblin": continue
            goblins.append(c)
        matched = None
        for c in goblins:
            for face in faces:
                # prop match flexible
                same_prop = prop_ok(face["prop"], str(c.get("prop_type") or ""))
                if not same_prop:
                    fp = canon_prop("MLB", face["prop"])
                    cp = canon_prop("MLB", str(c.get("prop_type") or ""))
                    same_prop = fp == cp
                if not same_prop:
                    continue
                if P._line_key(c.get("line")) == P._line_key(face["line"]):
                    matched = {**face, "pick_type": "goblin", "board_prop": c.get("prop_type"), "board_line": c.get("line")}
                    break
            if matched: break
        if matched:
            print(f"LIVE L5=5 {matched['player']:22} {matched['prop']:22} {matched['line']} L10={matched['l10_over']:.0f}")
            live.append(matched)
        else:
            alt = [(c.get("prop_type"), c.get("line")) for c in goblins]
            want = [(f["prop"], f["line"]) for f in faces]
            print(f"MISS {player:22} want={want[:3]} board_goblins={alt[:5]}")

print(f"\nMatched live L5=5: {len(live)}")
Path(r"H:/PropORACLE/data/reports/pp_l5eq5_matched.json").write_text(
    __import__("json").dumps([{k:r[k] for k in ("player","prop","line","l5_over","l10_over")} for r in live], indent=2),
    encoding="utf-8",
)
