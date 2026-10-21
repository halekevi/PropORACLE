"""List keep-eligible MLB Goblin OVER with L5=5, then check live board faces."""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from utils.mlb_keep_gates import mlb_goblin_keep_eligible
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import react_search, prop_ok, _norm

p = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/mlb/step8_mlb_direction_clean.xlsx")
df = pd.read_excel(p)

def row_dict(r):
    d = {str(c): r[c] for c in df.columns}
    # normalize for gates
    return {
        "sport": "MLB",
        "side": "OVER",
        "direction": "OVER",
        "pick_type": "goblin",
        "player": str(d.get("Player") or ""),
        "prop": str(d.get("Prop") or ""),
        "prop_type": str(d.get("Prop") or ""),
        "line": float(d.get("Line")),
        "l5_over": float(pd.to_numeric(d.get("L5 Over"), errors="coerce") or 0),
        "l10_over": float(pd.to_numeric(d.get("L10 Over"), errors="coerce") or 0),
        "def_tier": str(d.get("Def Tier") or ""),
        "own_pitch_tier": str(d.get("Own Pitch Tier") or d.get("Own Pitch") or d.get("Def Tier") or ""),
        "opp_bats": str(d.get("Opp Bats") or d.get("Opp Bat Tier") or ""),
        "team": str(d.get("Team") or ""),
        "opp": str(d.get("Opp") or ""),
        **{k: d[k] for k in d},
    }

cands = []
for _, r in df.iterrows():
    if "goblin" not in str(r.get("Pick Type") or "").lower(): continue
    if str(r.get("Direction") or "").upper() != "OVER": continue
    l5 = float(pd.to_numeric(r.get("L5 Over"), errors="coerce") or 0)
    if l5 < 5: continue
    rec = row_dict(r)
    if not mlb_goblin_keep_eligible(rec):
        continue
    cands.append(rec)

# unique by player+prop+line prefer keep
print(f"keep+L5=5 candidates: {len(cands)}")
# prefer deeper cover / higher L10
for rec in sorted(cands, key=lambda x: (-x["l10_over"], x["player"], x["prop"]))[:40]:
    print(f"  L5={rec['l5_over']} L10={rec['l10_over']:.0f} {rec['player']:22} {rec['prop']:22} {rec['line']} D={rec.get('def_tier')}")

# Probe live for top unique players
seen_players=set()
probe=[]
for rec in sorted(cands, key=lambda x: (-x["l10_over"], x["player"])):
    pl=rec["player"]
    if pl in seen_players: continue
    # prefer one face per player - best L10 for that player still L5=5 keep
    faces=[x for x in cands if x["player"]==pl]
    faces.sort(key=lambda x: -x["l10_over"])
    probe.append(faces[0])
    seen_players.add(pl)
    if len(probe)>=18: break

print(f"\nProbing {len(probe)} unique players on live board...")
live_ok=[]
with sync_playwright() as pwy:
    browser = pwy.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=120_000)
    page=None
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                page=pg; break
        if page: break
    page.bring_to_front()
    P.navigate_board_for_sport(page, "MLB", settle_ms=2000)
    for rec in probe:
        player, prop, line = rec["player"], rec["prop"], rec["line"]
        frame=P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(prop))
        page.wait_for_timeout(500)
        react_search(page,""); page.wait_for_timeout(60); react_search(page, player); page.wait_for_timeout(1000)
        frame=P.find_prizepicks_frame(page)
        cards=P.get_all_cards(frame)
        hit=None
        for c in cards:
            if _norm(player) not in _norm(c.get("player")) and _norm(c.get("player")) not in _norm(player):
                continue
            if str(c.get("pick_type") or "").lower()!="goblin":
                continue
            if not prop_ok(prop, str(c.get("prop_type") or "")):
                # allow Ks alias
                if "strike" in _norm(prop) or _norm(prop) in ("ks","pitcherstrikeouts"):
                    if "ks" not in _norm(c.get("prop_type")) and "strike" not in _norm(c.get("prop_type")):
                        continue
                else:
                    continue
            if P._line_key(c.get("line")) == P._line_key(line):
                hit=c; break
        if hit:
            print(f"LIVE  {player:22} {prop:22} {line} (board {hit.get('line')} {hit.get('prop_type')})")
            live_ok.append({**rec, "board_prop": hit.get("prop_type"), "board_line": hit.get("line")})
        else:
            # show goblin alts
            alts=[(c.get("prop_type"), c.get("line")) for c in cards if _norm(player.split()[-1]) in _norm(c.get("player")) and str(c.get("pick_type")).lower()=="goblin"]
            print(f"MISS  {player:22} {prop:22} {line}  alts={alts[:4]}")

print(f"\nLIVE L5=5 keep matches: {len(live_ok)}")
Path(r"H:/PropORACLE/data/reports/pp_l5eq5_live_2026-09-25.json").write_text(
    __import__("json").dumps([{k:v for k,v in r.items() if k in ("player","prop","line","l5_over","l10_over","def_tier","board_prop","board_line")} for r in live_ok], indent=2),
    encoding="utf-8",
)
