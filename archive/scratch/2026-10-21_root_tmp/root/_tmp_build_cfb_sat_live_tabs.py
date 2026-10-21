"""Probe live CFB Goblin yard faces vs Sat step8 L5=5 clears; rebuild tab slips."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.cfb_keep_gates import (
    cfb_ticket_fade,
    cfb_pass_yards_goblin_skip_d,
    cfb_list_eligible,
)
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, _norm
from utils.prop_norm import canon_prop

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx"
)
OUT = _REPO / "data" / "reports" / "cfb_sat_live_tabs_2026-09-26.json"

YARD_PROPS = {"pass_yds", "rush_yds", "rec_yds"}


def d_ok(side: str, od: str) -> bool:
    o = str(od).lower()
    if side == "OVER":
        return ("weak" in o) or ("below" in o)
    if side == "UNDER":
        return ("elite" in o) or ("above" in o)
    return False


def load_clears() -> list[dict]:
    df = pd.read_excel(STEP8)
    d = df["Direction"].astype(str).str.upper()
    l5 = pd.to_numeric(df["L5 Over"], errors="coerce").where(
        d == "OVER", pd.to_numeric(df["L5 Under"], errors="coerce")
    )
    l10 = pd.to_numeric(df["L10 Over"], errors="coerce").where(
        d == "OVER", pd.to_numeric(df["L10 Under"], errors="coerce")
    )
    l5avg = pd.to_numeric(df["Last 5 Avg"], errors="coerce")
    line = pd.to_numeric(df["Line"], errors="coerce")
    rows = []
    for i, r in df.iterrows():
        side = str(r["Direction"]).upper()
        if float(l5.loc[i] or 0) < 5 or float(l10.loc[i] or 0) < 8:
            continue
        pt = str(r["Pick Type"])
        if "goblin" not in pt.lower() or side != "OVER":
            continue
        prop = canon_prop("CFB", str(r["Prop"]))
        if prop not in YARD_PROPS:
            continue
        rec = {
            "sport": "CFB",
            "Direction": side,
            "side": side,
            "pick_type": pt,
            "Pick Type": pt,
            "Prop": r["Prop"],
            "prop": r["Prop"],
        }
        if cfb_ticket_fade(rec) or not cfb_list_eligible(rec):
            continue
        skip = cfb_pass_yards_goblin_skip_d(rec)
        od = str(r.get("Def Tier") or "")
        if not skip and not d_ok(side, od):
            continue
        avg, ln = l5avg.loc[i], line.loc[i]
        if pd.isna(avg) or pd.isna(ln):
            continue
        cover = float(avg) - float(ln)
        need = 15 if prop == "pass_yds" else 8
        if cover < need:
            continue
        # PrizePicks board labels Rec Yards, not Receiving Yards
        prop_label = str(r["Prop"])
        if prop == "rec_yds":
            prop_label = "Rec Yards"
        elif prop == "pass_yds":
            prop_label = "Pass Yards"
        elif prop == "rush_yds":
            prop_label = "Rush Yards"
        rows.append(
            {
                "player": str(r["Player"]),
                "team": str(r["Team"]).upper(),
                "prop": prop_label,
                "prop_type": prop_label,
                "line": float(r["Line"]),
                "l5": float(l5.loc[i]),
                "l10": float(l10.loc[i]),
                "cover": cover,
                "od": od,
                "cp": prop,
                "pick_type": "goblin",
                "direction": "OVER",
                "sport": "CFB",
                "search": str(r["Player"]),
            }
        )
    # deepest goblin per player+prop
    best: dict = {}
    for r in rows:
        key = (r["player"], r["prop"])
        if key not in best or r["line"] < best[key]["line"]:
            best[key] = r
    return sorted(best.values(), key=lambda z: (-z["l10"], -z["cover"], z["player"]))


def probe_live(page, clears: list[dict]) -> list[dict]:
    live = []
    P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
    for rec in clears:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(rec["prop"]))
        page.wait_for_timeout(500)
        react_search(page, "")
        page.wait_for_timeout(50)
        react_search(page, rec["player"])
        page.wait_for_timeout(1100)
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        hit = None
        pn = _norm(rec["player"])
        want_prop = _norm(rec["prop"])
        for c in cards:
            cn = _norm(c.get("player"))
            if pn not in cn and cn not in pn:
                continue
            if str(c.get("pick_type") or "").lower() != "goblin":
                continue
            cp = _norm(c.get("prop_type"))
            # Rec Yards vs Receiving Yards
            if want_prop not in cp and cp not in want_prop:
                if not (
                    ("rec" in want_prop and "yard" in want_prop)
                    and ("rec" in cp and "yard" in cp)
                ):
                    continue
            if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                hit = c
                break
        if hit:
            print(
                f"LIVE  {rec['player']:24} {rec['prop']:18} {rec['line']} "
                f"L10={rec['l10']:.0f}",
                flush=True,
            )
            live.append(rec)
        else:
            # show nearest goblin if any
            gob = [
                c
                for c in cards
                if (_norm(rec["player"]) in _norm(c.get("player")) or _norm(c.get("player")) in _norm(rec["player"]))
                and str(c.get("pick_type") or "").lower() == "goblin"
            ]
            alt = ""
            if gob:
                g0 = gob[0]
                alt = f"  live_goblin={g0.get('line')} {g0.get('prop_type')}"
            print(
                f"MISS  {rec['player']:24} {rec['prop']:18} {rec['line']}{alt}",
                flush=True,
            )
    return live


def pack(live: list[dict]) -> list[dict]:
    """Power 3s then Flex 4s; unique players; prefer 2+ teams per slip."""
    remaining = list(live)
    tickets = []
    while len(remaining) >= 3:
        # Prefer Power while we have enough for at least one Flex later, else Power
        want_flex = len(tickets) >= 3 and len(remaining) >= 4
        n = 4 if want_flex else 3
        if n > len(remaining):
            n = 3
        # greedily pick n legs with max unique teams
        chunk = []
        used_p = set()
        used_t = set()
        pool = list(remaining)
        # first take from different teams
        for r in pool:
            if r["player"] in used_p:
                continue
            if r["team"] in used_t and len(chunk) < n - 1:
                # save same-team for later unless we need filler
                continue
            chunk.append(r)
            used_p.add(r["player"])
            used_t.add(r["team"])
            if len(chunk) == n:
                break
        if len(chunk) < n:
            for r in pool:
                if r["player"] in used_p:
                    continue
                chunk.append(r)
                used_p.add(r["player"])
                used_t.add(r["team"])
                if len(chunk) == n:
                    break
        if len(chunk) < 3:
            break
        if len({c["team"] for c in chunk}) < 2:
            # skip this packing attempt: rotate first player to end
            remaining = remaining[1:] + remaining[:1]
            if len(tickets) > 20:
                break
            continue
        mode = "flex" if len(chunk) == 4 else "power"
        tid = f"{'F4' if mode == 'flex' else 'P3'}-{len([t for t in tickets if t['mode'] == mode]) + 1}"
        tickets.append({"id": tid, "mode": mode, "legs": chunk})
        rem_players = {c["player"] for c in chunk}
        remaining = [r for r in remaining if r["player"] not in rem_players]
    return tickets


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for leg in legs:
        print(
            f"  {leg['player']} ({leg['team']}) {leg['prop']} {leg['line']}",
            flush=True,
        )
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(leg["prop"]))
        page.wait_for_timeout(600)
        react_search(page, "")
        page.wait_for_timeout(80)
        react_search(page, leg["search"])
        page.wait_for_timeout(1200)
        frame = P.find_prizepicks_frame(page)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(frame)
            except Exception:
                pass
            return {"id": tid, "ready": False, "n": 0, "px": None, "fails": [leg["player"]]}
        time.sleep(0.25)
    try:
        label = "Flex Play" if mode == "flex" else "Power Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(
            timeout=2000
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
    print(raw[:350], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_cfb_{tid.lower()}_2026-09-26.png"
    try:
        page.screenshot(path=str(shot), full_page=False)
    except Exception:
        pass
    return {
        "id": tid,
        "ready": ready,
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["player"], x["prop"], x["line"]) for x in legs],
    }


def main() -> int:
    clears = load_clears()
    print(f"step8 L5=5 yard clears: {len(clears)}", flush=True)
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = None
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                page = pg
                break
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1200)
        page.bring_to_front()
        live = probe_live(page, clears)
        print(f"\nLive exact-line Goblin pool: {len(live)}", flush=True)
        tickets = pack(live)
        print(f"Packed {len(tickets)} tickets: {[t['id'] for t in tickets]}", flush=True)
        if not tickets:
            OUT.write_text(
                json.dumps({"live": live, "tickets": [], "results": []}, indent=2),
                encoding="utf-8",
            )
            print("Not enough live faces for a Power 3")
            return 2
        # open tabs
        boards = [page]
        while len(boards) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(800)
            boards.append(pg)
            print(f"opened tab {len(boards)}", flush=True)
        results = []
        for pg, ticket in zip(boards, tickets):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]}
                )
        for pg, r in zip(boards, results):
            if r.get("ready"):
                pg.bring_to_front()
                break
        payload = {
            "live_n": len(live),
            "live": [
                {
                    "player": x["player"],
                    "team": x["team"],
                    "prop": x["prop"],
                    "line": x["line"],
                    "l10": x["l10"],
                }
                for x in live
            ],
            "tickets": [
                {
                    "id": t["id"],
                    "mode": t["mode"],
                    "legs": [
                        {
                            "player": l["player"],
                            "team": l["team"],
                            "prop": l["prop"],
                            "line": l["line"],
                        }
                        for l in t["legs"]
                    ],
                }
                for t in tickets
            ],
            "results": results,
        }
        OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')}",
                flush=True,
            )
        print(f"wrote {OUT}", flush=True)
        print("No submit.", flush=True)
        return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
