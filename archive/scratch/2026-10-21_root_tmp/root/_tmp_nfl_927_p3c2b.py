#!/usr/bin/env python3
"""One more NFL P3 from leftover faces + quick live probe. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, _norm

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = _REPO / "data" / "reports" / "nfl_pp_tabs_2026-09-27_cont2.json"

CANDIDATES = [
    {"player": "David Montgomery", "prop": "Rec Yards", "line": 4.5, "team": "HOU"},
    {"player": "Kyler Murray", "prop": "Pass Yards", "line": 149.5, "team": "MIN"},
    {"player": "Malachi Fields", "prop": "Rec Yards", "line": 4.5, "team": "NYG"},
    {"player": "Jadarian Price", "prop": "Rec Yards", "line": 4.5, "team": "SEA"},
    {"player": "Darnell Washington", "prop": "Recs", "line": 1.5, "team": "PIT"},
]


def as_leg(r):
    return {
        **r,
        "prop_type": r["prop"],
        "pick_type": "goblin",
        "direction": "OVER",
        "side": "OVER",
        "sport": "NFL",
        "search": r["player"],
    }


def probe(page, rec):
    react_search(page, rec["player"])
    page.wait_for_timeout(1200)
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    pn = _norm(rec["player"])
    hits = [c for c in cards if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn]
    want_prop = _norm(rec["prop"])
    for c in hits:
        got = _norm(c.get("prop_type"))
        ok_prop = got == want_prop or (want_prop in ("recs", "receptions") and got in ("recs", "receptions"))
        if not ok_prop and "yard" in want_prop:
            ok_prop = "yard" in got and (
                ("rec" in want_prop and "rec" in got)
                or ("rush" in want_prop and "rush" in got)
                or ("pass" in want_prop and "pass" in got)
            )
        if not ok_prop:
            continue
        if str(c.get("pick_type") or "").lower() != "goblin":
            continue
        if P._line_key(c.get("line")) != P._line_key(rec["line"]):
            # try cycle
            P.cycle_card_to_pick_type(
                frame,
                c.get("more_btn"),
                player=rec["player"],
                prop=c.get("prop_type") or rec["prop"],
                want_pick="goblin",
                want_line=rec["line"],
                require_line=True,
                more_index=c.get("more_index"),
                max_clicks=10,
            )
            page.wait_for_timeout(300)
            react_search(page, rec["player"])
            page.wait_for_timeout(800)
            hits2 = P.get_all_cards(P.find_prizepicks_frame(page))
            for c2 in hits2:
                if pn not in _norm(c2.get("player")) and _norm(c2.get("player")) not in pn:
                    continue
                if str(c2.get("pick_type") or "").lower() != "goblin":
                    continue
                if P._line_key(c2.get("line")) == P._line_key(rec["line"]):
                    print(f"LIVE {rec['player']} {rec['prop']} {rec['line']}", flush=True)
                    return True
            print(f"MOVED {rec['player']} want={rec['line']}", flush=True)
            return False
        print(f"LIVE {rec['player']} {rec['prop']} {rec['line']}", flush=True)
        return True
    print(f"MISS {rec['player']} {rec['prop']} {rec['line']}", flush=True)
    return False


def build(page, legs):
    tid = "P3-C2b"
    print(f"\n======== {tid} ========", flush=True)
    for x in legs:
        print(f"  {x['player']} {x['prop']} {x['line']}", flush=True)
    P.navigate_board_for_sport(page, "NFL", settle_ms=1600)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    for x in legs:
        react_search(page, x["player"])
        page.wait_for_timeout(1300)
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, as_leg(x))
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [x["player"]], "n": 0}
        time.sleep(0.2)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception:
        pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else 3)
    print(f"[{tid}] slip_ok={n==3} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    return {
        "id": tid,
        "ready": n == 3,
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["player"], x["prop"], x["line"], x["team"]) for x in legs],
    }


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = next(
            (pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")),
            None,
        )
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1000)
        P.navigate_board_for_sport(page, "NFL", settle_ms=1500)
        live = [c for c in CANDIDATES if probe(page, c)]
        # diversify teams
        picked = []
        teams = set()
        for c in live:
            if c["player"] in {p["player"] for p in picked}:
                continue
            if c["team"] in teams and len(picked) < 2:
                continue
            picked.append(c)
            teams.add(c["team"])
            if len(picked) == 3:
                break
        if len(picked) < 3:
            for c in live:
                if c["player"] in {p["player"] for p in picked}:
                    continue
                picked.append(c)
                if len(picked) == 3:
                    break
        print("picked", [(p["player"], p["prop"], p["line"]) for p in picked], flush=True)
        if len(picked) < 3:
            print("need 3 live", flush=True)
            OUT.write_text(json.dumps({"live": live, "picked": picked}, indent=2), encoding="utf-8")
            return 2
        res = build(page, picked)
        OUT.write_text(json.dumps({"live": live, "picked": picked, "result": res}, indent=2, default=str), encoding="utf-8")
        print("SUMMARY", res, flush=True)
        return 0 if res.get("ready") else 2


if __name__ == "__main__":
    raise SystemExit(main())
