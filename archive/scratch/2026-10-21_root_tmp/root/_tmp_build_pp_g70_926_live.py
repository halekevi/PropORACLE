#!/usr/bin/env python3
"""Probe live G70 faces for 2026-09-26, pack P3/F4 from what still matches, build PP tabs."""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.pp_manual_slip import (
    add_cycled_leg,
    n_correct_power,
    order_legs_tennis_first,
    prop_ok,
    react_search,
    _norm,
)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
G70 = Path(r"H:/PropORACLE_main_cp/data/reports/goblin70_tickets_2026-09-26.json")
OUT = _REPO / "data" / "reports" / "pp_g70_live_rebuild_2026-09-26.json"

PROP_FIX = {
    "rushing_yards": "Rush Yards",
    "receiving_yards": "Rec Yards",
    "passing_yards": "Pass Yards",
}


def _prop(raw: str) -> str:
    s = str(raw or "").strip()
    key = re.sub(r"[_\s]+", " ", s).strip().lower()
    if key in PROP_FIX:
        return PROP_FIX[key]
    if "_" in s:
        return " ".join(w.capitalize() for w in s.split("_"))
    return s


def candidate_legs() -> list[dict]:
    d = json.loads(G70.read_text(encoding="utf-8"))
    best: dict[tuple, dict] = {}
    for t in d.get("tickets") or []:
        for L in t.get("legs") or []:
            pick = str(L.get("pick_type") or L.get("Pick Type") or "goblin").lower()
            if pick != "goblin":
                continue
            side = str(L.get("direction") or L.get("side") or "OVER").upper()
            if side != "OVER":
                continue
            player = str(L.get("player") or "").strip()
            prop = _prop(L.get("prop") or L.get("Prop") or "")
            sport = str(L.get("sport") or "").upper()
            line = float(L.get("line") or L.get("Line"))
            k = (sport, _norm(player), _norm(prop))
            # prefer deeper (lower) goblin line
            cur = best.get(k)
            if cur is None or line < cur["line"]:
                best[k] = {
                    "sport": sport,
                    "player": player,
                    "search": player,
                    "prop_type": prop,
                    "prop": prop,
                    "direction": "OVER",
                    "side": "OVER",
                    "line": line,
                    "pick_type": "goblin",
                }
    return sorted(best.values(), key=lambda z: (z["sport"], z["player"], z["prop_type"]))


def hidden_search(page, player: str) -> list[dict]:
    page.evaluate(
        """(q) => {
          const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
          if (!inp) return false;
          const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
          setter.call(inp, '');
          inp.dispatchEvent(new Event('input', {bubbles:true}));
          setter.call(inp, q);
          inp.dispatchEvent(new Event('input', {bubbles:true}));
          return true;
        }""",
        player,
    )
    page.wait_for_timeout(1200)
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    return [
        c
        for c in cards
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]


def probe_one(page, leg: dict) -> dict | None:
    sp = leg["sport"]
    P.navigate_board_for_sport(page, sp, settle_ms=1600)
    hits = hidden_search(page, leg["player"])
    if not hits:
        # last-name fallback
        hits = hidden_search(page, leg["player"].split()[-1])
    if not hits:
        print(f"MISS {sp} {leg['player']} {leg['prop_type']} {leg['line']}", flush=True)
        return None
    start = next(
        (
            c
            for c in hits
            if prop_ok(leg["prop_type"], str(c.get("prop_type") or ""))
        ),
        hits[0],
    )
    exact = any(
        prop_ok(leg["prop_type"], str(c.get("prop_type") or ""))
        and str(c.get("pick_type") or "").lower() == "goblin"
        and P._line_key(c.get("line")) == P._line_key(leg["line"])
        for c in hits
    )
    if not exact:
        frame = P.find_prizepicks_frame(page)
        P.cycle_card_to_pick_type(
            frame,
            start.get("more_btn"),
            player=leg["player"],
            prop=start.get("prop_type") or leg["prop_type"],
            want_pick="goblin",
            want_line=leg["line"],
            require_line=True,
            more_index=start.get("more_index"),
        )
        page.wait_for_timeout(350)
        hits = hidden_search(page, leg["player"])
        exact = any(
            prop_ok(leg["prop_type"], str(c.get("prop_type") or ""))
            and str(c.get("pick_type") or "").lower() == "goblin"
            and P._line_key(c.get("line")) == P._line_key(leg["line"])
            for c in hits
        )
    if not exact:
        # accept any live goblin same prop (deeper/shallower) — report live line
        live_g = [
            c
            for c in hits
            if prop_ok(leg["prop_type"], str(c.get("prop_type") or ""))
            and str(c.get("pick_type") or "").lower() == "goblin"
        ]
        if not live_g:
            print(
                f"MOVED {sp} {leg['player']} want={leg['line']} (no goblin)",
                flush=True,
            )
            return None
        # prefer deepest goblin
        live_g.sort(key=lambda c: float(c.get("line") or 99))
        live_line = float(live_g[0].get("line"))
        print(
            f"LIVE ALT {sp} {leg['player']} {leg['prop_type']} "
            f"{leg['line']}->{live_line}",
            flush=True,
        )
        return {**leg, "line": live_line, "alt": True}
    print(
        f"LIVE OK {sp} {leg['player']} {leg['prop_type']} {leg['line']}",
        flush=True,
    )
    return {**leg, "alt": False}


def pack(live: list[dict]) -> list[dict]:
    """Pack up to 5 Power3 + 4 Flex4, unique players, 2+ sports when possible."""
    rem = list(live)
    tickets: list[dict] = []

    def take(n: int, mode: str, tid: str) -> bool:
        nonlocal rem
        chunk = []
        used_p: set[str] = set()
        used_sp: set[str] = set()
        for r in rem:
            if r["player"] in used_p:
                continue
            # diversify sports early
            if r["sport"] in used_sp and len(chunk) < n - 1 and len(used_sp) < n:
                continue
            chunk.append(r)
            used_p.add(r["player"])
            used_sp.add(r["sport"])
            if len(chunk) == n:
                break
        if len(chunk) < n:
            for r in rem:
                if r["player"] in used_p:
                    continue
                chunk.append(r)
                used_p.add(r["player"])
                if len(chunk) == n:
                    break
        if len(chunk) < n:
            return False
        tickets.append({"id": tid, "mode": mode, "legs": chunk})
        taken = {c["player"] for c in chunk}
        rem = [r for r in rem if r["player"] not in taken]
        return True

    i = 1
    while i <= 5 and len(rem) >= 3:
        if not take(3, "power", f"P3-{i}"):
            break
        i += 1
    j = 1
    while j <= 4 and len(rem) >= 4:
        if not take(4, "flex", f"F4-{j}"):
            break
        j += 1
    return tickets


def _land_sport(page, frame, sport: str, *, allow_goto: bool):
    if allow_goto:
        P.navigate_board_for_sport(page, sport, settle_ms=1800)
        return P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, sport, settle_ms=1500)
    if switched is None:
        # soccer/CFB chips flaky — one goto retry only empties slip; abort instead
        print(f"[SLIP] sport tab miss {sport}", flush=True)
        return None
    return switched


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    legs = order_legs_tennis_first([{**x, "search": x["player"]} for x in legs])
    print(f"\n======== {tid} {mode.upper()} ========", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    added = []
    for i, leg in enumerate(legs):
        frame = P.find_prizepicks_frame(page)
        landed = _land_sport(page, frame, leg["sport"], allow_goto=(i == 0))
        if landed is None:
            try:
                P.clear_slip(frame)
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": 0}
        frame = landed
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(700)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, leg["search"])
        page.wait_for_timeout(1300)
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": len(added)}
        added.append(leg["player"])
        time.sleep(0.2)
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
    print(f"[{tid}] ready={n==len(legs)} n={n} N-correct={px}", flush=True)
    return {
        "id": tid,
        "ready": n == len(legs),
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["player"], x["prop_type"], x["line"], x["sport"]) for x in legs],
    }


def main() -> int:
    cands = candidate_legs()
    print(f"candidates from G70 report: {len(cands)}", flush=True)
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = next(
            (
                pg
                for pg in ctx.pages
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
            ),
            None,
        )
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1200)
        page.bring_to_front()
        live = []
        for leg in cands:
            hit = probe_one(page, leg)
            if hit:
                live.append(hit)
        print(f"\nlive goblin faces={len(live)}", flush=True)
        tickets = pack(live)
        print(f"packed {[t['id'] for t in tickets]}", flush=True)
        for t in tickets:
            print(
                f"  {t['id']}: "
                + " | ".join(
                    f"{l['sport']} {l['player']} {l['line']}" for l in t["legs"]
                ),
                flush=True,
            )
        if not tickets:
            print("Not enough live faces for a Power 3")
            OUT.write_text(
                json.dumps({"live": live, "tickets": [], "results": []}, indent=2),
                encoding="utf-8",
            )
            return 2

        pages = [page]
        while len(pages) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(900)
            pages.append(pg)
            print(f"opened tab {len(pages)}", flush=True)

        results = []
        for pg, ticket in zip(pages, tickets):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "fails": [str(e)], "n": 0}
                )

        OUT.write_text(
            json.dumps(
                {
                    "live": [
                        {
                            "sport": x["sport"],
                            "player": x["player"],
                            "prop": x["prop_type"],
                            "line": x["line"],
                            "alt": x.get("alt"),
                        }
                        for x in live
                    ],
                    "tickets": [
                        {
                            "id": t["id"],
                            "mode": t["mode"],
                            "legs": [
                                {
                                    "sport": l["sport"],
                                    "player": l["player"],
                                    "prop": l["prop_type"],
                                    "line": l["line"],
                                }
                                for l in t["legs"]
                            ],
                        }
                        for t in tickets
                    ],
                    "results": results,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
                flush=True,
            )
        print(f"wrote {OUT}", flush=True)
        return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
