"""Finish F5-2 on second board tab from remaining L5=5 live Goblins."""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, prop_ok, _norm

CDP = "http://127.0.0.1:9222"
OUT = _REPO / "data" / "reports" / "cfb_sat_flex46_2026-09-26.json"

F5 = {
    "id": "F5-2",
    "mode": "flex",
    "legs": [
        {
            "sport": "CFB",
            "player": "Jayden Maiava",
            "prop": "Pass Yards",
            "prop_type": "Pass Yards",
            "line": 189.5,
            "pick_type": "goblin",
            "direction": "OVER",
            "team": "USC",
        },
        {
            "sport": "CFB",
            "player": "Avery Johnson",
            "prop": "Rush Yards",
            "prop_type": "Rush Yards",
            "line": 19.5,
            "pick_type": "goblin",
            "direction": "OVER",
            "team": "KSU",
        },
        {
            "sport": "CFB",
            "player": "Julian Sayin",
            "prop": "Pass Yards",
            "prop_type": "Pass Yards",
            "line": 209.5,
            "pick_type": "goblin",
            "direction": "OVER",
            "team": "OSU",
        },
        {
            "sport": "CFB",
            "player": "Junior Sherrill",
            "prop": "Rec Yards",
            "prop_type": "Rec Yards",
            "line": 39.5,
            "pick_type": "goblin",
            "direction": "OVER",
            "team": "VAN",
        },
        {
            "sport": "CFB",
            "player": "Caden Knighten",
            "prop": "Rush Yards",
            "prop_type": "Rush Yards",
            "line": 19.5,
            "pick_type": "goblin",
            "direction": "OVER",
            "team": "BAY",
        },
    ],
}


def yard_ok(want: str, got: str) -> bool:
    if prop_ok(want, got):
        return True
    w, g = _norm(want), _norm(got)
    for a in ("pass", "rush", "rec"):
        if a in w and "yard" in w and a in g and "yard" in g:
            return True
    return False


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
          inp.dispatchEvent(new Event('change', {bubbles:true}));
          return true;
        }""",
        player,
    )
    page.wait_for_timeout(1400)
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    return [
        c
        for c in cards
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]


def add_leg(page, leg: dict) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    hits = hidden_search(page, leg["player"])
    if not hits:
        hits = hidden_search(page, leg["player"].split()[-1])
    if not hits:
        print(f"NO HITS {leg['player']}", flush=True)
        return False
    start = next(
        (c for c in hits if yard_ok(leg["prop"], str(c.get("prop_type") or ""))),
        hits[0],
    )
    already = any(
        yard_ok(leg["prop"], str(c.get("prop_type") or ""))
        and str(c.get("pick_type") or "").lower() == "goblin"
        and P._line_key(c.get("line")) == P._line_key(leg["line"])
        for c in hits
    )
    if not already:
        P.cycle_card_to_pick_type(
            frame,
            start.get("more_btn"),
            player=leg["player"],
            prop=start.get("prop_type") or leg["prop"],
            want_pick="goblin",
            want_line=leg["line"],
            require_line=True,
            more_index=start.get("more_index"),
        )
        page.wait_for_timeout(400)
        hidden_search(page, leg["player"])
    r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, leg)
    if not r.ok:
        print(r.loud_fail(), flush=True)
        return False
    return True


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [
            pg
            for pg in ctx.pages
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
        ]
        # Prefer a tab that is NOT the ready F6-1 (has Raynor)
        page = None
        f61 = None
        for pg in boards:
            try:
                slip = (
                    P.read_slip(
                        P.find_prizepicks_frame(pg), n_legs=6, ticket_type="flex"
                    )
                    or {}
                )
                raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
            except Exception:
                raw = ""
            if "Jaylen Raynor" in raw and "Sam Leavitt" in raw:
                f61 = pg
                print("KEEP F6-1", flush=True)
            elif page is None:
                page = pg
        if page is None:
            page = ctx.new_page()
            page.goto(
                "https://app.prizepicks.com/board?league_id=15",
                wait_until="domcontentloaded",
                timeout=60_000,
            )
            page.wait_for_timeout(1200)
        print("======== F5-2 ========", flush=True)
        page.bring_to_front()
        page.set_viewport_size({"width": 1400, "height": 900})
        P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "flex")
        for leg in F5["legs"]:
            print(f"  + {leg['player']} {leg['prop']} {leg['line']}", flush=True)
            if not add_leg(page, leg):
                try:
                    P.clear_slip(P.find_prizepicks_frame(page))
                except Exception:
                    pass
                print("FAILED", flush=True)
                return 2
            time.sleep(0.15)
        try:
            P.find_prizepicks_frame(page).get_by_text(
                "Flex Play", exact=True
            ).first.click(timeout=2500)
        except Exception:
            pass
        slip = (
            P.read_slip(
                P.find_prizepicks_frame(page), n_legs=5, ticket_type="flex"
            )
            or {}
        )
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        n = int(slip.get("n_selected") or 0)
        m = re.search(r"5\s*correct\s*pays\s*([\d.]+)\s*x", raw, re.I)
        px = float(m.group(1)) if m else None
        ready = n == 5
        print(f"[F5-2] ready={ready} n={n} N-correct={px}", flush=True)
        print(raw[:450], flush=True)
        # merge into report
        prev = {}
        if OUT.exists():
            prev = json.loads(OUT.read_text(encoding="utf-8"))
        prev["F5-2"] = {
            "ready": ready,
            "n": n,
            "px": px,
            "legs": [(l["player"], l["prop"], l["line"]) for l in F5["legs"]],
            "raw": raw[:700],
        }
        OUT.write_text(json.dumps(prev, indent=2, default=str), encoding="utf-8")
        if ready:
            page.bring_to_front()
        print("No submit.", flush=True)
        return 0 if ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
