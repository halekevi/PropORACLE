"""Fill empty PP tabs with more CFB L5=5 Goblin Power 3s. Do not touch Avery slip tab."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, prop_ok, _norm

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board?league_id=15"
OUT = _REPO / "data" / "reports" / "cfb_sat_more_tabs_2026-09-26.json"

# Live exact-line Goblins from last probe (exclude Avery — already on user's slip)
TICKETS = [
    {
        "id": "P3-A",
        "mode": "power",
        "legs": [
            {
                "sport": "CFB",
                "player": "Jaylen Raynor",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 99.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "ISU",
            },
            {
                "sport": "CFB",
                "player": "Katin Houser",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 124.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "ILL",
            },
            {
                "sport": "CFB",
                "player": "Cam Edwards",
                "prop": "Rush Yards",
                "prop_type": "Rush Yards",
                "line": 39.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "MSU",
            },
        ],
    },
    {
        "id": "P3-B",
        "mode": "power",
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
                "player": "Rodney Fields Jr.",
                "prop": "Rush Yards",
                "prop_type": "Rush Yards",
                "line": 34.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "KSU",
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
    },
    {
        "id": "P3-C",
        "mode": "power",
        "legs": [
            {
                "sport": "CFB",
                "player": "Arch Manning",
                "prop": "Pass Yards",
                "prop_type": "Pass Yards",
                "line": 174.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TEX",
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
                "player": "Terrance Carter Jr.",
                "prop": "Rec Yards",
                "prop_type": "Rec Yards",
                "line": 14.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TTU",
            },
        ],
    },
    {
        "id": "P3-D",
        "mode": "power",
        "legs": [
            {
                "sport": "CFB",
                "player": "Emmett Mosley V",
                "prop": "Rec Yards",
                "prop_type": "Rec Yards",
                "line": 14.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TEX",
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
                "player": "J'Koby Williams",
                "prop": "Rush Yards",
                "prop_type": "Rush Yards",
                "line": 49.5,
                "pick_type": "goblin",
                "direction": "OVER",
                "team": "TTU",
            },
        ],
    },
]


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
    page.wait_for_timeout(1500)
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
    frame = P.find_prizepicks_frame(page)
    r = add_cycled_leg(P, frame, page, leg)
    if not r.ok:
        print(r.loud_fail(), flush=True)
        return False
    return True


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} ========", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    # Soft navigate — don't wipe other tabs
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        print(f"  + {leg['player']} {leg['prop']} {leg['line']}", flush=True)
        if not add_leg(page, leg):
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "n": 0, "fails": [leg["player"]]}
        time.sleep(0.2)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
            timeout=2500
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power"
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    # Prefer slip N-correct 3-correct line (1.3x for goblin power3), not 1st place
    px = None
    if "3 correct pays" in raw.lower() or "3 Correct" in raw:
        import re

        m = re.search(r"3\s*correct\s*pays\s*([\d.]+)\s*x", raw, re.I)
        if m:
            px = float(m.group(1))
    if px is None:
        px = n_correct_power(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
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
        "raw": raw[:700],
    }


def main() -> int:
    results = []
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        keep = None
        empty = []
        for pg in ctx.pages:
            if "prizepicks.com" not in (pg.url or "") or "board" not in (pg.url or ""):
                continue
            try:
                slip = (
                    P.read_slip(
                        P.find_prizepicks_frame(pg), n_legs=3, ticket_type="power"
                    )
                    or {}
                )
                raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
            except Exception:
                raw = ""
            if "Avery Johnson" in raw and "Arch Manning" in raw:
                keep = pg
                print("KEEP Avery slip tab", flush=True)
            else:
                empty.append(pg)
        while len(empty) < len(TICKETS):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            empty.append(pg)
            print(f"opened tab {len(empty)}", flush=True)
        for page, ticket in zip(empty, TICKETS):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "fails": [str(e)], "n": 0}
                )
        # leave Avery tab alone; bring first ready new tab forward
        for page, r in zip(empty, results):
            if r.get("ready"):
                page.bring_to_front()
                break
        payload = {
            "kept_avery": keep is not None,
            "results": results,
            "tickets": TICKETS,
        }
        OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        print(f"  P3-2 Avery slip kept={keep is not None}", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
                flush=True,
            )
        print(f"wrote {OUT}", flush=True)
        print("No submit. Power 3 Goblin N-correct is 1.3x ($20->$26) — not 6x 1st place.", flush=True)
        return 0 if any(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
