"""Build CFB tickets: Popular search first (narrows), then cycle Goblin face."""
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
OUT = _REPO / "data" / "reports" / "cfb_sat_tabs_ready_2026-09-26.json"

TICKETS = [
    {
        "id": "P3-1",
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
    {
        "id": "P3-2",
        "mode": "power",
        "legs": [
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


def popular_search(frame, page, player: str) -> list[dict]:
    """Search on Popular so PP actually filters tiles."""
    # Prefer Popular chip; ignore failure
    try:
        P._switch_board_filter(frame, page, "Popular")
    except Exception:
        pass
    page.wait_for_timeout(400)
    frame = P.find_prizepicks_frame(page)
    # clear + type via keyboard on page-level search (PP header)
    typed = False
    for root in (page, frame):
        for sel in [
            "input[placeholder*='Search']",
            "input[type='search']",
            "input[aria-label*='Search']",
            "input[aria-label='search']",
        ]:
            box = root.locator(sel).first
            try:
                if box.count() == 0:
                    continue
                box.click(force=True, timeout=1500)
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.wait_for_timeout(120)
                page.keyboard.type(player, delay=35)
                page.wait_for_timeout(1400)
                typed = True
                break
            except Exception:
                continue
        if typed:
            break
    if not typed:
        # fallback evaluate
        page.evaluate(
            """(q) => {
              const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search'], input[type='search']");
              if (!inp) return false;
              const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
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
    hits = [
        c
        for c in cards
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]
    print(f"[SEARCH] Popular {player!r} total={len(cards)} hits={len(hits)}", flush=True)
    return hits


def add_leg_popular(page, leg: dict) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    hits = popular_search(frame, page, leg["player"])
    if not hits:
        print(f"NO HITS {leg['player']}", flush=True)
        return False
    # Prefer a yard card matching prop among hits for cycle start
    start = None
    for c in hits:
        if yard_ok(leg["prop"], str(c.get("prop_type") or "")):
            start = c
            break
    if start is None:
        start = hits[0]
    # Scroll more button into view via index
    idx = start.get("more_index")
    if idx is not None:
        try:
            frame.locator("body").evaluate(
                """(i) => {
                  const btns = Array.from(document.querySelectorAll('button')).filter(b =>
                    ((b.innerText||'').includes('More') || (b.getAttribute('aria-label')||'').toLowerCase().includes('more'))
                  );
                  const el = btns[i];
                  if (el) el.scrollIntoView({block:'center'});
                  return !!el;
                }""",
                int(idx),
            )
            page.wait_for_timeout(300)
        except Exception:
            pass
    # Cycle to exact goblin line
    frame = P.find_prizepicks_frame(page)
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
    page.wait_for_timeout(450)
    frame = P.find_prizepicks_frame(page)
    # Re-search to refresh cards, then add_cycled_leg
    popular_search(frame, page, leg["player"])
    r = add_cycled_leg(P, frame, page, leg)
    if not r.ok:
        print(r.loud_fail(), flush=True)
        return False
    return True


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for leg in legs:
        print(f"  {leg['player']} {leg['prop']} {leg['line']}", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        ok = add_leg_popular(page, leg)
        if not ok:
            frame = P.find_prizepicks_frame(page)
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
    print(raw[:450], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_cfb_{tid.lower()}_ready_2026-09-26.png"
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
        "raw": raw[:600],
    }


def main() -> int:
    results = []
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [
            pg
            for pg in ctx.pages
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
        ]
        if not boards:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1500)
            boards = [pg]
        while len(boards) < len(TICKETS):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(900)
            boards.append(pg)
            print(f"opened tab {len(boards)}", flush=True)
        for page, ticket in zip(boards, TICKETS):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]}
                )
        for page, r in zip(boards, results):
            if r.get("ready"):
                page.bring_to_front()
                print(f"front {r['id']}", flush=True)
                break
    OUT.write_text(
        json.dumps({"tickets": TICKETS, "results": results}, indent=2, default=str),
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
    print("No submit.", flush=True)
    return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
