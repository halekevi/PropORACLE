"""Build CFB USC-ORE L5=4 Goblin tickets on separate PP tabs. No submit."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, prop_ok, _norm

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"

# Planned tickets from L5=4 list (deepest Goblin where multiple lines; unique players)
# Skip Pass TDs (ticket-stack fade). Skip Rush Attempts Goblin (list fade).
TICKETS = [
  {
    "id": "CFB-P3-1",
    "mode": "power",
    "legs": [
      {"sport": "CFB", "player": "Jayden Maiava", "prop_type": "Pass Yards", "direction": "OVER", "line": 189.5, "pick_type": "goblin"},
      {"sport": "CFB", "player": "Kayden Dixon-Wyatt", "prop_type": "Receiving Yards", "direction": "OVER", "line": 29.5, "pick_type": "goblin"},
      {"sport": "CFB", "player": "King Miller", "prop_type": "Rush Yards", "direction": "OVER", "line": 39.5, "pick_type": "goblin"},
    ],
  },
  {
    "id": "CFB-P3-2",
    "mode": "power",
    "legs": [
      {"sport": "CFB", "player": "Waymond Jordan", "prop_type": "Rush Yards", "direction": "OVER", "line": 14.5, "pick_type": "goblin"},
      {"sport": "CFB", "player": "Zacharyus Williams", "prop_type": "Receiving Yards", "direction": "OVER", "line": 9.5, "pick_type": "goblin"},
      {"sport": "CFB", "player": "Mark Bowman", "prop_type": "Receiving Yards", "direction": "OVER", "line": 14.5, "pick_type": "goblin"},
    ],
  },
  {
    "id": "CFB-F4-1",
    "mode": "flex",
    "legs": [
      {"sport": "CFB", "player": "Jayden Maiava", "prop_type": "Pass Yards", "direction": "OVER", "line": 199.5, "pick_type": "goblin"},
      {"sport": "CFB", "player": "Kayden Dixon-Wyatt", "prop_type": "Receptions", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
      {"sport": "CFB", "player": "Waymond Jordan", "prop_type": "Rush Yards", "direction": "OVER", "line": 19.5, "pick_type": "goblin"},
      {"sport": "CFB", "player": "Zacharyus Williams", "prop_type": "Receiving Yards", "direction": "OVER", "line": 14.5, "pick_type": "goblin"},
    ],
  },
]


def find_live_goblin(frame, page, player: str, prop: str, prefer_line: float | None = None):
    """Return best matching live goblin card line for player/prop, or None."""
    cards = P.get_all_cards(frame)
    hits = []
    pn = _norm(player)
    for c in cards:
        cn = _norm(c.get("player"))
        if pn not in cn and cn not in pn:
            continue
        if str(c.get("pick_type") or "").lower() != "goblin":
            continue
        if not prop_ok(prop, str(c.get("prop_type") or "")):
            # CFB aliases
            cp = _norm(c.get("prop_type"))
            wp = _norm(prop)
            if wp in ("passyards", "passingyards") and cp in ("passyards", "passingyards", "passyds"):
                pass
            elif wp in ("rushyards", "rushingyards") and cp in ("rushyards", "rushingyards", "rushyds"):
                pass
            elif wp in ("receivingyards", "recyards") and ("rec" in cp and "yard" in cp):
                pass
            elif wp == "receptions" and cp in ("receptions", "rec"):
                pass
            else:
                continue
        hits.append(c)
    if not hits:
        return None
    if prefer_line is not None:
        for c in hits:
            if P._line_key(c.get("line")) == P._line_key(prefer_line):
                return c
    # deepest goblin = lowest OVER line
    hits.sort(key=lambda c: float(c.get("line") or 999))
    return hits[0]


def land_cfb(page):
    P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
    return P.find_prizepicks_frame(page)


def build_one(page, ticket):
    tid, mode = ticket["id"], ticket["mode"]
    legs = [{**l, "search": l["player"]} for l in ticket["legs"]]
    print(f"\n======== {tid} {mode} ========", flush=True)
    page.bring_to_front()
    frame = land_cfb(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)

    resolved = []
    for i, leg in enumerate(legs):
        frame = P.find_prizepicks_frame(page)
        if i > 0:
            switched = P.switch_sport_tab(frame, page, "CFB", settle_ms=1200)
            if switched is not None:
                frame = switched
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(700)
        react_search(page, "")
        page.wait_for_timeout(80)
        react_search(page, leg["search"])
        page.wait_for_timeout(1400)
        frame = P.find_prizepicks_frame(page)
        card = find_live_goblin(frame, page, leg["player"], leg["prop_type"], leg["line"])
        if card is None:
            # try without prefer line
            card = find_live_goblin(frame, page, leg["player"], leg["prop_type"], None)
        if card is None:
            print(f"NO LIVE GOBLIN {leg['player']} {leg['prop_type']} want {leg['line']}", flush=True)
            try:
                P.clear_slip(frame)
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": 0}
        live_line = float(card.get("line"))
        live_prop = str(card.get("prop_type") or leg["prop_type"])
        print(f"  + {leg['player']} {live_prop} O{live_line} (planned {leg['line']})", flush=True)
        use = {**leg, "prop_type": live_prop, "prop": live_prop, "line": live_line, "pick_type": "goblin"}
        r = add_cycled_leg(P, frame, page, use)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": len(resolved)}
        resolved.append(f"{leg['player']} {live_prop} O{live_line}")
        time.sleep(0.3)

    try:
        label = "Flex Play" if mode == "flex" else "Power Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(timeout=2000)
    except Exception:
        pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode) or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
    print(raw[:350], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_{tid.lower()}_2026-09-26.png"
    try:
        page.screenshot(path=str(shot), full_page=False)
    except Exception:
        pass
    return {"id": tid, "ready": ready, "n": n, "px": px, "legs": resolved, "fails": []}


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
        ctx = browser.contexts[0]
        boards = []
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                boards.append(pg)
        if not boards:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1500)
            boards.append(pg)
        while len(boards) < len(TICKETS):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            boards.append(pg)
            print(f"opened tab {len(boards)}", flush=True)

        for page, ticket in zip(boards, TICKETS):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append({"id": ticket["id"], "ready": False, "fails": [str(e)]})

        for page, r in zip(boards, results):
            if r.get("ready"):
                page.bring_to_front()
                print(f"\nBrought {r['id']} to front", flush=True)
                break

    print("\n======== SUMMARY ========", flush=True)
    for r in results:
        print(f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} N-correct={r.get('px')} legs={r.get('legs')} fails={r.get('fails')}", flush=True)
    print("L5=4 CFB (not L5=5). No submit.", flush=True)
    return 0 if any(r.get("ready") for r in results) else 2


raise SystemExit(main())
