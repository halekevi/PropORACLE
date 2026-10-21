"""Redo CFB tickets: USC + ORE (2-team). Clear old slips. No submit."""
from __future__ import annotations
import sys, time
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, _norm
import utils.pp_manual_slip as M

M.PROP_ALIASES.update({
    "recs": ("recs", "receptions"),
    "receptions": ("recs", "receptions"),
    "passtds": ("passtds", "passingtds", "passingtd"),
    "receivingyards": ("receivingyards", "recyards", "recyds"),
    "recyards": ("receivingyards", "recyards", "recyds"),
})

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"

# Candidate pool: USC L5=4-ish + ORE L5=3 (planned lines; live may adjust)
USC_CANDS = [
    {"player": "Jayden Maiava", "search": "Maiava", "prop_type": "Pass TDs", "chip": "Pass TDs", "line": 1.5, "team": "USC"},
    {"player": "Kayden Dixon-Wyatt", "search": "Dixon-Wyatt", "prop_type": "Receptions", "chip": "Receptions", "line": 2.5, "team": "USC"},
    {"player": "Zacharyus Williams", "search": "Zacharyus", "prop_type": "Receptions", "chip": "Receptions", "line": 1.5, "team": "USC"},
    {"player": "Mark Bowman", "search": "Bowman", "prop_type": "Receptions", "chip": "Receptions", "line": 2.5, "team": "USC"},
]
ORE_CANDS = [
    {"player": "Evan Stewart", "search": "Evan Stewart", "prop_type": "Receptions", "chip": "Receptions", "line": 2.5, "team": "ORE"},
    {"player": "Evan Stewart", "search": "Evan Stewart", "prop_type": "Receiving Yards", "chip": "Receiving Yards", "line": 29.5, "team": "ORE"},
    {"player": "Dakorien Moore", "search": "Dakorien", "prop_type": "Receiving Yards", "chip": "Receiving Yards", "line": 24.5, "team": "ORE"},
    {"player": "Dierre Hill Jr.", "search": "Dierre Hill", "prop_type": "Receptions", "chip": "Receptions", "line": 1.5, "team": "ORE"},
]


def find_live_goblin(frame, player: str, prop_hint: str):
    cards = P.get_all_cards(frame)
    last = player.split()[-1].replace(".", "").lower()
    hits = []
    for c in cards:
        pn = str(c.get("player") or "").lower()
        if last not in pn and _norm(player) not in _norm(c.get("player")):
            continue
        if str(c.get("pick_type") or "").lower() != "goblin":
            continue
        hits.append(c)
    if not hits:
        return None
    # prefer matching prop family
    ph = _norm(prop_hint)
    scored = []
    for c in hits:
        cp = _norm(c.get("prop_type"))
        score = 0
        if ph in cp or cp in ph:
            score += 10
        if "td" in ph and "td" in cp:
            score += 10
        if "rec" in ph and "rec" in cp:
            score += 5
        if "yard" in ph and "yard" in cp:
            score += 5
        scored.append((score, float(c.get("line") or 999), c))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return scored[0][2]


def probe(page, cand):
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    try:
        P._switch_board_filter(frame, page, cand["chip"])
    except Exception:
        pass
    page.wait_for_timeout(500)
    react_search(page, "")
    page.wait_for_timeout(60)
    react_search(page, cand["search"])
    page.wait_for_timeout(1200)
    frame = P.find_prizepicks_frame(page)
    card = find_live_goblin(frame, cand["player"], cand["prop_type"])
    if card is None:
        # try cycle from any face
        cards = P.get_all_cards(frame)
        anyc = None
        last = cand["player"].split()[-1].replace(".", "").lower()
        for c in cards:
            if last in str(c.get("player") or "").lower():
                anyc = c
                break
        if anyc and anyc.get("more_btn"):
            P.cycle_card_to_pick_type(
                frame, anyc.get("more_btn"),
                player=cand["player"],
                prop=anyc.get("prop_type") or cand["prop_type"],
                want_pick="goblin",
                want_line=None,
                require_line=False,
                more_index=anyc.get("more_index"),
            )
            page.wait_for_timeout(450)
            frame = P.find_prizepicks_frame(page)
            card = find_live_goblin(frame, cand["player"], cand["prop_type"])
    if card is None:
        return None
    return {
        "sport": "CFB",
        "player": cand["player"],
        "search": cand["search"],
        "team": cand["team"],
        "prop_type": str(card.get("prop_type") or cand["prop_type"]),
        "prop": str(card.get("prop_type") or cand["prop_type"]),
        "direction": "OVER",
        "line": float(card.get("line")),
        "pick_type": "goblin",
        "chip": cand["chip"],
    }


def add_leg(page, leg):
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    chip = leg.get("chip") or ("Pass TDs" if "td" in leg["prop_type"].lower() else (
        "Receiving Yards" if "yard" in leg["prop_type"].lower() else "Receptions"))
    try:
        P._switch_board_filter(frame, page, chip)
    except Exception:
        pass
    page.wait_for_timeout(550)
    react_search(page, "")
    page.wait_for_timeout(50)
    react_search(page, leg["search"])
    page.wait_for_timeout(1300)
    frame = P.find_prizepicks_frame(page)
    r = add_cycled_leg(P, frame, page, leg)
    if r.ok:
        return True
    # live fallback click
    card = find_live_goblin(frame, leg["player"], leg["prop_type"])
    if card is None:
        print(f"  DROPPED no card {leg['player']}", flush=True)
        return False
    live = {**leg, "prop_type": str(card.get("prop_type")), "prop": str(card.get("prop_type")), "line": float(card.get("line"))}
    print(f"  fallback {live['player']} {live['prop_type']} O{live['line']}", flush=True)
    r2 = add_cycled_leg(P, frame, page, live)
    if r2.ok:
        leg["line"] = live["line"]
        leg["prop_type"] = live["prop_type"]
        return True
    if card.get("more_btn"):
        try:
            card["more_btn"].click(timeout=1500, no_wait_after=True)
        except Exception:
            card["more_btn"].click(force=True, timeout=2500, no_wait_after=True)
        page.wait_for_timeout(600)
        leg["line"] = live["line"]
        leg["prop_type"] = live["prop_type"]
        return True
    print(f"  DROPPED {leg['player']}: {r.loud_fail()}", flush=True)
    return False


def build(page, legs, tid):
    teams = {l["team"] for l in legs}
    assert len(teams) >= 2, f"{tid} needs 2 teams, got {teams}"
    print(f"\n======== {tid} teams={sorted(teams)} ========", flush=True)
    for leg in legs:
        print(f"  plan {leg['team']} {leg['player']} {leg['prop_type']} O{leg['line']}", flush=True)
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    added = []
    for leg in legs:
        print(f"+ {leg['team']} {leg['player']} {leg['prop_type']} O{leg['line']}", flush=True)
        if not add_leg(page, leg):
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": len(added)}
        added.append(f"{leg['team']} {leg['player']} {leg['prop_type']} O{leg['line']}")
        time.sleep(0.25)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception:
        pass
    page.wait_for_timeout(500)
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    # team diversity checks
    has_usc = "USC" in raw
    has_ore = "ORE" in raw
    banner_bad = "two different teams" in raw.lower() or "at least two" in raw.lower()
    ready = n == len(legs) and has_usc and has_ore and not banner_bad
    print(f"[{tid}] ready={ready} n={n} N-correct={px} USC={has_usc} ORE={has_ore} banner_bad={banner_bad}", flush=True)
    print(raw[:450], flush=True)
    try:
        page.screenshot(path=str(_REPO / "data" / "reports" / f"pp_{tid.lower()}.png"), full_page=False)
    except Exception:
        pass
    return {"id": tid, "ready": ready, "n": n, "px": px, "legs": added, "fails": [], "raw": raw[:200]}


def main():
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=180_000)
        ctx = browser.contexts[0]
        boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")]
        if not boards:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1500)
            boards.append(pg)
        page0 = boards[0]
        page0.bring_to_front()
        P.navigate_board_for_sport(page0, "CFB", settle_ms=2200)

        print("=== PROBE LIVE GOBLINS ===", flush=True)
        usc_live, ore_live = [], []
        for c in USC_CANDS:
            hit = probe(page0, c)
            if hit:
                print(f"  USC LIVE {hit['player']} {hit['prop_type']} O{hit['line']}", flush=True)
                usc_live.append(hit)
            else:
                print(f"  USC MISS {c['player']} {c['prop_type']}", flush=True)
        for c in ORE_CANDS:
            hit = probe(page0, c)
            if hit:
                print(f"  ORE LIVE {hit['player']} {hit['prop_type']} O{hit['line']}", flush=True)
                # unique player prefer first
                if not any(x["player"] == hit["player"] for x in ore_live):
                    ore_live.append(hit)
            else:
                print(f"  ORE MISS {c['player']} {c['prop_type']}", flush=True)

        if not usc_live or not ore_live:
            print(f"Cannot build 2-team: usc={len(usc_live)} ore={len(ore_live)}")
            return 2

        # unique USC players
        usc_uniq = []
        seen = set()
        for x in usc_live:
            if x["player"] in seen:
                continue
            seen.add(x["player"])
            usc_uniq.append(x)

        tickets = []
        # Ticket 1: 2 USC + 1 ORE
        if len(usc_uniq) >= 2 and ore_live:
            tickets.append(("CFB-2T-A", [usc_uniq[0], ore_live[0], usc_uniq[1]]))
        # Ticket 2: 1 USC + 1 ORE + 1 USC (different set) or 2 ORE + 1 USC
        if len(usc_uniq) >= 3 and len(ore_live) >= 1:
            ore2 = ore_live[1] if len(ore_live) > 1 else ore_live[0]
            # ensure different ORE player if possible when same as ticket1
            tickets.append(("CFB-2T-B", [usc_uniq[2], ore2, usc_uniq[0] if usc_uniq[0]["player"] != usc_uniq[2]["player"] else usc_uniq[1]]))
        elif len(usc_uniq) >= 1 and len(ore_live) >= 2:
            tickets.append(("CFB-2T-B", [ore_live[0], usc_uniq[0], ore_live[1]]))

        # dedupe players within a ticket
        cleaned = []
        for tid, legs in tickets:
            seen_p = set()
            uniq_legs = []
            for leg in legs:
                if leg["player"] in seen_p:
                    continue
                seen_p.add(leg["player"])
                uniq_legs.append(dict(leg))
            if len(uniq_legs) < 2:
                continue
            if len({l["team"] for l in uniq_legs}) < 2:
                continue
            # need at least 2 legs; prefer 3
            if len(uniq_legs) == 2:
                # try add another USC/ORE not used
                for pool in (usc_uniq, ore_live):
                    for cand in pool:
                        if cand["player"] not in seen_p and cand["team"] in {l["team"] for l in uniq_legs} or True:
                            if cand["player"] not in seen_p:
                                uniq_legs.append(dict(cand))
                                seen_p.add(cand["player"])
                                break
                    if len(uniq_legs) >= 3:
                        break
            cleaned.append((tid, uniq_legs[:3]))

        if not cleaned:
            print("No valid 2-team ticket plans")
            return 2

        print(f"\nBuilding {len(cleaned)} tickets...", flush=True)
        while len(boards) < len(cleaned):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            boards.append(pg)

        results = []
        for page, (tid, legs) in zip(boards, cleaned):
            try:
                results.append(build(page, legs, tid))
            except Exception as e:
                print(f"{tid} EXC {e}", flush=True)
                results.append({"id": tid, "ready": False, "fails": [str(e)]})

        for page, r in zip(boards, results):
            if r.get("ready"):
                page.bring_to_front()
                print(f"Brought {r['id']} to front", flush=True)
                break

        print("\n======== SUMMARY ========", flush=True)
        for r in results:
            print(f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} N-correct={r.get('px')} legs={r.get('legs')} fails={r.get('fails')}", flush=True)
        print("No submit. Each slip has USC + ORE.", flush=True)
        return 0 if any(r.get("ready") for r in results) else 2


raise SystemExit(main())
