#!/usr/bin/env python3
"""Gated 9/20 rebuild v3: keep gates + L5>=4 + click_leg (no wrong-prop clicks)."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"

TICKETS = [
    {
        "id": "A",
        "mode": "power",
        "legs": [
            {"sport": "TENNIS", "player": "Anna Bondar", "search": "Bondar", "prop": "Total Games", "line": 17.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "TENNIS", "player": "Moyuka Uchijima", "search": "Uchijima", "prop": "Total Games Won", "line": 5.5, "pick": "goblin", "dir": "OVER"},
            # live board missed 16.5 earlier; 17.5 keep L5=4
            {"sport": "TENNIS", "player": "Maria Sakkari", "search": "Sakkari", "prop": "Total Games", "line": 17.5, "pick": "goblin", "dir": "OVER"},
        ],
    },
    {
        "id": "B",
        "mode": "power",
        "legs": [
            {"sport": "TENNIS", "player": "Janice Tjen", "search": "Tjen", "prop": "Total Games", "line": 17.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "TENNIS", "player": "Polina Kudermetova", "search": "Kudermetova", "prop": "Total Games Won", "line": 5.5, "pick": "goblin", "dir": "OVER"},
        ],
    },
    {
        "id": "C",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Rashod Bateman", "search": "Bateman", "prop": "Rec Yards", "line": 35.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Roman Wilson", "search": "Roman Wilson", "prop": "Rec Yards", "line": 33.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Tyler Allgeier", "search": "Allgeier", "prop": "Longest Rec", "line": 6.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Keenan Allen", "search": "Keenan Allen", "prop": "Longest Rec", "line": 13.5, "pick": "standard", "dir": "UNDER"},
        ],
    },
    {
        "id": "D",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Justice Hill", "search": "Justice Hill", "prop": "Rec Yards", "line": 13.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Xavier Legette", "search": "Legette", "prop": "Rec Yards", "line": 16.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Rico Dowdle", "search": "Dowdle", "prop": "Rush Yards", "line": 32.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Woody Marks", "search": "Woody Marks", "prop": "Rush Yards", "line": 28.5, "pick": "standard", "dir": "OVER"},
        ],
    },
]


def react_search(page, q: str) -> None:
    page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return;
      const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
      inp.dispatchEvent(new Event('change', {bubbles:true}));
      inp.focus();
    }""",
        q,
    )


def prop_match(card_prop: str, want: str) -> bool:
    cp, wp = P._norm(card_prop or ""), P._norm(want or "")
    if wp == "total games":
        return cp == "total games"
    if wp in ("games won", "total games won"):
        return cp in ("games won", "total games won")
    if wp == "rush yards":
        return cp == "rush yards"
    if wp in ("rec yards", "receiving yards"):
        return cp in ("rec yards", "receiving yards")
    if wp in ("longest reception", "longest rec", "long reception"):
        return cp in ("longest reception", "longest rec", "long reception")
    if wp in ("receptions", "recs"):
        return cp in ("receptions", "recs")
    return wp == cp


def cycle_face_exact(frame, card, want_pick: str, want_line: float):
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen = set()
    for i in range(16):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        key = (pt, line)
        print(f"  face[{i}] {pt} {line}")
        if key in seen and i > 0:
            break
        seen.add(key)
        try:
            lf = float(line) if line is not None else None
        except (TypeError, ValueError):
            lf = None
        if pt == want_pick and lf is not None and abs(lf - want_line) < 0.01 and 0 < lf < 200:
            card = dict(card)
            card["line"] = lf
            card["pick_type"] = want_pick
            return lf, card
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.4)
        # re-bind more btn after swap
        btn = card.get("more_btn")
    return None, card


def slip_raw(page) -> str:
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=6, ticket_type="power") or {}
    return slip.get("raw_slip_section") or slip.get("raw_text") or ""


def slip_has_leg(raw: str, player: str, prop: str, line: float) -> bool:
    low = raw.lower().replace(".", "")
    if player.split()[-1].replace(".", "").lower() not in low:
        return False
    if str(line) not in raw and f"{line:g}" not in raw:
        # allow 6.5 vs 6.50
        if f"{float(line)}" not in raw:
            return False
    pn = P._norm(prop)
    rn = P._norm(raw)
    if "longest" in pn:
        return "longest rec" in rn
    if "rec yards" in pn or "receiving" in pn:
        return "rec yards" in rn or "receiving yards" in rn
    if "rush" in pn:
        return "rush yards" in rn
    if "games won" in pn:
        return "games won" in rn
    if pn == "total games":
        return "total games" in rn and "games won" not in raw.lower().split(player.split()[-1].lower())[-1][:80]
    return True


def add_leg(page, leg) -> bool:
    frame = P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1200)
    if switched is not None:
        frame = switched
    P.dismiss_modal(frame, page)
    react_search(page, "")
    page.wait_for_timeout(400)
    react_search(page, leg["search"])
    page.wait_for_timeout(2000)
    cards = P.get_all_cards(frame)
    needle = leg["player"].split()[-1].replace(".", "").lower()
    hits = []
    for c in cards:
        pname = str(c.get("player") or "")
        if needle not in pname.lower().replace(".", ""):
            continue
        if "+" in pname:
            continue
        if not prop_match(str(c.get("prop_type") or ""), leg["prop"]):
            continue
        hits.append(c)
    print(f"  candidates={len(hits)}")
    if not hits:
        # retry search once
        react_search(page, "")
        page.wait_for_timeout(300)
        react_search(page, leg["search"])
        page.wait_for_timeout(2200)
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        for c in cards:
            pname = str(c.get("player") or "")
            if needle not in pname.lower().replace(".", ""):
                continue
            if "+" in pname:
                continue
            if prop_match(str(c.get("prop_type") or ""), leg["prop"]):
                hits.append(c)
        print(f"  retry candidates={len(hits)}")
    if not hits:
        return False
    card = hits[0]
    print(f"  using {card.get('player')} | {card.get('prop_type')} | {card.get('line')}")
    line, card = cycle_face_exact(frame, card, leg["pick"], float(leg["line"]))
    if line is None:
        print("  exact face miss")
        return False
    print(f"  final {leg['pick']} {line}")
    # Re-bind card after face cycle so More/Less targets the live tile.
    frame = P.find_prizepicks_frame(page)
    live = None
    for c in P.get_all_cards(frame):
        pname = str(c.get("player") or "")
        if needle not in pname.lower().replace(".", ""):
            continue
        if "+" in pname:
            continue
        if not prop_match(str(c.get("prop_type") or ""), leg["prop"]):
            continue
        try:
            cl = float(c.get("line"))
        except (TypeError, ValueError):
            continue
        if abs(cl - float(line)) < 0.01:
            live = c
            break
    if live is None:
        live = card
        live["line"] = line
        live["pick_type"] = leg["pick"]
    else:
        live["pick_type"] = leg["pick"]
    ok = P.click_leg(frame, live, leg["dir"])
    print(f"  click_leg {ok} via {live.get('prop_type')} {live.get('line')}")
    if not ok:
        return False
    time.sleep(0.6)
    raw = slip_raw(page)
    if not slip_has_leg(raw, leg["player"], leg["prop"], line):
        print(f"  slip mismatch for {leg['player']} {leg['prop']} {line}")
        print(f"  slip snippet: {raw[:500]}")
        return False
    return True


def build(page, ticket) -> dict:
    tid, mode = ticket["id"], ticket["mode"]
    print(f"\n======== {tid} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, ticket["legs"][0]["sport"], settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    fails = []
    for leg in ticket["legs"]:
        print(f"\n--- {leg['player']} {leg['prop']} {leg['line']} ---")
        ok = add_leg(page, leg)
        print("  result", ok)
        if not ok:
            fails.append(leg["player"])
        try:
            P.set_ticket_type(P.find_prizepicks_frame(page), mode)
        except Exception:
            pass
    try:
        label = "Power Play" if mode == "power" else "Flex Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(timeout=1500)
    except Exception:
        pass
    raw = slip_raw(page)
    n = int((P.read_slip(P.find_prizepicks_frame(page), n_legs=len(ticket["legs"]), ticket_type=mode) or {}).get("n_selected") or 0)
    # reject contaminated props on NFL tickets
    bad = False
    if tid in ("C", "D"):
        if "35.5Rush" in raw.replace(" ", "") or "30.5Rec Yards" in raw.replace("\n", ""):
            # Allgeier rush / Keenan rec yards contamination patterns
            if "Allgeier" in raw and "35.5" in raw and "Rush" in raw:
                print("  CONTAMINATED Allgeier rush")
                bad = True
            if "Keenan" in raw and "30.5" in raw and "Rec Yards" in raw:
                print("  CONTAMINATED Keenan rec yards")
                bad = True
    print(f"[{tid} FINAL] {n}/{len(ticket['legs'])} bad={bad}\n{raw[:1400]}")
    ready = n >= len(ticket["legs"]) and not fails and not bad
    return {"id": tid, "ready": ready, "n": n, "fails": fails}


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "")]
        pages = []
        if boards:
            pages.append(boards[0])
            try:
                boards[0].goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
                boards[0].wait_for_timeout(1200)
            except Exception as e:
                print("goto warn", e)
        while len(pages) < len(TICKETS):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            pages.append(pg)

        for page, ticket in zip(pages[: len(TICKETS)], TICKETS):
            try:
                results.append(build(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append({"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]})
        pages[0].bring_to_front()

    print("\n======== FINAL SUMMARY ========")
    for r in results:
        print(f"  {r['id']}: {'READY' if r['ready'] else 'INCOMPLETE'} n={r['n']} fails={r['fails'] or '-'}")
    print(
        "Gates: tennis keep + L5>=4; NFL Standard keep only "
        "(NFL_GOBLIN_TICKETS_ENABLED=False) + wider L5>=4 + D."
    )
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
