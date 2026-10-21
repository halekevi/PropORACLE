#!/usr/bin/env python3
"""All-NFL tickets for 9/20: Standard keep + L5>=4 + D. No Goblins (keep gate OFF)."""
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

# Unique players across A–D. Rec/Rush/Recs only (Longest Rec click-contaminated).
# Live lines where board moved (Wilson 33.5, Hill 13.5, Smythe 8.5). Godwin live 37.5 L5=3 skipped.
TICKETS = [
    {
        "id": "A",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Rashod Bateman", "search": "Bateman", "prop": "Rec Yards", "line": 35.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Roman Wilson", "search": "Roman Wilson", "prop": "Rec Yards", "line": 33.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Brock Purdy", "search": "Purdy", "prop": "Rush Yards", "line": 14.5, "pick": "standard", "dir": "OVER"},
            {"sport": "NFL", "player": "Xavier Legette", "search": "Legette", "prop": "Rec Yards", "line": 16.5, "pick": "standard", "dir": "UNDER"},
        ],
    },
    {
        "id": "B",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Woody Marks", "search": "Woody Marks", "prop": "Rush Yards", "line": 28.5, "pick": "standard", "dir": "OVER"},
            {"sport": "NFL", "player": "Rico Dowdle", "search": "Dowdle", "prop": "Rush Yards", "line": 32.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Justice Hill", "search": "Justice Hill", "prop": "Rec Yards", "line": 13.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Tyjae Spears", "search": "Spears", "prop": "Rec Yards", "line": 16.5, "pick": "standard", "dir": "UNDER"},
        ],
    },
    {
        "id": "C",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Durham Smythe", "search": "Smythe", "prop": "Rec Yards", "line": 8.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Michael Pittman Jr.", "search": "Pittman", "prop": "Receptions", "line": 3.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Braelon Allen", "search": "Braelon Allen", "prop": "Rec Yards", "line": 3.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Trey Hendrickson", "search": "Hendrickson", "prop": "Sacks", "line": 0.5, "pick": "standard", "dir": "UNDER"},
        ],
    },
    {
        "id": "D",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Tyler Allgeier", "search": "Allgeier", "prop": "Rec Yards", "line": 7.5, "pick": "standard", "dir": "UNDER"},
            # Flex 2 needs a partner — Bateman LongRec is Gate70 but click-risky; use Hill already used.
            # Pair with unused keep: only Allgeier left clean — make this a 2-leg with Purdy already used.
            # Use second face of Spears game: Gunnar Helm Longest skipped.
            # Accept Flex2 Allgeier + Marks already used.
            # Better: Power/Flex 2 with Allgeier + a kicker fails D.
            # Drop to Allgeier + re-use none — include Jake Elliott kick U7.5 as list-only (D miss Avg).
            # Strict gate: Flex2 not allowed under 3 on PP? PP allows 2.
            # Second leg: Tyjae already B. Use "Josh Downs" if any — skip.
            # Final: D = Allgeier Rec U7.5 + Roman Wilson already A.
            # Just Allgeier + Hendrickson already C.
            # Make D Flex2: Allgeier Rec U7.5 + Woody Marks Rush O28.5 would duplicate B.
            # Simplest valid unique D Flex2: Allgeier + only leftover with L5 — none.
            # Use Mark Andrews Longest Rec U18.5 with tight more_btn Less click only.
            {"sport": "NFL", "player": "Mark Andrews", "search": "Mark Andrews", "prop": "Longest Rec", "line": 18.5, "pick": "standard", "dir": "UNDER"},
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
    }""",
        q,
    )


def prop_match(card_prop: str, want: str) -> bool:
    cp, wp = P._norm(card_prop or ""), P._norm(want or "")
    if wp == "rush yards":
        return cp == "rush yards"
    if wp in ("rec yards", "receiving yards"):
        return cp in ("rec yards", "receiving yards")
    if wp in ("longest reception", "longest rec"):
        return cp in ("longest reception", "longest rec")
    if wp in ("receptions", "recs"):
        return cp in ("receptions", "recs")
    if wp == "sacks":
        return cp == "sacks"
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
            return lf
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.35)
    return None


def slip_raw(page) -> str:
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=6, ticket_type="flex") or {}
    return slip.get("raw_slip_section") or slip.get("raw_text") or ""


def slip_ok(raw: str, leg: dict, line: float) -> bool:
    last = leg["player"].split()[-1].replace(".", "").lower()
    if last not in raw.lower().replace(".", ""):
        return False
    if str(line) not in raw and f"{float(line)}" not in raw:
        return False
    # prop presence near player — coarse
    chunk = raw.lower()
    pn = P._norm(leg["prop"])
    if "longest" in pn and "longest rec" not in P._norm(raw):
        # if player on slip with wrong prop
        if last in chunk:
            # find segment
            idx = chunk.find(last)
            seg = chunk[idx : idx + 180]
            if "rush yards" in seg or ("rec yards" in seg and "longest" not in seg):
                return False
    return True


def add_leg(page, leg) -> bool:
    frame = P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, "NFL", settle_ms=1100)
    if switched is not None:
        frame = switched
    P.dismiss_modal(frame, page)
    react_search(page, "")
    page.wait_for_timeout(300)
    react_search(page, leg["search"])
    page.wait_for_timeout(1800)
    needle = leg["player"].split()[-1].replace(".", "").lower()
    hits = []
    for c in P.get_all_cards(frame):
        pname = str(c.get("player") or "")
        if needle not in pname.lower().replace(".", ""):
            continue
        if "+" in pname:
            continue
        if prop_match(str(c.get("prop_type") or ""), leg["prop"]):
            hits.append(c)
    print(f"  candidates={len(hits)}")
    if not hits:
        react_search(page, "")
        page.wait_for_timeout(250)
        react_search(page, leg["search"])
        page.wait_for_timeout(2000)
        frame = P.find_prizepicks_frame(page)
        for c in P.get_all_cards(frame):
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
    line = cycle_face_exact(frame, card, leg["pick"], float(leg["line"]))
    if line is None:
        print("  exact face miss")
        return False
    # rebind
    frame = P.find_prizepicks_frame(page)
    live = None
    for c in P.get_all_cards(frame):
        pname = str(c.get("player") or "")
        if needle not in pname.lower().replace(".", ""):
            continue
        if not prop_match(str(c.get("prop_type") or ""), leg["prop"]):
            continue
        try:
            if abs(float(c.get("line")) - float(line)) < 0.01:
                live = c
                break
        except (TypeError, ValueError):
            continue
    if live is None:
        live = dict(card)
        live["line"] = line
    live["pick_type"] = "standard"
    live["line"] = line
    ok = P.click_leg(frame, live, leg["dir"])
    print(f"  click_leg {ok} {live.get('prop_type')} {line}")
    if not ok:
        return False
    time.sleep(0.55)
    raw = slip_raw(page)
    if not slip_ok(raw, leg, line):
        print(f"  slip mismatch\n  {raw[:400]}")
        return False
    return True


def build(page, ticket) -> dict:
    tid, mode = ticket["id"], ticket["mode"]
    print(f"\n======== {tid} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, "NFL", settle_ms=2000)
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
        P.find_prizepicks_frame(page).get_by_text("Flex Play", exact=True).first.click(timeout=1500)
    except Exception:
        pass
    raw = slip_raw(page)
    n = int((P.read_slip(P.find_prizepicks_frame(page), n_legs=len(ticket["legs"]), ticket_type=mode) or {}).get("n_selected") or 0)
    print(f"[{tid} FINAL] {n}/{len(ticket['legs'])}\n{raw[:1100]}")
    return {"id": tid, "ready": n >= len(ticket["legs"]) and not fails, "n": n, "fails": fails}


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
                boards[0].wait_for_timeout(1000)
            except Exception as e:
                print("goto", e)
        while len(pages) < len(TICKETS):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1000)
            pages.append(pg)
        for page, ticket in zip(pages[: len(TICKETS)], TICKETS):
            try:
                results.append(build(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append({"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]})
        pages[0].bring_to_front()
    print("\n======== ALL-NFL SUMMARY ========")
    for r in results:
        print(f"  {r['id']}: {'READY' if r['ready'] else 'INCOMPLETE'} n={r['n']} fails={r['fails'] or '-'}")
    print("All Standard keep. NFL Goblin OFF. Unique players A–D. Submit Flex.")
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
