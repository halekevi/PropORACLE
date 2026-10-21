#!/usr/bin/env python3
"""Build Sun 9/20 A–C from the hand-checked card only. No submit.

ticket_source=handcheck — refuses wider-window / algorithmic NFL READY.
NFL C = Marks Goblin Rush O25.5 + Hollins Rec O31.5 + Vele Rec O36.5 (n=1 hand-check).
D/E from the original file (hard Croskey/Goedert/Wicks, Cousins) are NOT rebuilt —
those lines were walked back on L5 / sit judgment after the first card.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.nfl_keep_gates import (
    assert_nfl_ready_source,
    assert_nfl_ticket_l5_source,
)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
TICKET_SOURCE = "handcheck"

# Loud: this build must never use padded cache L5 for eligibility.
assert_nfl_ticket_l5_source("handcheck")  # ok — not wider_window
assert_nfl_ready_source(TICKET_SOURCE)

TICKETS = [
    {
        "id": "A",
        "mode": "power",
        "legs": [
            {"sport": "TENNIS", "player": "Anna Bondar", "search": "Bondar", "prop": "Total Games", "line": 17.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "TENNIS", "player": "Moyuka Uchijima", "search": "Uchijima", "prop": "Total Games Won", "line": 5.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "TENNIS", "player": "Maria Sakkari", "search": "Sakkari", "prop": "Total Games", "line": 16.5, "pick": "goblin", "dir": "OVER"},
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
        "mode": "power",
        "legs": [
            {"sport": "NFL", "player": "Woody Marks", "search": "Woody Marks", "prop": "Rush Yards", "line": 25.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "Mack Hollins", "search": "Mack Hollins", "prop": "Rec Yards", "line": 31.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "Devaughn Vele", "search": "Devaughn Vele", "prop": "Rec Yards", "line": 36.5, "pick": "goblin", "dir": "OVER"},
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
    if wp == "total games":
        return cp == "total games"
    if wp in ("games won", "total games won"):
        return cp in ("games won", "total games won")
    if wp == "rush yards":
        return cp == "rush yards"
    if wp in ("rec yards", "receiving yards"):
        return cp in ("rec yards", "receiving yards")
    return wp == cp


def cycle_exact(frame, card, want_pick: str, want_line: float):
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


def tight_click(page, player: str, prop: str, line: float, direction: str) -> str:
    action = "Less" if direction.upper() in ("UNDER", "LESS") else "More"
    last = player.split()[-1].replace(".", "")
    return page.evaluate(
        """({last, prop, line, action}) => {
          const want = String(line);
          const pn = prop.toLowerCase();
          const btns = Array.from(document.querySelectorAll('button')).filter(
            b => ((b.innerText||'').trim()===action)
          );
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<8 && root;i++) {
              const t = (root.innerText||'');
              const tl = t.toLowerCase();
              const moreCount = (t.match(/\\bMore\\b/g)||[]).length;
              if (moreCount !== 1 || t.length > 420) { root = root.parentElement; continue; }
              if (!tl.includes(last.toLowerCase()) || !t.includes(want)) { root = root.parentElement; continue; }
              if (tl.includes('+') || tl.includes('combo') || tl.includes('henry')) {
                root = root.parentElement; continue;
              }
              let propOk = false;
              if (pn === 'total games') propOk = tl.includes('total games') && !tl.includes('won') && !tl.includes('1st');
              else if (pn.includes('games won')) propOk = tl.includes('games won') && !tl.includes('1st');
              else if (pn.includes('rush')) propOk = tl.includes('rush yards') && !tl.includes('rec');
              else if (pn.includes('rec')) propOk = (tl.includes('rec yards') || tl.includes('receiving')) && !tl.includes('longest') && !tl.includes('rush');
              if (!propOk) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,120);
            }
          }
          return 'no';
        }""",
        {"last": last, "prop": prop, "line": line, "action": action},
    )


def slip_raw(page) -> str:
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=6, ticket_type="power") or {}
    return slip.get("raw_slip_section") or slip.get("raw_text") or ""


def add_leg(page, leg) -> bool:
    frame = P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1100)
    if switched is not None:
        frame = switched
    P.dismiss_modal(frame, page)
    react_search(page, "")
    page.wait_for_timeout(300)
    react_search(page, leg["search"])
    page.wait_for_timeout(1800)
    needle = leg["player"].split()[-1].replace(".", "").lower()
    hits = []
    for attempt in range(2):
        frame = P.find_prizepicks_frame(page)
        hits = [
            c
            for c in P.get_all_cards(frame)
            if needle in str(c.get("player") or "").lower().replace(".", "")
            and "+" not in str(c.get("player") or "")
            and prop_match(str(c.get("prop_type") or ""), leg["prop"])
        ]
        print(f"  candidates={len(hits)}" + (" (retry)" if attempt else ""))
        if hits:
            break
        react_search(page, "")
        page.wait_for_timeout(250)
        react_search(page, leg["search"])
        page.wait_for_timeout(2000)
    if not hits:
        return False
    card = hits[0]
    print(f"  using {card.get('player')} | {card.get('prop_type')} | {card.get('line')}")
    line = cycle_exact(frame, card, leg["pick"], float(leg["line"]))
    if line is None:
        print("  exact face miss — refuse nearest fallback")
        return False
    msg = tight_click(page, leg["player"], leg["prop"], line, leg["dir"])
    print(f"  click {msg}")
    if not str(msg).startswith("ok"):
        return False
    time.sleep(0.55)
    raw = slip_raw(page)
    if needle not in raw.lower().replace(".", ""):
        print("  not on slip")
        return False
    if "henry" in raw.lower():
        print("  Henry contamination")
        return False
    if str(line) not in raw:
        print("  wrong line on slip")
        return False
    return True


def build(page, ticket) -> dict:
    tid, mode = ticket["id"], ticket["mode"]
    print(f"\n======== {tid} [{TICKET_SOURCE}] ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, ticket["legs"][0]["sport"], settle_ms=2000)
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
    print(f"[{tid} FINAL] {n}/{len(ticket['legs'])}\n{raw[:1000]}")
    ready = n >= len(ticket["legs"]) and not fails
    if ready:
        assert_nfl_ready_source(TICKET_SOURCE)  # NFL C included; tennis-only would also pass tag
        print(f"[{tid}] READY source={TICKET_SOURCE} (hand-checked list — not wider-window)")
    return {"id": tid, "ready": ready, "n": n, "fails": fails, "source": TICKET_SOURCE}


def main() -> int:
    # Prove the blocked door still raises (self-check, then continue).
    try:
        assert_nfl_ticket_l5_source("wider_window")
        print("FATAL: wider_window should have raised")
        return 3
    except Exception as e:
        print(f"guard ok: {type(e).__name__}: {e}")

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
        while len(pages) < 3:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1000)
            pages.append(pg)
        for page, ticket in zip(pages[:3], TICKETS):
            try:
                results.append(build(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append({"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)], "source": TICKET_SOURCE})
        pages[0].bring_to_front()

    print("\n======== HANDCHECK BUILD SUMMARY ========")
    for r in results:
        tag = "READY" if r["ready"] else "INCOMPLETE"
        print(f"  {r['id']}: {tag} n={r['n']} source={r.get('source')} fails={r['fails'] or '-'}")
    print("No submit. D/E not rebuilt (not on the cleared hand-check set).")
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
