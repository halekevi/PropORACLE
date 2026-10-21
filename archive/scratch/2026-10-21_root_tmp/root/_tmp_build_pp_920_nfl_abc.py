#!/usr/bin/env python3
"""Build all-NFL Flex A/B/C on PrizePicks (Standard keep + L5>=4 + D)."""
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
        "mode": "flex",
        "legs": [
            {"player": "Rashod Bateman", "search": "Bateman", "prop": "Rec Yards", "line": 35.5, "dir": "UNDER"},
            {"player": "Roman Wilson", "search": "Roman Wilson", "prop": "Rec Yards", "line": 33.5, "dir": "UNDER"},
            {"player": "Brock Purdy", "search": "Purdy", "prop": "Rush Yards", "line": 14.5, "dir": "OVER"},
            {"player": "Xavier Legette", "search": "Legette", "prop": "Rec Yards", "line": 16.5, "dir": "UNDER"},
        ],
    },
    {
        "id": "B",
        "mode": "flex",
        "legs": [
            {"player": "Woody Marks", "search": "Woody Marks", "prop": "Rush Yards", "line": 28.5, "dir": "OVER"},
            {"player": "Rico Dowdle", "search": "Dowdle", "prop": "Rush Yards", "line": 32.5, "dir": "UNDER"},
            {"player": "Justice Hill", "search": "Justice Hill", "prop": "Rec Yards", "line": 13.5, "dir": "UNDER"},
            {"player": "Tyler Allgeier", "search": "Allgeier", "prop": "Rec Yards", "line": 7.5, "dir": "UNDER"},
        ],
    },
    {
        "id": "C",
        "mode": "flex",
        "legs": [
            {"player": "Durham Smythe", "search": "Smythe", "prop": "Rec Yards", "line": 8.5, "dir": "UNDER"},
            {"player": "Trey Hendrickson", "search": "Hendrickson", "prop": "Sacks", "line": 0.5, "dir": "UNDER"},
            {"player": "Braelon Allen", "search": "Braelon Allen", "prop": "Rec Yards", "line": 3.5, "dir": "UNDER"},
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
    if wp == "sacks":
        return cp == "sacks"
    return wp == cp


def cycle_exact(frame, card, want_line: float):
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen = set()
    for i in range(14):
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
        if pt == "standard" and lf is not None and abs(lf - want_line) < 0.01 and 0 < lf < 200:
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
              if (tl.includes('+') || tl.includes('combo')) { root = root.parentElement; continue; }
              let propOk = false;
              if (pn.includes('rush')) propOk = tl.includes('rush yards') && !tl.includes('rec');
              else if (pn.includes('rec yards') || pn.includes('receiving'))
                propOk = (tl.includes('rec yards') || tl.includes('receiving yards')) && !tl.includes('longest') && !tl.includes('rush');
              else if (pn.includes('sack')) propOk = tl.includes('sack');
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
    slip = P.read_slip(frame, n_legs=6, ticket_type="flex") or {}
    return slip.get("raw_slip_section") or slip.get("raw_text") or ""


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
    line = cycle_exact(frame, card, float(leg["line"]))
    if line is None:
        print("  exact face miss")
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
    idx = raw.lower().find(needle)
    seg = raw[idx : idx + 220]
    pn = leg["prop"].lower()
    if "rec yards" in pn and "Rush Yards" in seg and "Rec Yards" not in seg:
        print("  contaminated to rush")
        return False
    if str(line) not in seg:
        print(f"  wrong line seg={seg[:100]!r}")
        return False
    return True


def build(page, ticket) -> dict:
    tid = ticket["id"]
    print(f"\n======== {tid} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, "NFL", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "flex")
    fails = []
    for leg in ticket["legs"]:
        print(f"\n--- {leg['player']} {leg['prop']} {leg['line']} ---")
        ok = add_leg(page, leg)
        print("  result", ok)
        if not ok:
            fails.append(leg["player"])
        try:
            P.set_ticket_type(P.find_prizepicks_frame(page), "flex")
        except Exception:
            pass
    try:
        P.find_prizepicks_frame(page).get_by_text("Flex Play", exact=True).first.click(timeout=1500)
    except Exception:
        pass
    raw = slip_raw(page)
    n = int((P.read_slip(P.find_prizepicks_frame(page), n_legs=len(ticket["legs"]), ticket_type="flex") or {}).get("n_selected") or 0)
    print(f"[{tid} FINAL] {n}/{len(ticket['legs'])}\n{raw[:1000]}")
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
                results.append({"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]})
        pages[0].bring_to_front()
    print("\n======== BUILD SUMMARY ========")
    for r in results:
        print(f"  {r['id']}: {'READY' if r['ready'] else 'INCOMPLETE'} n={r['n']} fails={r['fails'] or '-'}")
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
