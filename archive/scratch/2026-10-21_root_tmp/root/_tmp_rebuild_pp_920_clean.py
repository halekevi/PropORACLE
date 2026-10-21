#!/usr/bin/env python3
"""Close extra PP tabs; rebuild A–E cleanly (5 tabs). No submit."""
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

# Hard gate: directional L5 >= 4 (wider window OK). No L5<4 faces.
# SKIPPED: Croskey 49.5/43.5 (3/5), Goedert 32.5 (3/5), Wicks all (L5<4),
# Cousins U192.5 (3/5), Monangai 34.5 (3/5).
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
            # Marks 14.5 = deepest L5=5 Goblin; Hollins/Vele near faces still L5=5
            {"sport": "NFL", "player": "Woody Marks", "search": "Woody Marks", "prop": "Rush Yards", "line": 14.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "Mack Hollins", "search": "Mack Hollins", "prop": "Rec Yards", "line": 31.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "Devaughn Vele", "search": "Devaughn Vele", "prop": "Rec Yards", "line": 36.5, "pick": "goblin", "dir": "OVER"},
        ],
    },
    {
        "id": "D",
        "mode": "power",
        "legs": [
            {"sport": "NFL", "player": "Jacory Croskey-Merritt", "search": "Croskey-Merritt", "prop": "Rush Yards", "line": 24.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "Dallas Goedert", "search": "Dallas Goedert", "prop": "Rec Yards", "line": 17.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "C.J. Stroud", "search": "Stroud", "prop": "Pass Yards", "line": 159.5, "pick": "goblin", "dir": "OVER"},
        ],
    },
    {
        "id": "E",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Woody Marks", "search": "Woody Marks", "prop": "Rush Yards", "line": 14.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "Mack Hollins", "search": "Mack Hollins", "prop": "Rec Yards", "line": 31.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "Dallas Goedert", "search": "Dallas Goedert", "prop": "Rec Yards", "line": 17.5, "pick": "goblin", "dir": "OVER"},
            {"sport": "NFL", "player": "Rashod Bateman", "search": "Bateman", "prop": "Rec Yards", "line": 35.5, "pick": "standard", "dir": "UNDER"},
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
    if wp == "pass yards":
        return cp in ("pass yards", "passing yards")
    return wp == cp


def cycle_face(frame, card, want_pick: str, want_line: float):
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen, faces = set(), []
    for i in range(14):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        key = (pt, line)
        print(f"  face[{i}] {pt} {line}")
        if key in seen and i > 0:
            break
        seen.add(key)
        if pt == want_pick and line is not None:
            faces.append(float(line))
            if abs(float(line) - want_line) < 0.01:
                return float(line), btn, idx
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.4)
    if not faces:
        return None, btn, idx
    best = min(faces, key=lambda x: abs(x - want_line))
    seen = set()
    for i in range(14):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "").lower()
        line = st.get("line")
        if pt == want_pick and line is not None and abs(float(line) - best) < 0.01:
            return best, btn, idx
        key = (pt, line)
        if key in seen and i > 0:
            break
        seen.add(key)
        P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop="")
        time.sleep(0.35)
    return best, btn, idx


def click_more_less(page, player: str, prop: str, line: float, direction: str) -> str:
    action = "Less" if direction.upper() in ("UNDER", "LESS") else "More"
    last = player.split()[-1]
    return page.evaluate(
        """({last, prop, line, action, full}) => {
          const want = String(line);
          const pn = prop.toLowerCase();
          const btns = Array.from(document.querySelectorAll('button')).filter(b => ((b.innerText||'').trim()===action));
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<12 && root;i++) {
              const t = root.innerText || '';
              const tl = t.toLowerCase();
              if (!tl.includes(last.toLowerCase()) || !t.includes(want) || t.length > 550) {
                root = root.parentElement; continue;
              }
              if (tl.includes('henry') || tl.includes('combo') || tl.includes('+')) {
                root = root.parentElement; continue;
              }
              let propOk = false;
              if (pn === 'total games') propOk = tl.includes('total games') && !tl.includes('won') && !tl.includes('1st');
              else if (pn.includes('games won')) propOk = tl.includes('games won') && !tl.includes('1st');
              else if (pn.includes('rush')) propOk = tl.includes('rush yards');
              else if (pn.includes('rec')) propOk = tl.includes('rec yards') || tl.includes('receiving');
              else if (pn.includes('pass')) propOk = tl.includes('pass yards');
              if (!propOk) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,140);
            }
          }
          return 'no';
        }""",
        {"last": last, "prop": prop, "line": line, "action": action, "full": player},
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
    page.wait_for_timeout(250)
    react_search(page, leg["search"])
    page.wait_for_timeout(1500)
    cards = P.get_all_cards(frame)
    hits = []
    for c in cards:
        pname = str(c.get("player") or "")
        if leg["player"].split()[-1].lower() not in pname.lower():
            continue
        if "+" in pname:
            continue
        if not prop_match(str(c.get("prop_type") or ""), leg["prop"]):
            continue
        hits.append(c)
    print(f"  candidates={len(hits)}")
    if not hits:
        return False
    card = hits[0]
    print(f"  using {card.get('player')} | {card.get('prop_type')} | {card.get('line')}")
    line, btn, idx = cycle_face(frame, card, leg["pick"], float(leg["line"]))
    if line is None:
        return False
    print(f"  final {leg['pick']} {line}")
    msg = click_more_less(page, leg["player"], leg["prop"], line, leg["dir"])
    print(f"  click {msg}")
    if not str(msg).startswith("ok"):
        return False
    time.sleep(0.55)
    raw = slip_raw(page)
    if "henry" in raw.lower():
        print("  Henry contamination")
        return False
    if leg["player"].split()[-1].lower() not in raw.lower():
        print("  not on slip")
        return False
    return True


def build(page, ticket) -> dict:
    tid, mode = ticket["id"], ticket["mode"]
    print(f"\n======== {tid} ========")
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
    print(f"[{tid} FINAL] {n}/{len(ticket['legs'])}\n{raw[:1200]}")
    ready = n >= len(ticket["legs"]) and not fails and "henry" not in raw.lower()
    return {"id": tid, "ready": ready, "n": n, "fails": fails}


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        # Keep existing pages; only add what we need (never close the last tab).
        boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "")]
        print(f"existing prizepicks pages: {len(boards)}")
        pages = []
        # Reuse first board page for ticket A; create new for the rest
        if boards:
            pages.append(boards[0])
            try:
                boards[0].goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
                boards[0].wait_for_timeout(1500)
            except Exception as e:
                print("reuse goto warn", e)
        while len(pages) < len(TICKETS):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1500)
            pages.append(pg)
            print("opened", TICKETS[len(pages) - 1]["id"])

        for page, ticket in zip(pages, TICKETS):
            try:
                results.append(build(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append({"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]})

        # Bring A front
        pages[0].bring_to_front()

    print("\n======== FINAL SUMMARY ========")
    for r in results:
        print(f"  {r['id']}: {'READY' if r['ready'] else 'INCOMPLETE'} n={r['n']} fails={r['fails'] or '-'}")
    print("Submit manually in each tab. Do not submit C+E (Marks/Hollins) or D+E (Goedert) together.")
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
