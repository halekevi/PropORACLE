#!/usr/bin/env python3
"""Finish B/C/D only. E ready. A hopefully intact. No submit."""
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
        "id": "B",
        "mode": "power",
        "legs": [
            {"sport": "TENNIS", "player": "Janice Tjen", "search": "Tjen",
             "prop_type": "Total Games", "direction": "OVER", "line": 17.5, "pick_type": "goblin"},
            {"sport": "TENNIS", "player": "Polina Kudermetova", "search": "Kudermetova",
             "prop_type": "Total Games Won", "direction": "OVER", "line": 5.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "C",
        "mode": "power",
        "legs": [
            {"sport": "NFL", "player": "Woody Marks", "search": "Woody Marks",
             "prop_type": "Rush Yards", "direction": "OVER", "line": 25.5, "pick_type": "goblin"},
            {"sport": "NFL", "player": "Mack Hollins", "search": "Mack Hollins",
             "prop_type": "Rec Yards", "direction": "OVER", "line": 31.5, "pick_type": "goblin"},
            {"sport": "NFL", "player": "Devaughn Vele", "search": "Devaughn Vele",
             "prop_type": "Rec Yards", "direction": "OVER", "line": 36.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "D",
        "mode": "power",
        "legs": [
            {"sport": "NFL", "player": "Jacory Croskey-Merritt", "search": "Croskey-Merritt",
             "prop_type": "Rush Yards", "direction": "OVER", "line": 49.5, "pick_type": "goblin"},
            {"sport": "NFL", "player": "Dallas Goedert", "search": "Dallas Goedert",
             "prop_type": "Rec Yards", "direction": "OVER", "line": 32.5, "pick_type": "goblin"},
            {"sport": "NFL", "player": "Dontayvion Wicks", "search": "Dontayvion Wicks",
             "prop_type": "Rec Yards", "direction": "OVER", "line": 29.5, "pick_type": "goblin"},
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


def clear_search(page) -> None:
    react_search(page, "")
    page.wait_for_timeout(400)


def prop_match(card_prop: str, want: str) -> bool:
    cp = P._norm(card_prop or "")
    wp = P._norm(want or "")
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


def name_match(card_player: str, leg: dict) -> bool:
    p = (card_player or "").lower()
    full = leg["player"].lower()
    last = leg["player"].split()[-1].lower()
    search = leg["search"].lower()
    if full in p or search in p:
        return True
    # require last name and (first token or search)
    if last in p and (leg["player"].split()[0].lower() in p or search.split()[0].lower() in p):
        return True
    return False


def cycle_to_face(frame, card, *, want_pick: str, want_line: float):
    btn = card.get("more_btn")
    idx = card.get("more_index")
    seen = set()
    faces = []
    for i in range(16):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        key = (pt, line)
        print(f"  face[{i}] {pt} {line}")
        if key in seen and i > 0:
            break
        seen.add(key)
        if pt == want_pick:
            faces.append(st)
            try:
                if abs(float(line) - float(want_line)) < 0.01:
                    return st, btn, idx
            except Exception:
                pass
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.4)
    if not faces:
        return None, btn, idx
    def dist(s):
        try:
            return abs(float(s.get("line")) - float(want_line))
        except Exception:
            return 999
    best = min(faces, key=dist)
    print(f"  nearest {want_pick} -> {best.get('line')}")
    seen2 = set()
    for i in range(16):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "").lower()
        line = st.get("line")
        key = (pt, line)
        try:
            if pt == want_pick and abs(float(line) - float(best.get("line"))) < 0.01:
                return st, btn, idx
        except Exception:
            pass
        if key in seen2 and i > 0:
            break
        seen2.add(key)
        P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop="")
        time.sleep(0.35)
    return best, btn, idx


def click_more_less(page, name: str, prop: str, line: float, direction: str) -> str:
    action = "Less" if direction.upper() in ("UNDER", "LESS") else "More"
    return page.evaluate(
        """({name, prop, line, action}) => {
          const want = String(line);
          const pn = prop.toLowerCase();
          const btns = Array.from(document.querySelectorAll('button')).filter(b =>
            ((b.innerText||'').trim() === action));
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<10 && root;i++) {
              const t = (root.innerText || '');
              if (!t.includes(name.split(' ').pop()) || !t.includes(want) || t.length > 500) {
                root = root.parentElement; continue;
              }
              const tl = t.toLowerCase();
              let propOk = false;
              if (pn.includes('total games') && !pn.includes('won'))
                propOk = tl.includes('total games') && !tl.includes('won') && !tl.includes('1st');
              else if (pn.includes('games won'))
                propOk = tl.includes('games won') && !tl.includes('1st');
              else if (pn.includes('rush'))
                propOk = tl.includes('rush yards') && !tl.includes('combo') && !tl.includes('+');
              else if (pn.includes('rec'))
                propOk = (tl.includes('rec yards') || tl.includes('receiving')) && !tl.includes('combo');
              else if (pn.includes('pass'))
                propOk = tl.includes('pass yards');
              else propOk = tl.includes(pn);
              if (!propOk) { root = root.parentElement; continue; }
              // name: prefer full last+first presence
              const nameL = name.toLowerCase();
              if (!tl.includes(nameL.split(' ').pop())) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,160);
            }
          }
          return 'no';
        }""",
        {"name": name, "prop": prop, "line": line, "action": action},
    )


def add_one(page, frame, leg) -> bool:
    clear_search(page)
    switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1200)
    if switched is not None:
        frame = switched
    P.dismiss_modal(frame, page)
    react_search(page, leg["search"])
    page.wait_for_timeout(1600)

    cards = P.get_all_cards(frame)
    hits = [c for c in cards if name_match(str(c.get("player") or ""), leg)
            and prop_match(str(c.get("prop_type") or ""), leg["prop_type"])]
    # drop combo cards
    hits = [c for c in hits if "+" not in str(c.get("player") or "") and "combo" not in P._norm(c.get("prop_type"))]
    print(f"  candidates={len(hits)}")
    if not hits:
        return False
    card = hits[0]
    print(f"  using {card.get('player')} | {card.get('prop_type')} | {card.get('line')} | {card.get('pick_type')}")

    st, btn, idx = cycle_to_face(frame, card, want_pick=leg["pick_type"], want_line=float(leg["line"]))
    if st is None:
        return False
    final_line = float(st.get("line"))
    print(f"  final {st.get('pickType')} {final_line}")

    # Click More/Less by exact name+line in DOM (avoids wrong-player clicks)
    msg = click_more_less(page, leg["player"], leg["prop_type"], final_line, leg["direction"])
    print(f"  click: {msg}")
    if not str(msg).startswith("ok"):
        # fallback library click
        card2 = dict(card)
        card2["more_btn"] = btn
        card2["more_index"] = idx
        card2["line"] = final_line
        card2["pick_type"] = leg["pick_type"]
        ok = P.click_leg(frame, card2, leg["direction"])
        print(f"  fallback click_leg={ok}")
        return bool(ok)
    time.sleep(0.5)
    return True


def slip_has(page, needles: list[str]) -> bool:
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=6, ticket_type="power") or {}
    raw = ((slip.get("raw_slip_section") or slip.get("raw_text") or "")).lower()
    return all(n.lower() in raw for n in needles)


def build(page, ticket):
    tid = ticket["id"]
    mode = ticket["mode"]
    print(f"\n======== {tid} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, ticket["legs"][0]["sport"], settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)

    fails = []
    for leg in ticket["legs"]:
        print(f"\n--- {leg['player']} {leg['prop_type']} {leg['line']} ---")
        frame = P.find_prizepicks_frame(page)
        ok = add_one(page, frame, leg)
        # verify this player landed
        landed = slip_has(page, [leg["player"].split()[-1]])
        print(f"  result={'OK' if ok else 'FAIL'} landed={landed}")
        if not ok or not landed:
            fails.append(leg["player"])
        try:
            P.set_ticket_type(P.find_prizepicks_frame(page), mode)
        except Exception:
            pass
        page.wait_for_timeout(400)

    try:
        label = "Power Play" if mode == "power" else "Flex Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(timeout=1500)
    except Exception:
        pass
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=len(ticket["legs"]), ticket_type=mode) or {}
    raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
    n = int(slip.get("n_selected") or 0)
    print(f"\n[{tid} FINAL] {n}/{len(ticket['legs'])}\n{raw[:1400]}")
    # reject if wrong players appear
    bad = []
    if tid == "D" and "henry" in raw.lower():
        bad.append("Henry contamination")
    ready = n >= len(ticket["legs"]) and not fails and not bad
    return {"id": tid, "ready": ready, "n": n, "fails": fails + bad, "raw": raw}


def main():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=30_000)
        ctx = browser.contexts[0]
        pages = []
        for t in TICKETS:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1500)
            pages.append(pg)
        for page, ticket in zip(pages, TICKETS):
            try:
                results.append(build(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append({"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)], "raw": ""})
        pages[0].bring_to_front()
    print("\n======== SUMMARY ========")
    for r in results:
        print(f"  {r['id']}: {'READY' if r['ready'] else 'INCOMPLETE'} n={r['n']} fails={r['fails'] or '-'}")
    print("E should still be ready from prior tab. A from first build. Submit manually.")
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
