#!/usr/bin/env python3
"""Build D only: Bill / Goedert / Wicks. Guard against Henry. No submit."""
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

# Croskey first — cycle is flaky once 2 legs already sit on the slip
LEGS = [
    {"player": "Jacory Croskey-Merritt", "search": "Croskey-Merritt", "prop_type": "Rush Yards", "line": 49.5},
    {"player": "Dontayvion Wicks", "search": "Dontayvion Wicks", "prop_type": "Rec Yards", "line": 29.5},
    {"player": "Dallas Goedert", "search": "Dallas Goedert", "prop_type": "Rec Yards", "line": 32.5},
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


def slip_raw(page) -> str:
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=6, ticket_type="power") or {}
    return (slip.get("raw_slip_section") or slip.get("raw_text") or "")


def click_face(page, last: str, line: float, prop_key: str) -> str:
    return page.evaluate(
        """({last, line, propKey}) => {
          const want = String(line);
          const btns = Array.from(document.querySelectorAll('button')).filter(b =>
            ((b.innerText||'').trim() === 'More'));
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
              if (propKey === 'rec') propOk = tl.includes('rec yards') || tl.includes('receiving');
              if (propKey === 'rush') propOk = tl.includes('rush yards');
              if (!propOk) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,180);
            }
          }
          return 'no';
        }""",
        {"last": last, "line": line, "propKey": prop_key},
    )


def cycle_goblin(frame, card, want_line: float):
    btn = card.get("more_btn")
    idx = card.get("more_index")
    seen = set()
    goblins = []
    for i in range(14):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "").lower()
        line = st.get("line")
        key = (pt, line)
        print(f"  face[{i}] {pt} {line}")
        if key in seen and i > 0:
            break
        seen.add(key)
        if pt == "goblin" and line is not None:
            goblins.append(float(line))
            if abs(float(line) - want_line) < 0.01:
                return float(line), btn, idx
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.4)
    if not goblins:
        return None, btn, idx
    best = min(goblins, key=lambda x: abs(x - want_line))
    # re-cycle to best
    seen2 = set()
    for i in range(14):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "").lower()
        line = st.get("line")
        key = (pt, line)
        if pt == "goblin" and line is not None and abs(float(line) - best) < 0.01:
            return float(line), btn, idx
        if key in seen2 and i > 0:
            break
        seen2.add(key)
        P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop="")
        time.sleep(0.35)
    return best, btn, idx


def add_leg(page, leg) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.switch_sport_tab(frame, page, "NFL", settle_ms=1000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, "")
    page.wait_for_timeout(300)
    react_search(page, leg["search"])
    page.wait_for_timeout(1600)

    cards = P.get_all_cards(frame)
    want_prop = P._norm(leg["prop_type"])
    hits = []
    for c in cards:
        pname = str(c.get("player") or "")
        if leg["player"].split()[-1].lower() not in pname.lower():
            continue
        if "+" in pname or "combo" in P._norm(c.get("prop_type")):
            continue
        cp = P._norm(c.get("prop_type"))
        if want_prop == "rec yards" and cp not in ("rec yards", "receiving yards"):
            continue
        if want_prop == "rush yards" and cp != "rush yards":
            continue
        hits.append(c)
    print(f"  candidates={len(hits)}")
    if not hits:
        return False
    card = hits[0]
    print(f"  using {card.get('player')} {card.get('prop_type')} {card.get('line')}")
    line, btn, idx = cycle_goblin(frame, card, float(leg["line"]))
    if line is None:
        return False
    print(f"  goblin line {line}")
    last = leg["player"].split()[-1]
    prop_key = "rec" if "rec" in leg["prop_type"].lower() else "rush"
    msg = click_face(page, last, line, prop_key)
    print(f"  click {msg}")
    if not str(msg).startswith("ok"):
        return False
    time.sleep(0.6)
    raw = slip_raw(page)
    if "henry" in raw.lower():
        print("  BLOCK: Henry appeared — abort this add")
        return False
    if last.lower() not in raw.lower():
        print("  BLOCK: player not on slip")
        return False
    return True


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=30_000)
        page = browser.contexts[0].new_page()
        page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(2000)
        P.navigate_board_for_sport(page, "NFL", settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        P.set_ticket_type(frame, "power")

        for leg in LEGS:
            print(f"\n=== {leg['player']} ===")
            ok = add_leg(page, leg)
            print("result", ok)
            if not ok:
                # clear and stop
                print("FAILED — leaving slip for inspection")
                raw = slip_raw(page)
                print(raw[:1200])
                return 2
            try:
                P.set_ticket_type(P.find_prizepicks_frame(page), "power")
            except Exception:
                pass

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass
        raw = slip_raw(page)
        print("\n[D FINAL]\n", raw[:1600])
        ok = (
            "wicks" in raw.lower()
            and "goedert" in raw.lower()
            and ("croskey" in raw.lower() or "merritt" in raw.lower())
            and "henry" not in raw.lower()
        )
        print("READY" if ok else "INCOMPLETE")
        page.bring_to_front()
        return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
