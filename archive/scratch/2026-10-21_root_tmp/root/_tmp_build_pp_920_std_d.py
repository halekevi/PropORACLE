#!/usr/bin/env python3
"""Build ticket D: Soccer Standard Flex 4 (Saves keep Gate70). Leave A/B/C alone."""
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

# Gate70 soccer Saves keep: L5>=4 + own D. All Standard.
TICKET = {
    "id": "D",
    "mode": "flex",
    "legs": [
        {
            "sport": "SOCCER",
            "player": "Edoardo Corvi",
            "search": "Corvi",
            "prop": "Goalie Saves",
            "line": 2.5,
            "pick": "standard",
            "dir": "OVER",
        },
        {
            "sport": "SOCCER",
            "player": "Yehvann Diouf",
            "search": "Diouf",
            "prop": "Goalie Saves",
            "line": 3.0,
            "pick": "standard",
            "dir": "UNDER",
        },
        {
            "sport": "SOCCER",
            "player": "Jean Butez",
            "search": "Butez",
            "prop": "Goalie Saves",
            "line": 2.5,
            "pick": "standard",
            "dir": "UNDER",
        },
        {
            "sport": "SOCCER",
            "player": "Jeffrey de Lange",
            "search": "de Lange",
            "prop": "Goalie Saves",
            "line": 5.0,
            "pick": "standard",
            "dir": "UNDER",
        },
    ],
}


def react_search(page, q: str) -> None:
    page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return;
      const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
    }""",
        q,
    )


def prop_match(card_prop: str, want: str) -> bool:
    cp, wp = P._norm(card_prop or ""), P._norm(want or "")
    if "saves" in wp:
        return "saves" in cp and "combo" not in cp
    return wp == cp


def cycle_exact(frame, card, want_pick: str, want_line: float):
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
        if pt == want_pick and lf is not None and abs(lf - want_line) < 0.01:
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
              if (!tl.includes('saves')) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,140);
            }
          }
          return 'no';
        }""",
        {"last": last, "prop": prop, "line": line, "action": action},
    )


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "")]
        # Prefer 4th tab so A/B/C stay put; else new page
        if len(boards) >= 4:
            page = boards[3]
        else:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1200)
        page.bring_to_front()
        P.navigate_board_for_sport(page, "SOCCER", settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "flex")
        fails = []
        for leg in TICKET["legs"]:
            print(f"\n--- {leg['player']} {leg['dir']} {leg['line']} ---")
            frame = P.find_prizepicks_frame(page)
            P.switch_sport_tab(frame, page, "SOCCER", settle_ms=1000)
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            react_search(page, "")
            page.wait_for_timeout(250)
            react_search(page, leg["search"])
            page.wait_for_timeout(1600)
            frame = P.find_prizepicks_frame(page)
            needle = leg["player"].split()[-1].lower()
            hits = [
                c
                for c in P.get_all_cards(frame)
                if needle in str(c.get("player") or "").lower()
                and "+" not in str(c.get("player") or "")
                and prop_match(str(c.get("prop_type") or ""), leg["prop"])
            ]
            print(f"  candidates={len(hits)}")
            if not hits:
                fails.append(leg["player"])
                continue
            card = hits[0]
            line = cycle_exact(frame, card, leg["pick"], float(leg["line"]))
            if line is None:
                print("  exact face miss")
                fails.append(leg["player"])
                continue
            msg = tight_click(page, leg["player"], leg["prop"], line, leg["dir"])
            print(f"  click {msg}")
            if not str(msg).startswith("ok"):
                fails.append(leg["player"])
                continue
            time.sleep(0.5)
            try:
                P.set_ticket_type(P.find_prizepicks_frame(page), "flex")
            except Exception:
                pass
        try:
            P.find_prizepicks_frame(page).get_by_text("Flex Play", exact=True).first.click(
                timeout=1500
            )
        except Exception:
            pass
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=4, ticket_type="flex") or {}
        raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
        n = int(slip.get("n_selected") or 0)
        print(f"\n[D FINAL] {n}/4 fails={fails}\n{raw[:1100]}")
        ready = n >= 4 and not fails
        print("D READY Standard Flex4 soccer Saves" if ready else "D INCOMPLETE")
        return 0 if ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
