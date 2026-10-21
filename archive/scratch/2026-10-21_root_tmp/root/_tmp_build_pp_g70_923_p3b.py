#!/usr/bin/env python3
"""Build 9/23 G70 Power 3s without Walks 0.5 (that mix paid 1.3x). No submit."""
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
SHOT = _REPO / "data" / "reports"

TICKETS = [
    {
        "id": "T1",
        "name": "Lugo HA / Henderson Ks / Sale Ks",
        "legs": [
            {"sport": "MLB", "player": "Seth Lugo", "search": "Seth Lugo", "prop_type": "Hits Allowed", "prop_click": "hits allowed", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Logan Henderson", "search": "Logan Henderson", "prop_type": "Pitcher Strikeouts", "prop_click": "ks", "direction": "OVER", "line": 3.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Chris Sale", "search": "Chris Sale", "prop_type": "Pitcher Strikeouts", "prop_click": "ks", "direction": "OVER", "line": 4.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "T2",
        "name": "Kirby HA / Gausman Ks / Malonga Reb",
        "legs": [
            {"sport": "MLB", "player": "George Kirby", "search": "George Kirby", "prop_type": "Hits Allowed", "prop_click": "hits allowed", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Kevin Gausman", "search": "Kevin Gausman", "prop_type": "Pitcher Strikeouts", "prop_click": "ks", "direction": "OVER", "line": 3.5, "pick_type": "goblin"},
            {"sport": "WNBA", "player": "Dominique Malonga", "search": "Malonga", "prop_type": "Rebounds", "prop_click": "rebounds", "direction": "OVER", "line": 5.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "T3",
        "name": "Urena HA / Cole Ks / Bueckers PA",
        "legs": [
            {"sport": "MLB", "player": "Walbert Urena", "search": "Urena", "prop_type": "Hits Allowed", "prop_click": "hits allowed", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Gerrit Cole", "search": "Gerrit Cole", "prop_type": "Pitcher Strikeouts", "prop_click": "ks", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "WNBA", "player": "Paige Bueckers", "search": "Bueckers", "prop_type": "Pts+Asts", "prop_click": "pts+asts", "direction": "OVER", "line": 19.5, "pick_type": "goblin"},
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


def tight_click(page, player: str, prop_click: str, line: float, direction: str) -> str:
    action = "Less" if direction.upper() in ("UNDER", "LESS") else "More"
    last = player.split()[-1].replace(".", "")
    return page.evaluate(
        """({last, prop, line, action}) => {
          const want = String(line);
          const pn = prop.toLowerCase();
          const btns = [...document.querySelectorAll('button')].filter(
            b => ((b.innerText||'').trim()===action)
          );
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<10 && root;i++) {
              const t = (root.innerText||'');
              const tl = t.toLowerCase();
              const moreCount = (t.match(/\\bMore\\b/g)||[]).length;
              if (moreCount !== 1 || t.length > 420) { root = root.parentElement; continue; }
              if (!tl.includes(last.toLowerCase()) || !t.includes(want)) { root = root.parentElement; continue; }
              if (tl.includes('+') && !pn.includes('pts')) { root = root.parentElement; continue; }
              let ok = false;
              if (pn.includes('hits')) ok = tl.includes('hits allowed');
              else if (pn === 'ks' || pn.includes('strike'))
                ok = (tl.includes('ks') || tl.includes('strikeout')) && !tl.includes('walk');
              else if (pn.includes('rebound')) ok = tl.includes('rebound') && !tl.includes('pts') && !tl.includes('ast');
              else if (pn.includes('pts+ast') || pn.includes('pts+asts'))
                ok = (tl.includes('pts+ast') || tl.includes('points+ast') || tl.includes('pts + ast'));
              if (!ok) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,140);
            }
          }
          return 'no';
        }""",
        {"last": last, "prop": prop_click, "line": line, "action": action},
    )


def n_correct_power3(raw: str) -> float | None:
    import re
    m = re.search(
        r"Power Play.*?3 correct pays\s*(\d+(?:\.\d+)?)\s*X",
        raw,
        flags=re.I | re.S,
    )
    if m:
        return float(m.group(1))
    return None


def add_one(frame, page, leg) -> bool:
    switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1600)
    if switched is not None:
        frame = switched
    else:
        P.navigate_board_for_sport(page, leg["sport"], settle_ms=2000)
        frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, "")
    page.wait_for_timeout(200)
    react_search(page, leg["search"])
    page.wait_for_timeout(1600)
    frame = P.find_prizepicks_frame(page)
    ok = P.add_leg(frame, page, leg, strict_lines=True, require_line=True)
    msg = tight_click(page, leg["player"], leg["prop_click"], float(leg["line"]), leg["direction"])
    print("  tight_click", msg)
    if str(msg).startswith("ok"):
        ok = True
    try:
        P.set_ticket_type(P.find_prizepicks_frame(page), "power")
    except Exception:
        pass
    return bool(ok)


def find_board(browser):
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                return ctx, pg
    ctx = browser.contexts[0]
    pg = ctx.new_page()
    pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
    return ctx, pg


def build(page, ticket: dict) -> dict:
    tid = ticket["id"]
    legs = ticket["legs"]
    print(f"\n======== {tid} {ticket['name']} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    page.wait_for_timeout(400)
    fails = []
    for leg in legs:
        print(f"\n--- {leg['sport']} {leg['player']} {leg['prop_type']} {leg['line']} ---")
        if not add_one(frame, page, leg):
            fails.append(leg["player"])
            print("  FAIL")
        else:
            print("  OK")
        frame = P.find_prizepicks_frame(page)
        time.sleep(0.4)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
    except Exception:
        pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="power") or {}
    raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power3(raw)
    shot = SHOT / f"pp_g70_{tid.lower()}_nobb_2026-09-23.png"
    try:
        page.screenshot(path=str(shot), full_page=False)
    except Exception:
        shot = None
    print(f"[{tid} FINAL] n={n} fails={fails} 3-correct={px}x")
    print(raw[:700])
    return {
        "id": tid,
        "ready": n >= 3 and not fails,
        "n": n,
        "fails": fails,
        "payout_3": px,
        "raw": raw[:900],
        "shot": str(shot) if shot else "",
        "meets_2x": px is not None and px >= 2.0,
    }


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx, first = find_board(browser)
        pages = [first]
        for _ in range(2):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            pages.append(pg)
        for page, ticket in zip(pages, TICKETS):
            try:
                results.append(build(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append({"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)], "payout_3": None, "meets_2x": False})
        try:
            pages[0].bring_to_front()
        except Exception:
            pass
    print("\n======== SUMMARY ========")
    for r in results:
        print(
            f"  {r['id']} n={r.get('n')} 3-correct={r.get('payout_3')} "
            f"2x={'YES' if r.get('meets_2x') else 'NO'} fails={r.get('fails')}"
        )
    print("No submit.")
    return 0 if all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
