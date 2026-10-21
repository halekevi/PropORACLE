#!/usr/bin/env python3
"""Repair all-NFL B+C; clear D. A already READY."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
import importlib.util

spec = importlib.util.spec_from_file_location("nfl", _REPO / "_tmp_rebuild_pp_920_nfl_only.py")
nfl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nfl)

CDP = "http://127.0.0.1:9222"

# Replace Spears (Rush contamination) / Allen Rush contamination / drop Pittman miss
TICKETS = [
    {
        "id": "B",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Woody Marks", "search": "Woody Marks", "prop": "Rush Yards", "line": 28.5, "pick": "standard", "dir": "OVER"},
            {"sport": "NFL", "player": "Rico Dowdle", "search": "Dowdle", "prop": "Rush Yards", "line": 32.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Justice Hill", "search": "Justice Hill", "prop": "Rec Yards", "line": 13.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Tyler Allgeier", "search": "Allgeier", "prop": "Rec Yards", "line": 7.5, "pick": "standard", "dir": "UNDER"},
        ],
    },
    {
        "id": "C",
        "mode": "flex",
        "legs": [
            {"sport": "NFL", "player": "Durham Smythe", "search": "Smythe", "prop": "Rec Yards", "line": 8.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Trey Hendrickson", "search": "Hendrickson", "prop": "Sacks", "line": 0.5, "pick": "standard", "dir": "UNDER"},
            {"sport": "NFL", "player": "Braelon Allen", "search": "Braelon Allen", "prop": "Rec Yards", "line": 3.5, "pick": "standard", "dir": "UNDER"},
        ],
    },
]


def tight_click(page, player: str, prop: str, line: float, direction: str) -> str:
    """Click More/Less only on a card root that contains player+prop+line and at most one More."""
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
              else if (pn.includes('reception') || pn==='recs')
                propOk = (tl.includes('receptions') || /\\brecs\\b/.test(tl)) && !tl.includes('longest');
              if (!propOk) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,120);
            }
          }
          return 'no';
        }""",
        {"last": last, "prop": prop, "line": line, "action": action},
    )


def add_leg_tight(page, leg) -> bool:
    frame = P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, "NFL", settle_ms=1100)
    if switched is not None:
        frame = switched
    P.dismiss_modal(frame, page)
    nfl.react_search(page, "")
    page.wait_for_timeout(300)
    nfl.react_search(page, leg["search"])
    page.wait_for_timeout(1800)
    needle = leg["player"].split()[-1].replace(".", "").lower()
    hits = [
        c
        for c in P.get_all_cards(frame)
        if needle in str(c.get("player") or "").lower().replace(".", "")
        and "+" not in str(c.get("player") or "")
        and nfl.prop_match(str(c.get("prop_type") or ""), leg["prop"])
    ]
    # sacks prop_match
    if not hits and "sack" in leg["prop"].lower():
        hits = [
            c
            for c in P.get_all_cards(frame)
            if needle in str(c.get("player") or "").lower()
            and "sack" in P._norm(str(c.get("prop_type") or ""))
        ]
    print(f"  candidates={len(hits)}")
    if not hits:
        return False
    card = hits[0]
    print(f"  using {card.get('player')} | {card.get('prop_type')} | {card.get('line')}")
    line = nfl.cycle_face_exact(frame, card, leg["pick"], float(leg["line"]))
    if line is None:
        print("  exact face miss")
        return False
    msg = tight_click(page, leg["player"], leg["prop"], line, leg["dir"])
    print(f"  tight_click {msg}")
    if not str(msg).startswith("ok"):
        return False
    time.sleep(0.55)
    raw = nfl.slip_raw(page)
    last = needle
    if last not in raw.lower().replace(".", ""):
        print("  not on slip")
        return False
    # reject wrong prop for this player
    idx = raw.lower().find(last)
    seg = raw[idx : idx + 200]
    pn = leg["prop"].lower()
    if "rec yards" in pn and "Rush Yards" in seg and "Rec Yards" not in seg:
        print("  contaminated rush")
        return False
    if "rush" in pn and "Rec Yards" in seg and "Rush Yards" not in seg:
        print("  contaminated rec")
        return False
    if str(line) not in seg and f"{float(line)}" not in seg:
        print(f"  wrong line in seg: {seg[:120]}")
        return False
    return True


def build(page, ticket) -> dict:
    # monkey-patch add
    nfl.add_leg = add_leg_tight
    # also need sacks in prop_match
    old = nfl.prop_match

    def pm(card_prop, want):
        if old(card_prop, want):
            return True
        cp, wp = P._norm(card_prop or ""), P._norm(want or "")
        return wp == "sacks" and cp == "sacks"

    nfl.prop_match = pm
    return nfl.build(page, ticket)


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "")]
        print(f"tabs={len(boards)}")
        # A=0 B=1 C=2 D=3
        results = []
        for ticket, idx in zip(TICKETS, [1, 2]):
            if idx >= len(boards):
                pg = ctx.new_page()
                pg.goto(nfl.BOARD, wait_until="domcontentloaded", timeout=60_000)
                boards.append(pg)
            page = boards[idx]
            results.append(build(page, ticket))
        # clear D if present
        if len(boards) > 3:
            page = boards[3]
            page.bring_to_front()
            try:
                frame = P.find_prizepicks_frame(page)
                P.clear_slip(frame)
                print("D cleared")
            except Exception as e:
                print("D clear", e)
        boards[0].bring_to_front()
    print("\n======== REPAIR SUMMARY ========")
    print("  A: READY (unchanged)")
    for r in results:
        print(f"  {r['id']}: {'READY' if r['ready'] else 'INCOMPLETE'} n={r['n']} fails={r['fails'] or '-'}")
    return 0 if all(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
