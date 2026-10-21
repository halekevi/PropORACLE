#!/usr/bin/env python3
"""Probe leftover 9/24 G70 faces vs live board. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

CANDIDATES = [
    {"sport": "WNBA", "player": "A'ja Wilson", "search": "Wilson", "prop_type": "Rebs+Asts", "direction": "OVER", "line": 9.5, "pick_type": "goblin"},
    {"sport": "MLB", "player": "Andrew Painter", "search": "Andrew Painter", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
    {"sport": "MLB", "player": "Tyler Phillips", "search": "Tyler Phillips", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
    {"sport": "MLB", "player": "Ian Seymour", "search": "Ian Seymour", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 3.5, "pick_type": "goblin"},
    {"sport": "MLB", "player": "Cam Schlittler", "search": "Cam Schlittler", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 3.5, "pick_type": "goblin"},
    {"sport": "MLB", "player": "Tyler Glasnow", "search": "Tyler Glasnow", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 4.5, "pick_type": "goblin"},
    {"sport": "TENNIS", "player": "Elise Mertens", "search": "Mertens", "prop_type": "Total Games", "direction": "OVER", "line": 19.5, "pick_type": "goblin"},
]


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


def dump_chips(page) -> list[str]:
    return page.evaluate(
        """() => {
      const els = Array.from(document.querySelectorAll('button, [role="tab"]'));
      const out = [];
      for (const e of els) {
        const t = (e.innerText || '').trim().split('\\n')[0].trim();
        if (t && t.length < 28) out.push(t);
      }
      return [...new Set(out)].slice(0, 80);
    }"""
    )


def dump_player_cards(frame, player: str) -> list[str]:
    cards = P.get_all_cards(frame)
    rows = []
    want = player.casefold().replace("'", "")
    for c in cards:
        nm = str(c.get("player") or "")
        if want[:6] not in nm.casefold().replace("'", "") and nm.casefold().replace("'", "") not in want:
            continue
        rows.append(f"{nm} | {c.get('line')} {c.get('prop_type')} | {c.get('pick_type')}")
    return rows


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=60_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if page is None:
            print("ERROR: no board")
            return 1
        page.bring_to_front()
        last_sport = None
        for leg in CANDIDATES:
            print(f"\n===== {leg['sport']} {leg['player']} {leg['prop_type']} {leg['line']} =====")
            frame = P.find_prizepicks_frame(page)
            if last_sport != leg["sport"]:
                switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1500)
                if switched is not None:
                    frame = switched
                last_sport = leg["sport"]
                chips = dump_chips(page)
                safe = [c.encode("ascii", "replace").decode() for c in chips[:40]]
                print("CHIPS", safe)
            P.dismiss_modal(frame, page)
            react_search(page, "")
            page.wait_for_timeout(200)
            react_search(page, leg["search"])
            page.wait_for_timeout(1400)
            frame = P.find_prizepicks_frame(page)
            rows = dump_player_cards(frame, leg["player"])
            print("SEARCH CARDS:")
            for r in rows[:12]:
                print(" ", r)
            tab = P.prop_type_to_board_filter(leg["prop_type"])
            P._switch_board_filter(frame, page, tab)
            page.wait_for_timeout(900)
            frame = P.find_prizepicks_frame(page)
            rows2 = dump_player_cards(frame, leg["player"])
            print(f"AFTER FILTER {tab}:")
            for r in rows2[:12]:
                print(" ", r)
            if not rows2:
                cards = P.get_all_cards(frame)
                print("FILTER SAMPLE:")
                for c in cards[:8]:
                    print(f"  {c.get('player')} | {c.get('line')} {c.get('prop_type')} | {c.get('pick_type')}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
