#!/usr/bin/env python3
"""Cycle leftover 9/24 G70 cards to the G70 line. Report live goblin. No submit."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

TARGETS = [
    {"sport": "WNBA", "player": "A'ja Wilson", "search": "Wilson", "prop": "Rebs+Asts", "chip": "Rebs+Asts", "line": 9.5},
    {"sport": "MLB", "player": "Andrew Painter", "search": "Andrew Painter", "prop": "Ks", "chip": "Pitcher Strikeouts", "line": 2.5},
    {"sport": "MLB", "player": "Tyler Phillips", "search": "Tyler Phillips", "prop": "Ks", "chip": "Pitcher Strikeouts", "line": 2.5},
    {"sport": "MLB", "player": "Ian Seymour", "search": "Ian Seymour", "prop": "Ks", "chip": "Pitcher Strikeouts", "line": 3.5},
    {"sport": "MLB", "player": "Tyler Mahle", "search": "Tyler Mahle", "prop": "PO", "chip": "Pitching Outs", "line": 14.5},
    {"sport": "TENNIS", "player": "Elise Mertens", "search": "Mertens", "prop": "Total Games", "chip": "Total Games", "line": 19.5},
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


def _norm(s: str) -> str:
    return "".join(ch for ch in str(s or "").casefold() if ch.isalnum())


def find_card(frame, player: str, prop: str) -> dict | None:
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    pp = _norm(prop)
    hits = []
    for c in cards:
        if pn[:6] not in _norm(c.get("player")) and _norm(c.get("player")) not in pn:
            continue
        cp = _norm(c.get("prop_type"))
        if pp in cp or cp in pp:
            hits.append(c)
    return hits[0] if hits else None


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
        last = None
        for t in TARGETS:
            print(f"\n===== {t['sport']} {t['player']} {t['prop']} want {t['line']} =====")
            frame = P.find_prizepicks_frame(page)
            if last != t["sport"]:
                if t["sport"] == "TENNIS":
                    P.navigate_board_for_sport(page, "TENNIS", settle_ms=2200)
                    frame = P.find_prizepicks_frame(page)
                else:
                    switched = P.switch_sport_tab(frame, page, t["sport"], settle_ms=1500)
                    if switched is not None:
                        frame = switched
                last = t["sport"]
            P.dismiss_modal(frame, page)
            react_search(page, "")
            page.wait_for_timeout(200)
            react_search(page, t["search"])
            page.wait_for_timeout(1400)
            frame = P.find_prizepicks_frame(page)
            P._switch_board_filter(frame, page, t["chip"])
            page.wait_for_timeout(900)
            frame = P.find_prizepicks_frame(page)
            card = find_card(frame, t["player"], t["prop"])
            if card is None:
                print("NO CARD")
                cards = P.get_all_cards(frame)
                for c in cards[:8]:
                    print(f"  sample {c.get('player')} | {c.get('line')} {c.get('prop_type')} | {c.get('pick_type')}")
                continue
            print(
                f"FOUND {card.get('player')} {card.get('line')} {card.get('prop_type')} {card.get('pick_type')}"
            )
            cycled = P.cycle_card_to_pick_type(
                frame,
                card.get("more_btn"),
                player=t["player"],
                prop=card.get("prop_type") or t["prop"],
                want_pick="goblin",
                want_line=t["line"],
                require_line=True,
                more_index=card.get("more_index"),
            )
            if cycled:
                print(f"MATCH goblin {cycled.get('line')}")
            else:
                cycled2 = P.cycle_card_to_pick_type(
                    frame,
                    card.get("more_btn"),
                    player=t["player"],
                    prop=card.get("prop_type") or t["prop"],
                    want_pick="goblin",
                    want_line=None,
                    require_line=False,
                    more_index=card.get("more_index"),
                )
                print(f"NO G70 LINE; live goblin={None if not cycled2 else cycled2.get('line')}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
