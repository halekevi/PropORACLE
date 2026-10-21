#!/usr/bin/env python3
"""9/24 T2 clean rebuild: Mertens / Wilson / Mahle. JS-click only. No submit."""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

LEGS = [
    {
        "sport": "TENNIS",
        "player": "Elise Mertens",
        "search": "Mertens",
        "prop": "Total Games",
        "chip": "Total Games",
        "line": 19.5,
        "navigate": True,
    },
    {
        "sport": "WNBA",
        "player": "A'ja Wilson",
        "search": "Wilson",
        "prop": "Rebs+Asts",
        "chip": "Rebs+Asts",
        "line": 9.5,
    },
    {
        "sport": "MLB",
        "player": "Tyler Mahle",
        "search": "Tyler Mahle",
        "prop": "Pitching Outs",
        "chip": "Pitching Outs",
        "line": 14.5,
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
    }""",
        q,
    )


def _norm(s: str) -> str:
    return "".join(ch for ch in str(s or "").casefold() if ch.isalnum())


def find_card(frame, player: str, prop: str) -> dict | None:
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    pp = _norm(prop)
    for c in cards:
        cn = _norm(c.get("player"))
        if pn not in cn and cn not in pn:
            continue
        cp = _norm(c.get("prop_type"))
        if pp in cp or cp in pp:
            return c
    return None


def force_clear(page, frame) -> None:
    page.evaluate(
        """() => {
      const btns = Array.from(document.querySelectorAll('button'));
      const b = btns.find(x => (x.innerText || '').trim() === 'Clear');
      if (b) b.click();
    }"""
    )
    page.wait_for_timeout(800)
    P.clear_slip(frame)


def add_cycled(frame, page, player: str, prop: str, line: float) -> bool:
    card = find_card(frame, player, prop)
    if card is None:
        print("NO CARD", player, prop)
        return False
    print(f"FOUND {card.get('player')} {card.get('line')} {card.get('prop_type')} {card.get('pick_type')}")
    need = P._line_key(card.get("line")) != P._line_key(line) or str(
        card.get("pick_type") or ""
    ).lower() != "goblin"
    if need:
        cycled = P.cycle_card_to_pick_type(
            frame,
            card.get("more_btn"),
            player=player,
            prop=card.get("prop_type") or prop,
            want_pick="goblin",
            want_line=line,
            require_line=True,
            more_index=card.get("more_index"),
        )
        if not cycled:
            print("CYCLE FAIL", player)
            return False
        page.wait_for_timeout(400)
        frame = P.find_prizepicks_frame(page)
        card = find_card(frame, player, prop)
        if card is None:
            print("CYCLE LOST CARD", player)
            return False
        print(f"CYCLED {card.get('player')} {card.get('line')} {card.get('pick_type')}")
    btn = card.get("more_btn")
    if btn is None:
        print("NO MORE BTN", player)
        return False
    try:
        btn.click(timeout=1500, no_wait_after=True)
    except Exception:
        btn.click(force=True, timeout=2500, no_wait_after=True)
    page.wait_for_timeout(500)
    print("CLICK more", player)
    return True


def slip_players(raw: str) -> list[str]:
    block = raw.split("Current Lineup", 1)[-1]
    names = []
    for m in re.finditer(r"\n([A-Za-z][A-Za-z .'\-]+)\n\n(?:TENNIS|WNBA|MLB)", block):
        names.append(m.group(1).strip())
    if not names:
        for m in re.finditer(
            r"\n([A-Za-z][A-Za-z .'\-]+)\n\n(?:TENNIS[^\n]*|WNBA[^\n]*|MLB[^\n]*)",
            block,
        ):
            names.append(m.group(1).strip())
    return names


def main() -> int:
    want = ["Elise Mertens", "A'ja Wilson", "Tyler Mahle"]
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
        P.navigate_board_for_sport(page, "TENNIS", settle_ms=1800)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        force_clear(page, frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")

        for leg in LEGS:
            print(f"\n=== {leg['sport']} {leg['player']} {leg['prop']} {leg['line']} ===")
            frame = P.find_prizepicks_frame(page)
            if leg.get("navigate"):
                P.navigate_board_for_sport(page, leg["sport"], settle_ms=1600)
            else:
                switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1400)
                if switched is not None:
                    frame = switched
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            P._switch_board_filter(frame, page, leg["chip"])
            page.wait_for_timeout(800)
            react_search(page, "")
            page.wait_for_timeout(200)
            react_search(page, leg["search"])
            page.wait_for_timeout(1600)
            frame = P.find_prizepicks_frame(page)
            ok = add_cycled(frame, page, leg["player"], leg["prop"], leg["line"])
            print("add", ok)
            time.sleep(0.5)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
        except Exception:
            pass
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
        names = slip_players(raw)
        n = int(slip.get("n_selected") or 0)
        m = re.search(r"Power Play.*?3 correct pays\s*(\d+(?:\.\d+)?)\s*X", raw, flags=re.I | re.S)
        px = float(m.group(1)) if m else None
        shot = _REPO / "data" / "reports" / "pp_g70_924_t2.png"
        page.screenshot(path=str(shot), full_page=False)
        print("\n[FINAL] n", n, "names", names, "3-correct", px)
        print(raw[:1400])
        if n != 3:
            print("NOT 3 LEGS")
            return 2
        missing = [w for w in want if not any(_norm(w) in _norm(x) or _norm(x) in _norm(w) for x in names)]
        if missing:
            print("WRONG NAMES missing", missing, "got", names)
            return 2
        if px is not None and px + 1e-9 < 1.4:
            print("UNDER 1.4x — do not submit")
            return 3
        print("READY. I did not submit.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
