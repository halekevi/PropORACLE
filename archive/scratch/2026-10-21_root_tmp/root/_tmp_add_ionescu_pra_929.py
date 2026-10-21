#!/usr/bin/env python3
"""Add Ionescu PRA U25 onto the open 2-goblin soccer slip. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
import utils.pp_manual_slip as M
from utils.pp_manual_slip import n_correct_power, prop_ok, react_search, _norm

CDP = "http://127.0.0.1:9222"


def player_cards(frame, player: str):
    pn = _norm(player)
    return [
        c
        for c in P.get_all_cards(frame)
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        # Prefer newest board tab (last created)
        pages = [
            p
            for p in ctx.pages
            if "prizepicks.com" in (p.url or "") and "board" in (p.url or "")
        ]
        if not pages:
            print("no board tab", flush=True)
            return 2
        page = pages[-1]
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)
        slip0 = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
        print(f"before n={slip0.get('n_selected')} raw_head={(slip0.get('raw_slip_section') or '')[:250]}", flush=True)

        # Switch to WNBA and add Ionescu PRA Less 25
        switched = P.switch_sport_tab(frame, page, "WNBA", settle_ms=1500)
        if switched is None:
            P.navigate_board_for_sport(page, "WNBA", settle_ms=1700)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, "PRA")
        page.wait_for_timeout(500)
        # PRA chip might be labeled Pts+Rebs+Asts
        try:
            P._switch_board_filter(frame, page, "Pts+Rebs+Asts")
        except Exception:
            pass
        page.wait_for_timeout(400)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, "Sabrina Ionescu")
        page.wait_for_timeout(1600)
        frame = P.find_prizepicks_frame(page)
        cards = player_cards(frame, "Sabrina Ionescu")
        print("cards:", [(c.get("player"), c.get("prop_type"), c.get("line"), c.get("pick_type")) for c in cards[:12]], flush=True)
        hits = [
            c
            for c in cards
            if prop_ok("Pts+Rebs+Asts", str(c.get("prop_type") or ""))
            or prop_ok("PRA", str(c.get("prop_type") or ""))
            or "pra" in _norm(c.get("prop_type"))
        ]
        # Prefer standard 25
        exact = [
            c
            for c in hits
            if str(c.get("pick_type") or "").lower() == "standard"
            and P._line_key(c.get("line")) == P._line_key(25)
        ]
        card = exact[0] if exact else (hits[0] if hits else None)
        if card is None:
            # cycle from any Ionescu card
            if not cards:
                print("no Ionescu cards", flush=True)
                return 2
            start = cards[0]
            P.cycle_card_to_pick_type(
                frame,
                start.get("more_btn"),
                player="Sabrina Ionescu",
                prop=start.get("prop_type") or "PRA",
                want_pick="standard",
                want_line=25.0,
                require_line=True,
                more_index=start.get("more_index"),
                max_clicks=16,
            )
            page.wait_for_timeout(400)
            react_search(page, "Sabrina Ionescu")
            page.wait_for_timeout(1200)
            frame = P.find_prizepicks_frame(page)
            cards = player_cards(frame, "Sabrina Ionescu")
            hits = [
                c
                for c in cards
                if prop_ok("Pts+Rebs+Asts", str(c.get("prop_type") or ""))
                or "pra" in _norm(c.get("prop_type"))
            ]
            exact = [
                c
                for c in hits
                if str(c.get("pick_type") or "").lower() == "standard"
                and P._line_key(c.get("line")) == P._line_key(25)
            ]
            card = exact[0] if exact else (hits[0] if hits else None)
        if card is None:
            print("still no PRA card", flush=True)
            return 2
        print(f"clicking {card.get('player')} {card.get('prop_type')} {card.get('line')} {card.get('pick_type')} UNDER", flush=True)
        ok = P.click_leg(frame, card, "UNDER")
        print(f"click_ok={ok}", flush=True)
        page.wait_for_timeout(800)
        # Also try Less button text click if slip didn't grow
        frame = P.find_prizepicks_frame(page)
        slip1 = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
        n = int(slip1.get("n_selected") or 0)
        if n < 3:
            # force Less on visible Ionescu PRA card via get_by_text
            try:
                # click Less near PRA 25
                page.get_by_text("Pts+Rebs+Asts", exact=False).first.wait_for(timeout=2000)
            except Exception:
                pass
            try:
                # Prefer card Less under 25
                less = frame.locator("text=Less").first
                less.click(timeout=2500)
                print("clicked Less fallback", flush=True)
                page.wait_for_timeout(600)
            except Exception as e:
                print(f"Less fallback fail {e}", flush=True)
            frame = P.find_prizepicks_frame(page)
            slip1 = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
            n = int(slip1.get("n_selected") or 0)

        raw = str(slip1.get("raw_slip_section") or "")
        px = n_correct_power(raw, n if n else 3)
        print(f"after n={n} N-correct={px}", flush=True)
        print(raw[:500], flush=True)
        return 0 if n >= 3 else 2


if __name__ == "__main__":
    raise SystemExit(main())
