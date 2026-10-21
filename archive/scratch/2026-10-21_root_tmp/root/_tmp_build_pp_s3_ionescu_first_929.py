#!/usr/bin/env python3
"""Fresh Power-3: Ionescu PRA U25 + Calvert SOT O0.5 + Yamal SOT O1.5. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import n_correct_power, prop_ok, react_search, _norm

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = Path(r"H:/PropORACLE_main_cp/data/reports/pp_s3_ionescu_calvert_yamal_2026-09-29.json")


def solo_cards(frame, player: str) -> list[dict]:
    pn = _norm(player)
    out = []
    for c in P.get_all_cards(frame):
        name = str(c.get("player") or "")
        if "+" in name or " / " in name:
            continue  # skip combos
        cn = _norm(name)
        if pn in cn or cn in pn:
            out.append(c)
    return out


def click_under_exact(frame, page, player: str, prop: str, line: float, pick: str) -> bool:
    cards = solo_cards(frame, player)
    hits = [c for c in cards if prop_ok(prop, str(c.get("prop_type") or ""))]
    exact = [
        c
        for c in hits
        if str(c.get("pick_type") or "").lower() == pick
        and P._line_key(c.get("line")) == P._line_key(line)
    ]
    card = exact[0] if exact else None
    if card is None and hits:
        # cycle
        start = hits[0]
        P.cycle_card_to_pick_type(
            frame,
            start.get("more_btn"),
            player=player,
            prop=start.get("prop_type") or prop,
            want_pick=pick,
            want_line=line,
            require_line=True,
            more_index=start.get("more_index"),
            max_clicks=16,
        )
        page.wait_for_timeout(350)
        react_search(page, player)
        page.wait_for_timeout(1000)
        frame = P.find_prizepicks_frame(page)
        cards = solo_cards(frame, player)
        hits = [c for c in cards if prop_ok(prop, str(c.get("prop_type") or ""))]
        exact = [
            c
            for c in hits
            if str(c.get("pick_type") or "").lower() == pick
            and P._line_key(c.get("line")) == P._line_key(line)
        ]
        card = exact[0] if exact else None
    if card is None:
        print(f"no exact {player} {prop} {line} {pick}; have={[(c.get('prop_type'), c.get('line'), c.get('pick_type')) for c in cards[:8]]}", flush=True)
        return False

    # Prefer scoped Less click via more_btn parent
    btn = card.get("more_btn")
    if btn is not None:
        found = P._eval_more_js(
            """
            el => {
              let p = el;
              for (let i = 0; i < 5; i++) p = p?.parentElement;
              if (!p) return false;
              const txt = (p.innerText || '');
              if (txt.includes('+')) return 'combo';
              const btns = p.querySelectorAll('button');
              for (const b of btns) {
                if ((b.innerText || '').trim() === 'Less') { b.click(); return true; }
              }
              return false;
            }
            """,
            btn,
            frame=frame,
            more_index=card.get("more_index"),
        )
        print(f"Less scoped={found} for {player} {card.get('prop_type')} {card.get('line')}", flush=True)
        if found is True or found == True:
            page.wait_for_timeout(500)
            return True
    ok = P.click_leg(frame, card, "UNDER")
    print(f"click_leg UNDER ok={ok}", flush=True)
    return bool(ok)


def click_over_exact(frame, page, player: str, prop: str, line: float, pick: str) -> bool:
    cards = solo_cards(frame, player)
    hits = [c for c in cards if prop_ok(prop, str(c.get("prop_type") or ""))]
    exact = [
        c
        for c in hits
        if str(c.get("pick_type") or "").lower() == pick
        and P._line_key(c.get("line")) == P._line_key(line)
    ]
    card = exact[0] if exact else None
    if card is None:
        print(f"no exact OVER {player} {prop} {line}; have={[(c.get('prop_type'), c.get('line'), c.get('pick_type')) for c in cards[:8]]}", flush=True)
        return False
    ok = P.click_leg(frame, card, "OVER")
    print(f"click_leg OVER {player} {prop} {line} ok={ok}", flush=True)
    return bool(ok)


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = ctx.new_page()
        page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(900)
        page.set_viewport_size({"width": 1400, "height": 900})

        # 1) Ionescu PRA U25
        P.navigate_board_for_sport(page, "WNBA", settle_ms=1700)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")
        try:
            P._switch_board_filter(frame, page, "PRA")
        except Exception:
            P._switch_board_filter(frame, page, "Pts+Rebs+Asts")
        page.wait_for_timeout(400)
        react_search(page, "Sabrina Ionescu")
        page.wait_for_timeout(1400)
        frame = P.find_prizepicks_frame(page)
        if not click_under_exact(frame, page, "Sabrina Ionescu", "PRA", 25.0, "standard"):
            # also try Pts+Rebs+Asts label
            frame = P.find_prizepicks_frame(page)
            if not click_under_exact(frame, page, "Sabrina Ionescu", "Pts+Rebs+Asts", 25.0, "standard"):
                print("FAIL Ionescu", flush=True)
                return 2
        page.wait_for_timeout(500)
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        print(f"after Ionescu n={slip.get('n_selected')}", flush=True)

        # 2) Calvert SOT 0.5 goblin
        switched = P.switch_sport_tab(frame, page, "Soccer", settle_ms=1400)
        if switched is None:
            P.navigate_board_for_sport(page, "Soccer", settle_ms=1600)
        frame = P.find_prizepicks_frame(page)
        P._switch_board_filter(frame, page, "Shots On Target")
        page.wait_for_timeout(350)
        react_search(page, "Dominic Calvert-Lewin")
        page.wait_for_timeout(1400)
        frame = P.find_prizepicks_frame(page)
        if not click_over_exact(frame, page, "Dominic Calvert-Lewin", "Shots On Target", 0.5, "goblin"):
            print("FAIL Calvert", flush=True)
            return 2
        page.wait_for_timeout(400)
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        print(f"after Calvert n={slip.get('n_selected')}", flush=True)

        # 3) Yamal SOT 1.5 goblin
        frame = P.find_prizepicks_frame(page)
        P._switch_board_filter(frame, page, "Shots On Target")
        page.wait_for_timeout(300)
        react_search(page, "Lamine Yamal")
        page.wait_for_timeout(1400)
        frame = P.find_prizepicks_frame(page)
        if not click_over_exact(frame, page, "Lamine Yamal", "Shots On Target", 1.5, "goblin"):
            # accept 0.5 if 1.5 gone
            if not click_over_exact(frame, page, "Lamine Yamal", "Shots On Target", 0.5, "goblin"):
                print("FAIL Yamal", flush=True)
                return 2
        page.wait_for_timeout(500)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
        except Exception:
            pass
        frame = P.find_prizepicks_frame(page)
        slip = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
        raw = str(slip.get("raw_slip_section") or "")
        n = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, n if n else 3)
        result = {"ok": n == 3, "n": n, "px": px, "raw_head": raw[:600]}
        OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"FINAL ok={n==3} n={n} N-correct={px}", flush=True)
        print(raw[:500], flush=True)
        print(f"wrote {OUT}. No submit.", flush=True)
        return 0 if n == 3 else 2


if __name__ == "__main__":
    raise SystemExit(main())
