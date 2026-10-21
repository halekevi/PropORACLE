#!/usr/bin/env python3
"""Finish Power-3: keep Ionescu if present, add Calvert SOT + Yamal SOT. No submit."""
from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import n_correct_power, prop_ok, react_search, _norm

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = Path(r"H:/PropORACLE_main_cp/data/reports/pp_s3_ionescu_calvert_yamal_2026-09-29.json")


def sot_ok(card_prop: str) -> bool:
    p = _norm(card_prop)
    return p in {"sot", "shotsontarget", "shotsontargets"} or prop_ok(
        "Shots On Target", card_prop
    )


def solo_cards(frame, player: str) -> list[dict]:
    pn = _norm(player)
    out = []
    for c in P.get_all_cards(frame):
        name = str(c.get("player") or "")
        if "+" in name:
            continue
        cn = _norm(name)
        if pn in cn or cn in pn:
            out.append(c)
    return out


def add_sot_over(frame, page, player: str, line: float) -> bool:
    P._switch_board_filter(frame, page, "SOT")
    page.wait_for_timeout(250)
    try:
        P._switch_board_filter(frame, page, "Shots On Target")
    except Exception:
        pass
    page.wait_for_timeout(300)
    react_search(page, player)
    page.wait_for_timeout(1400)
    frame = P.find_prizepicks_frame(page)
    cards = solo_cards(frame, player)
    hits = [c for c in cards if sot_ok(str(c.get("prop_type") or ""))]
    print(
        f"{player} sot cards={[ (c.get('prop_type'), c.get('line'), c.get('pick_type')) for c in hits ]}",
        flush=True,
    )
    exact = [
        c
        for c in hits
        if str(c.get("pick_type") or "").lower() == "goblin"
        and P._line_key(c.get("line")) == P._line_key(line)
    ]
    card = exact[0] if exact else None
    if card is None:
        # deepest goblin SOT
        gobs = [c for c in hits if str(c.get("pick_type") or "").lower() == "goblin"]
        gobs.sort(key=lambda c: float(c.get("line") or 99))
        card = gobs[0] if gobs else None
    if card is None:
        return False
    print(
        f"clicking {player} {card.get('prop_type')} {card.get('line')} OVER",
        flush=True,
    )
    return bool(P.click_leg(frame, card, "OVER"))


def ensure_ionescu(frame, page) -> bool:
    slip = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or "")
    if "ionescu" in raw.casefold() and int(slip.get("n_selected") or 0) >= 1:
        print(f"Ionescu already on slip n={slip.get('n_selected')}", flush=True)
        return True
    P.navigate_board_for_sport(page, "WNBA", settle_ms=1600)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    try:
        P._switch_board_filter(frame, page, "PRA")
    except Exception:
        pass
    react_search(page, "Sabrina Ionescu")
    page.wait_for_timeout(1400)
    frame = P.find_prizepicks_frame(page)
    cards = solo_cards(frame, "Sabrina Ionescu")
    hits = [
        c
        for c in cards
        if _norm(c.get("prop_type")) in {"pra", "ptsrebsasts"}
        or prop_ok("Pts+Rebs+Asts", str(c.get("prop_type") or ""))
    ]
    exact = [
        c
        for c in hits
        if str(c.get("pick_type") or "").lower() == "standard"
        and P._line_key(c.get("line")) == P._line_key(25)
    ]
    card = exact[0] if exact else None
    if card is None:
        print("no Ionescu PRA 25", flush=True)
        return False
    ok = P.click_leg(frame, card, "UNDER")
    page.wait_for_timeout(500)
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    print(f"Ionescu add ok={ok} n={slip.get('n_selected')}", flush=True)
    return int(slip.get("n_selected") or 0) >= 1


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = ctx.new_page()
        page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(800)
        page.set_viewport_size({"width": 1400, "height": 900})
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)

        if not ensure_ionescu(frame, page):
            return 2

        # Calvert
        switched = P.switch_sport_tab(
            P.find_prizepicks_frame(page), page, "Soccer", settle_ms=1400
        )
        if switched is None:
            P.navigate_board_for_sport(page, "Soccer", settle_ms=1600)
        frame = P.find_prizepicks_frame(page)
        if not add_sot_over(frame, page, "Dominic Calvert-Lewin", 0.5):
            print("FAIL Calvert", flush=True)
            return 2
        page.wait_for_timeout(400)
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        print(f"after Calvert n={slip.get('n_selected')}", flush=True)

        # Yamal 1.5 preferred
        frame = P.find_prizepicks_frame(page)
        if not add_sot_over(frame, page, "Lamine Yamal", 1.5):
            print("FAIL Yamal", flush=True)
            return 2
        page.wait_for_timeout(500)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
                timeout=2000
            )
        except Exception:
            pass
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        raw = str(slip.get("raw_slip_section") or "")
        n = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, n if n else 3)
        OUT.write_text(
            json.dumps({"ok": n == 3, "n": n, "px": px, "raw_head": raw[:700]}, indent=2),
            encoding="utf-8",
        )
        print(f"FINAL ok={n==3} n={n} N-correct={px}", flush=True)
        print(raw[:550], flush=True)
        print(f"wrote {OUT}. No submit.", flush=True)
        return 0 if n == 3 else 2


if __name__ == "__main__":
    raise SystemExit(main())
