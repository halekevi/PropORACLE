#!/usr/bin/env python3
"""Fix soccer 2-leg and clean tennis Zverev/Paul Power 2. No submit."""
from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import n_correct_power, react_search, _norm

spec = importlib.util.spec_from_file_location(
    "pp929", _REPO / "_tmp_build_pp_tickets_929.py"
)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"


def slip_raw(page) -> tuple[int, str]:
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
    return int(slip.get("n_selected") or 0), str(slip.get("raw_slip_section") or "")


def build_soccer(page) -> bool:
    print("\n======== SOCCER Baturina+Sucic ========", flush=True)
    P.navigate_board_for_sport(page, "Soccer", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    try:
        P._switch_board_filter(frame, page, "Shots")
    except Exception:
        pass

    for player in ("Martin Baturina", "Luka Sucic"):
        react_search(page, "")
        page.wait_for_timeout(150)
        react_search(page, player)
        page.wait_for_timeout(1500)
        ok, why, live = mod.add_leg_now(
            page,
            {
                "sport": "Soccer",
                "player": player,
                "prop": "Shots",
                "prop_type": "Shots",
                "direction": "OVER",
                "line": 0.5,
                "pick_type": "goblin",
            },
        )
        print(f"  add {player}: ok={ok} why={why} live={live.get('line') if isinstance(live, dict) else live}", flush=True)
        n, raw = slip_raw(page)
        print(f"  n={n} has_baturina={'baturina' in raw.casefold()} has_sucic={'sucic' in raw.casefold()}", flush=True)
        if not ok:
            return False
        time.sleep(0.4)

    n, raw = slip_raw(page)
    print(raw[:400], flush=True)
    return n == 2 and "baturina" in raw.casefold() and "sucic" in raw.casefold()


def build_zverev_paul(page) -> bool:
    print("\n======== TENNIS Zverev+Paul ========", flush=True)
    P.navigate_board_for_sport(page, "TENNIS", settle_ms=1700)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")

    legs = [
        {"sport": "TENNIS", "player": "Alexander Zverev", "prop": "Total Games", "prop_type": "Total Games", "direction": "OVER", "line": 18.5, "pick_type": "goblin"},
        {"sport": "TENNIS", "player": "Tommy Paul", "prop": "Total Games", "prop_type": "Total Games", "direction": "OVER", "line": 21.5, "pick_type": "goblin"},
    ]
    for i, leg in enumerate(legs):
        if i > 0:
            frame = P.find_prizepicks_frame(page)
        # clear search between
        react_search(page, "")
        page.wait_for_timeout(100)
        ok, why, live = mod.add_leg_now(page, leg)
        if not ok:
            # cycle from standard
            try:
                P._switch_board_filter(P.find_prizepicks_frame(page), page, "Total Games")
            except Exception:
                pass
            react_search(page, leg["player"])
            page.wait_for_timeout(1200)
            frame = P.find_prizepicks_frame(page)
            cards = [
                c
                for c in P.get_all_cards(frame)
                if _norm(leg["player"]) in _norm(c.get("player"))
                or _norm(c.get("player")) in _norm(leg["player"])
            ]
            if cards:
                start = cards[0]
                P.cycle_card_to_pick_type(
                    frame,
                    start.get("more_btn"),
                    player=leg["player"],
                    prop=start.get("prop_type") or "Total Games",
                    want_pick="goblin",
                    want_line=leg["line"],
                    require_line=False,
                    more_index=start.get("more_index"),
                    max_clicks=18,
                )
                page.wait_for_timeout(400)
                react_search(page, leg["player"])
                page.wait_for_timeout(1000)
                ok, why, live = mod.add_leg_now(page, leg)
        print(f"  {leg['player']}: ok={ok} {why}", flush=True)
        n, raw = slip_raw(page)
        print(f"  n={n}", flush=True)
        if not ok:
            return False
    n, raw = slip_raw(page)
    print(raw[:400], flush=True)
    px = n_correct_power(raw, n if n else 2)
    print(f"FINAL tennis n={n} N-correct={px}", flush=True)
    return n == 2


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        ok_s = ok_t = False
        page = ctx.new_page()
        page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(800)
        try:
            ok_s = build_soccer(page)
        except Exception as e:
            print(f"soccer EXC {e}", flush=True)
        page2 = ctx.new_page()
        page2.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
        page2.wait_for_timeout(800)
        try:
            ok_t = build_zverev_paul(page2)
        except Exception as e:
            print(f"tennis EXC {e}", flush=True)
        print(f"\nDONE soccer={ok_s} zverev_paul={ok_t}. No submit.", flush=True)
        return 0 if (ok_s or ok_t) else 2


if __name__ == "__main__":
    raise SystemExit(main())
