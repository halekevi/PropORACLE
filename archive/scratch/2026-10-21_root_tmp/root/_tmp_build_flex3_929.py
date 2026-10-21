#!/usr/bin/env python3
"""Flex 3-leg mix: Calvert SOT Goblin + Yamal SOT (live) + Ionescu PRA U25. No submit."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import n_correct_power, prop_ok, react_search, _norm

spec = importlib.util.spec_from_file_location(
    "pp929", _REPO / "_tmp_build_pp_tickets_929.py"
)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = Path(r"H:/PropORACLE_main_cp/data/reports/pp_flex3_calvert_yamal_ionescu_2026-09-29.json")


def patch_flex_build(page, ticket):
    """Like build_one but Flex + verify after each leg."""
    tid = ticket["id"]
    legs = ticket["legs"]
    print(f"\n======== {tid} (FLEX) ========", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, legs[0]["sport"], settle_ms=1700)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "flex")

    used = []
    for i, leg in enumerate(legs):
        if i > 0:
            switched = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1300)
            if switched is None:
                P.navigate_board_for_sport(page, leg["sport"], settle_ms=1500)
            frame = P.find_prizepicks_frame(page)
        # For Yamal SOT accept nearest goblin or standard
        ok, why, live = mod.add_leg_now(page, leg)
        if not ok and "Yamal" in leg["player"]:
            for line, pick in [(1.5, "goblin"), (0.5, "goblin"), (2.0, "standard"), (2.0, "goblin")]:
                alt = {**leg, "line": line, "pick_type": pick}
                ok, why, live = mod.add_leg_now(page, alt)
                if ok:
                    break
        if not ok:
            print(f"DROPPED: {why}", flush=True)
            return {"id": tid, "ok": False, "fails": [why], "n": 0, "legs": used}
        used.append(live)
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="flex") or {}
        print(f"  slip n after leg {i+1}: {slip.get('n_selected')}", flush=True)

    try:
        P.find_prizepicks_frame(page).get_by_text("Flex", exact=False).first.click(timeout=2000)
    except Exception:
        pass
    slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="flex") or {}
    raw = str(slip.get("raw_slip_section") or "")
    n = int(slip.get("n_selected") or 0)
    print(raw[:500], flush=True)
    return {
        "id": tid,
        "ok": n == 3,
        "n": n,
        "want_n": 3,
        "legs": used,
        "raw_head": raw[:700],
        "px": n_correct_power(raw, n if n else 3),
    }


def main() -> int:
    ticket = {
        "id": "F3-Calvert-Yamal-IonescuPRA",
        "legs": [
            {
                "sport": "Soccer",
                "player": "Dominic Calvert-Lewin",
                "prop": "SOT",
                "prop_type": "SOT",
                "direction": "OVER",
                "line": 0.5,
                "pick_type": "goblin",
            },
            {
                "sport": "Soccer",
                "player": "Lamine Yamal",
                "prop": "SOT",
                "prop_type": "SOT",
                "direction": "OVER",
                "line": 1.5,
                "pick_type": "goblin",
            },
            {
                "sport": "WNBA",
                "player": "Sabrina Ionescu",
                "prop": "PRA",
                "prop_type": "PRA",
                "direction": "UNDER",
                "line": 25.0,
                "pick_type": "standard",
            },
        ],
    }
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = ctx.new_page()
        page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(800)
        result = patch_flex_build(page, ticket)
        OUT.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        print(
            f"FINAL ok={result.get('ok')} n={result.get('n')} fails={result.get('fails')}",
            flush=True,
        )
        print(f"wrote {OUT}. No submit.", flush=True)
        return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
