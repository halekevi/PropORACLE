#!/usr/bin/env python3
"""Build one PrizePicks Power slip from exact G70 faces. Does not submit.

Hard rules:
- ``--anchor`` + ``assert_clears_exposure`` before any board click
  (same ledger as mixer + session; Dampier/Overton).
- A moved live line aborts the whole slip, prints ``DROPPED: LINE MOVED``,
  and clears any partial lineup. It does not continue as a 2-leg ticket.

  py -3.14 scripts/build_pp_manual_slip.py --min-payout 1.4 --date 2026-09-24 ^
      --anchor "A'ja Wilson" ^
      --leg "TENNIS|Elise Mertens|Total Games|OVER|19.5|goblin" ^
      --leg "WNBA|A'ja Wilson|Rebs+Asts|OVER|9.5|goblin" ^
      --leg "MLB|Tyler Mahle|Pitching Outs|OVER|14.5|goblin"
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.manual_slip_exposure import (
    ExposureBlocked,
    default_slate_date,
    load_session_slips,
)
from utils.pp_manual_slip import (
    add_cycled_leg,
    n_correct_power,
    order_legs_tennis_first,
    preflight_exposure,
    react_search,
)
from utils.slate_exposure_ledger import SlateExposureLedger

CDP = "http://127.0.0.1:9222"


def _parse_leg(raw: str) -> dict:
    parts = [p.strip() for p in str(raw).split("|")]
    while len(parts) < 6:
        parts.append("")
    sport, player, prop, direction, line, pick = parts[:6]
    try:
        line_f: float | None = float(line) if line else None
    except ValueError:
        line_f = None
    return {
        "sport": sport,
        "player": player,
        "search": player,
        "prop_type": prop,
        "direction": direction or "OVER",
        "line": line_f,
        "pick_type": (pick or "goblin").lower(),
    }


def _already_on_sport(page, sport: str) -> bool:
    league_id = P.PP_BOARD_LEAGUE_IDS.get(str(sport or "").strip().upper())
    url = str(getattr(page, "url", "") or "")
    return bool(league_id) and f"league_id={league_id}" in url


def _land_sport(page, frame, sport: str, *, allow_goto: bool):
    sp = str(sport or "").strip().upper()
    if allow_goto:
        P.navigate_board_for_sport(page, sp, settle_ms=1800)
        return P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, sp, settle_ms=1500)
    if switched is not None:
        return switched
    if _already_on_sport(page, sp):
        print(f"[SLIP] already on {sp} — staying (tab click missed, no goto)")
        return frame
    print(f"[SLIP] sport tab miss {sp} — not using goto (would drop slip)")
    return None


def _abort_partial(frame, why: str) -> None:
    print(why)
    try:
        P.clear_slip(frame)
        print("[SLIP] cleared partial lineup after abort")
    except Exception as e:
        print(f"[SLIP] WARN: partial lineup may still be on the board ({e})")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--leg",
        action="append",
        default=[],
        help="sport|player|prop|direction|line|pick_type (repeatable)",
    )
    ap.add_argument(
        "--anchor",
        required=True,
        help="Explicit anchor player for the slate exposure ledger",
    )
    ap.add_argument("--min-payout", type=float, default=1.4)
    ap.add_argument("--date", default="", help="Slate date YYYY-MM-DD")
    ap.add_argument("--cdp", default=CDP)
    ap.add_argument(
        "--session-only",
        action="store_true",
        help="Seed exposure from submitted session slips only (ignore packed G70 JSON).",
    )
    args = ap.parse_args()
    legs = [_parse_leg(x) for x in args.leg]
    if len(legs) < 2:
        print("ERROR: need at least two --leg", file=sys.stderr)
        return 1
    for leg in legs:
        if not leg["player"] or not leg["prop_type"] or leg["line"] is None:
            print(f"ERROR: bad leg {leg}", file=sys.stderr)
            return 1

    slate = str(args.date or "").strip()[:10] or default_slate_date()
    try:
        if args.session_only:
            ledger = SlateExposureLedger()
            for slip in load_session_slips(slate):
                slegs = [x for x in (slip.get("legs") or []) if isinstance(x, dict)]
                if slegs:
                    ledger.register(slegs, explicit_anchor_player=slip.get("anchor") or None)
            ok, reason = ledger.can_add(legs, explicit_anchor_player=str(args.anchor))
            if not ok:
                raise ExposureBlocked(reason)
            print("exposure OK (session-only; packed G70 ignored)")
        else:
            preflight_exposure(
                legs,
                slate_date=slate,
                explicit_anchor_player=str(args.anchor),
            )
            print("exposure OK")
    except ExposureBlocked as e:
        print(str(e))
        print("Do not build this slip.")
        return 2

    ordered = order_legs_tennis_first(legs)
    expected_n = len(ordered)

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(args.cdp, timeout=60_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "online-metrix" not in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if page is None:
            print("ERROR: no PrizePicks board tab")
            return 1
        page.bring_to_front()
        first = ordered[0]
        P.navigate_board_for_sport(page, first["sport"], settle_ms=1800)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")

        added: list[str] = []
        current_sport = ""
        for i, leg in enumerate(ordered):
            print(f"\n=== {leg['sport']} {leg['player']} {leg['prop_type']} {leg['line']} ===")
            frame = P.find_prizepicks_frame(page)
            sport = str(leg["sport"] or "").strip().upper()
            if sport != current_sport:
                landed = _land_sport(
                    page, frame, sport, allow_goto=(current_sport == "")
                )
                if landed is None:
                    _abort_partial(frame, f"DROPPED: SPORT TAB MISS {leg['player']}")
                    return 2
                frame = landed
                current_sport = sport
            P.dismiss_modal(frame, page)
            chip = P.prop_type_to_board_filter(leg["prop_type"])
            P._switch_board_filter(frame, page, chip)
            page.wait_for_timeout(800)
            react_search(page, "")
            page.wait_for_timeout(400)
            frame = P.find_prizepicks_frame(page)
            result = add_cycled_leg(P, frame, page, leg)
            if not result.ok and result.reason == "no_card":
                react_search(page, leg.get("search") or leg["player"])
                page.wait_for_timeout(1400)
                frame = P.find_prizepicks_frame(page)
                result = add_cycled_leg(P, frame, page, leg)
            if not result.ok:
                _abort_partial(frame, result.loud_fail())
                return 2
            added.append(leg["player"])
            time.sleep(0.3)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
                timeout=2000
            )
        except Exception:
            pass
        slip = P.read_slip(
            P.find_prizepicks_frame(page), n_legs=expected_n, ticket_type="power"
        ) or {}
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        selected = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, selected if selected else expected_n)
        print(f"\n[FINAL] n={selected} added={added} {expected_n}-correct={px}")
        print(raw[:1200])
        if selected != expected_n:
            _abort_partial(
                P.find_prizepicks_frame(page),
                f"FAIL: slip count {selected} != {expected_n} planned legs",
            )
            return 2
        missing = [name for name in added if name.split()[-1] not in raw]
        if missing:
            _abort_partial(
                P.find_prizepicks_frame(page),
                f"FAIL: missing from slip {missing}",
            )
            return 2
        if px is not None and px + 1e-9 < float(args.min_payout):
            print(f"UNDER {args.min_payout}x — do not submit")
            return 3
        print("READY on the board. I did not submit.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
