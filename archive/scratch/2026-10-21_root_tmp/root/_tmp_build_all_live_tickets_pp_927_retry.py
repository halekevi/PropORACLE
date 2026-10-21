#!/usr/bin/env python3
"""Retry: resolve live Goblin faces for every /tickets slip, then build CDP tabs. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
import utils.pp_manual_slip as M
from utils.pp_manual_slip import (
    add_cycled_leg,
    find_card,
    n_correct_power,
    prop_ok,
    react_search,
    _norm,
)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
TICKETS = _REPO / "ui_runner" / "runtime" / "tickets_latest.json"
OUT = _REPO / "data" / "reports" / "pp_all_live_tickets_2026-09-27_retry.json"

M.PROP_ALIASES.update(
    {
        "fgmade": ("fgmade", "madefgs", "fieldgoalsmade", "fg"),
        "madefgs": ("fgmade", "madefgs", "fieldgoalsmade"),
        "rushingyards": ("rushingyards", "rushyards", "rushyds"),
        "rushyards": ("rushingyards", "rushyards", "rushyds"),
        "receivingyards": ("receivingyards", "recyards", "receivingyds", "recyds"),
        "recyards": ("receivingyards", "recyards", "receivingyds", "recyds"),
        "earnedrunsallowed": ("earnedrunsallowed", "earnedruns", "er"),
        "earnedruns": ("earnedrunsallowed", "earnedruns", "er"),
        "walksallowed": ("walksallowed", "walks", "bb"),
        "hitsallowed": ("hitsallowed", "hits"),
        "pitchingouts": ("pitchingouts", "outs", "po"),
        "hitsrunsrbis": ("hitsrunsrbis", "hitsrunsribis"),
        "totalgames": ("totalgames",),
        "shotsontarget": ("shotsontarget", "sot"),
    }
)

PROP_DISPLAY = {
    "rushing_yards": "Rush Yards",
    "receiving_yards": "Rec Yards",
    "fg_made": "FG Made",
    "earned_runs_allowed": "Earned Runs Allowed",
    "walks_allowed": "Walks Allowed",
    "hits_allowed": "Hits Allowed",
    "pitching_outs": "Pitching Outs",
    "hits+runs+rbis": "Hits+Runs+RBIs",
    "total_games": "Total Games",
    "shots_on_target": "Shots On Target",
}


def _prop_label(raw: str) -> str:
    s = str(raw or "").strip()
    key = s.lower().replace(" ", "_")
    if key in PROP_DISPLAY:
        return PROP_DISPLAY[key]
    folded = "".join(ch for ch in s.casefold() if ch.isalnum())
    for k, v in PROP_DISPLAY.items():
        if "".join(ch for ch in k if ch.isalnum()) == folded:
            return v
    return s.replace("_", " ").title() if "_" in s else s


def load_all_tickets() -> list[dict]:
    d = json.loads(TICKETS.read_text(encoding="utf-8"))
    out: list[dict] = []
    for g in d.get("groups") or []:
        gname = str(g.get("group_name") or "")
        for t in g.get("tickets") or []:
            legs = []
            for leg in t.get("legs") or []:
                prop = _prop_label(str(leg.get("prop_type") or leg.get("prop") or ""))
                direction = str(leg.get("direction") or "OVER").upper()
                pick = str(leg.get("pick_type") or "goblin").strip().lower()
                legs.append(
                    {
                        "sport": str(leg.get("sport") or "").strip(),
                        "player": str(leg.get("player") or "").strip(),
                        "search": str(leg.get("player") or "").strip(),
                        "prop_type": prop,
                        "prop": prop,
                        "direction": direction,
                        "side": direction,
                        "line": float(leg["line"]),
                        "pick_type": pick,
                        "planned_line": float(leg["line"]),
                    }
                )
            if legs:
                out.append(
                    {
                        "id": t.get("ticket_id") or t.get("ticket_no") or gname,
                        "group": gname,
                        "mode": "power",
                        "legs": legs,
                        "n_legs": len(legs),
                    }
                )
    return out


def _player_hits(cards: list[dict], player: str) -> list[dict]:
    pn = _norm(player)
    return [
        c
        for c in cards
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]


def resolve_live_leg(page, leg: dict) -> dict | None:
    """Return leg with live line if pick_type face exists (exact or deepest ALT)."""
    want_pick = str(leg["pick_type"]).lower()
    react_search(page, "")
    page.wait_for_timeout(100)
    react_search(page, leg["player"])
    page.wait_for_timeout(1200)
    frame = P.find_prizepicks_frame(page)
    cards = _player_hits(P.get_all_cards(frame), leg["player"])
    if not cards:
        print(f"MISS {leg['sport']} {leg['player']} (no cards)", flush=True)
        return None

    def match_exact(hits: list[dict]) -> dict | None:
        for c in hits:
            if not prop_ok(leg["prop"], str(c.get("prop_type") or "")):
                continue
            if str(c.get("pick_type") or "").lower() != want_pick:
                continue
            if P._line_key(c.get("line")) == P._line_key(leg["line"]):
                return c
        return None

    exact = match_exact(cards)
    if exact is None:
        start = next(
            (
                c
                for c in cards
                if prop_ok(leg["prop"], str(c.get("prop_type") or ""))
            ),
            cards[0],
        )
        P.cycle_card_to_pick_type(
            frame,
            start.get("more_btn"),
            player=leg["player"],
            prop=start.get("prop_type") or leg["prop"],
            want_pick=want_pick,
            want_line=leg["line"],
            require_line=True,
            more_index=start.get("more_index"),
            max_clicks=14,
        )
        page.wait_for_timeout(350)
        react_search(page, leg["player"])
        page.wait_for_timeout(900)
        cards = _player_hits(P.get_all_cards(P.find_prizepicks_frame(page)), leg["player"])
        exact = match_exact(cards)

    if exact is not None:
        print(
            f"LIVE {leg['sport']:6} {leg['player']:22} {leg['prop']:22} "
            f"{leg['line']} ({want_pick})",
            flush=True,
        )
        return {**leg, "line": float(exact.get("line")), "alt": False}

    # Any face of same prop + pick_type (prefer deepest OVER / highest UNDER).
    same = [
        c
        for c in cards
        if prop_ok(leg["prop"], str(c.get("prop_type") or ""))
        and str(c.get("pick_type") or "").lower() == want_pick
    ]
    if not same:
        # One more cycle without require_line
        start = next(
            (
                c
                for c in cards
                if prop_ok(leg["prop"], str(c.get("prop_type") or ""))
            ),
            cards[0] if cards else None,
        )
        if start is not None:
            P.cycle_card_to_pick_type(
                P.find_prizepicks_frame(page),
                start.get("more_btn"),
                player=leg["player"],
                prop=start.get("prop_type") or leg["prop"],
                want_pick=want_pick,
                want_line=None,
                require_line=False,
                more_index=start.get("more_index"),
                max_clicks=14,
            )
            page.wait_for_timeout(350)
            react_search(page, leg["player"])
            page.wait_for_timeout(900)
            cards = _player_hits(
                P.get_all_cards(P.find_prizepicks_frame(page)), leg["player"]
            )
            same = [
                c
                for c in cards
                if prop_ok(leg["prop"], str(c.get("prop_type") or ""))
                and str(c.get("pick_type") or "").lower() == want_pick
            ]

    if not same:
        print(
            f"GONE {leg['sport']} {leg['player']} {leg['prop']} "
            f"want={leg['line']} ({want_pick})",
            flush=True,
        )
        return None

    if str(leg.get("direction") or "OVER").upper() == "UNDER":
        same.sort(key=lambda c: -float(c.get("line") or 0))
    else:
        same.sort(key=lambda c: float(c.get("line") or 99))
    c0 = same[0]
    live_line = float(c0.get("line"))
    print(
        f"ALT  {leg['sport']:6} {leg['player']:22} {leg['prop']:22} "
        f"{leg['line']} -> {live_line} ({want_pick})",
        flush=True,
    )
    return {**leg, "line": live_line, "alt": True, "planned_line": leg["line"]}


def resolve_ticket(page, ticket: dict) -> dict | None:
    resolved = []
    by_sport: dict[str, list[dict]] = {}
    for leg in ticket["legs"]:
        by_sport.setdefault(leg["sport"].upper(), []).append(leg)
    live_by_key: dict[tuple, dict] = {}
    for sport, chunk in by_sport.items():
        P.navigate_board_for_sport(page, sport, settle_ms=1600)
        for leg in chunk:
            got = resolve_live_leg(page, leg)
            if got is None:
                return None
            live_by_key[(_norm(leg["player"]), _norm(leg["prop"]))] = got
    for leg in ticket["legs"]:
        resolved.append(live_by_key[(_norm(leg["player"]), _norm(leg["prop"]))])
    return {**ticket, "legs": resolved}


def land_sport(page, frame, sport: str, *, allow_goto: bool):
    sp = str(sport or "").strip().upper()
    if allow_goto:
        P.navigate_board_for_sport(page, sp, settle_ms=1700)
        return P.find_prizepicks_frame(page)
    switched = P.switch_sport_tab(frame, page, sp, settle_ms=1400)
    if switched is None:
        P.navigate_board_for_sport(page, sp, settle_ms=1600)
        return P.find_prizepicks_frame(page)
    return switched


def add_leg(page, leg: dict) -> bool:
    frame = P.find_prizepicks_frame(page)
    direction = str(leg.get("direction") or "OVER").upper()
    if direction == "OVER":
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            return False
        return True
    player, prop, line, pick = leg["player"], leg["prop_type"], leg["line"], leg["pick_type"]
    card = find_card(
        P, frame, player, prop, line=line, pick=pick, line_key=P._line_key
    )
    if card is None:
        print(f"DROPPED: NO CARD {player} {prop} {line}", flush=True)
        return False
    need = P._line_key(card.get("line")) != P._line_key(line) or str(
        card.get("pick_type") or ""
    ).lower() != pick
    if need:
        P.cycle_card_to_pick_type(
            frame,
            card.get("more_btn"),
            player=player,
            prop=card.get("prop_type") or prop,
            want_pick=pick,
            want_line=line,
            require_line=True,
            more_index=card.get("more_index"),
        )
        page.wait_for_timeout(400)
        frame = P.find_prizepicks_frame(page)
        exact = find_card(
            P,
            frame,
            player,
            prop,
            line=line,
            pick=pick,
            line_key=P._line_key,
            require_exact=True,
        )
        if exact is None:
            print(f"DROPPED: LINE MOVED {player} want={line}", flush=True)
            return False
        card = exact
    if not P.click_leg(frame, card, "UNDER"):
        print(f"DROPPED: UNDER CLICK FAIL {player}", flush=True)
        return False
    print(f"[SLIP] clicked UNDER {player}", flush=True)
    return True


def build_one(page, ticket: dict) -> dict:
    tid, legs = ticket["id"], ticket["legs"]
    print(f"\n======== BUILD {tid} ========", flush=True)
    for x in legs:
        alt = " ALT" if x.get("alt") else ""
        print(
            f"  {x['sport']} {x['player']} {x['prop']} "
            f"{x['direction']} {x['line']}{alt} ({x['pick_type']})",
            flush=True,
        )
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    first = legs[0]
    P.navigate_board_for_sport(page, first["sport"], settle_ms=1700)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")

    for i, leg in enumerate(legs):
        frame = P.find_prizepicks_frame(page)
        landed = land_sport(page, frame, leg["sport"], allow_goto=(i == 0))
        if landed is None:
            print(f"DROPPED: SPORT TAB MISS {leg['player']}", flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [leg["player"]], "n": 0}
        frame = landed
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(650)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, leg["player"])
        page.wait_for_timeout(1200)
        if not add_leg(page, leg):
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [leg["player"]], "n": 0}
        time.sleep(0.2)

    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
            timeout=2000
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page),
            n_legs=len(legs),
            ticket_type="power",
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    print(f"[{tid}] slip_ok={n == len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    return {
        "id": tid,
        "group": ticket.get("group"),
        "ok": n == len(legs),
        "n": n,
        "want_n": len(legs),
        "px": px,
        "fails": [],
        "legs": [
            (
                x["sport"],
                x["player"],
                x["prop"],
                x["direction"],
                x["line"],
                bool(x.get("alt")),
            )
            for x in legs
        ],
        "raw_head": raw[:500],
    }


def main() -> int:
    planned = load_all_tickets()
    print(f"Planned slips: {len(planned)}", flush=True)
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page0 = next(
            (
                pg
                for pg in ctx.pages
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
            ),
            None,
        )
        if page0 is None:
            page0 = ctx.new_page()
            page0.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page0.wait_for_timeout(1000)

        # Resolve all tickets on one board page first (live faces).
        resolved: list[dict] = []
        skipped: list[dict] = []
        for t in planned:
            print(f"\n----- resolve {t['id']} -----", flush=True)
            got = resolve_ticket(page0, t)
            if got is None:
                skipped.append({"id": t["id"], "reason": "leg_gone"})
                print(f"SKIP {t['id']} — live face missing", flush=True)
            else:
                resolved.append(got)

        print(f"\nResolvable: {len(resolved)}/{len(planned)}", flush=True)
        if not resolved:
            OUT.write_text(
                json.dumps({"planned": planned, "skipped": skipped, "results": []}, indent=2),
                encoding="utf-8",
            )
            return 2

        pages = [page0]
        while len(pages) < len(resolved):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(700)
            pages.append(pg)

        results = []
        for pg, ticket in zip(pages, resolved):
            try:
                # Fresh clear before each build
                try:
                    P.clear_slip(P.find_prizepicks_frame(pg))
                except Exception:
                    pass
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append({"id": ticket["id"], "ok": False, "fails": [str(e)], "n": 0})

        OUT.write_text(
            json.dumps(
                {
                    "planned": len(planned),
                    "skipped": skipped,
                    "resolved": [
                        {
                            "id": t["id"],
                            "legs": [
                                (l["player"], l["prop"], l["line"], l.get("alt"))
                                for l in t["legs"]
                            ],
                        }
                        for t in resolved
                    ],
                    "results": results,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("\nSUMMARY", flush=True)
        ok_n = 0
        for r in results:
            print(
                f"  {r.get('id')} ok={r.get('ok')} n={r.get('n')}/{r.get('want_n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')}",
                flush=True,
            )
            if r.get("ok"):
                ok_n += 1
        for s in skipped:
            print(f"  SKIP {s.get('id')} {s.get('reason')}", flush=True)
        print(
            f"wrote {OUT}. Built {ok_n}/{len(results)} "
            f"(skipped {len(skipped)}). No submit.",
            flush=True,
        )
        return 0 if ok_n == len(results) and results else 2


if __name__ == "__main__":
    raise SystemExit(main())
