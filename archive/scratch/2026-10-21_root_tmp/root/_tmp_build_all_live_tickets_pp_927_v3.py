#!/usr/bin/env python3
"""Build each live /tickets slip on its own CDP tab, resolving faces at click time. No submit."""
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
from utils.pp_manual_slip import n_correct_power, prop_ok, react_search, _norm

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
TICKETS = _REPO / "ui_runner" / "runtime" / "tickets_latest.json"
OUT = _REPO / "data" / "reports" / "pp_all_live_tickets_2026-09-27_v3.json"

M.PROP_ALIASES.update(
    {
        "fgmade": ("fgmade", "madefgs", "fieldgoalsmade", "fg"),
        "madefgs": ("fgmade", "madefgs"),
        "rushingyards": ("rushingyards", "rushyards"),
        "rushyards": ("rushingyards", "rushyards"),
        "receivingyards": ("receivingyards", "recyards"),
        "recyards": ("receivingyards", "recyards"),
        "earnedrunsallowed": ("earnedrunsallowed", "earnedruns"),
        "earnedruns": ("earnedrunsallowed", "earnedruns"),
        "walksallowed": ("walksallowed", "walks"),
        "hitsallowed": ("hitsallowed", "hits"),
        "pitchingouts": ("pitchingouts", "outs", "po"),
        "hitsrunsrbis": ("hitsrunsrbis",),
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
    out = []
    for g in d.get("groups") or []:
        gname = str(g.get("group_name") or "")
        for t in g.get("tickets") or []:
            legs = []
            for leg in t.get("legs") or []:
                prop = _prop_label(str(leg.get("prop_type") or ""))
                legs.append(
                    {
                        "sport": str(leg.get("sport") or "").strip(),
                        "player": str(leg.get("player") or "").strip(),
                        "prop_type": prop,
                        "prop": prop,
                        "direction": str(leg.get("direction") or "OVER").upper(),
                        "line": float(leg["line"]),
                        "pick_type": str(leg.get("pick_type") or "goblin").lower(),
                    }
                )
            if legs:
                out.append(
                    {
                        "id": t.get("ticket_id") or gname,
                        "group": gname,
                        "legs": legs,
                    }
                )
    return out


def player_cards(frame, player: str) -> list[dict]:
    pn = _norm(player)
    return [
        c
        for c in P.get_all_cards(frame)
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]


def find_face(frame, leg: dict) -> dict | None:
    want_pick = leg["pick_type"]
    hits = [
        c
        for c in player_cards(frame, leg["player"])
        if prop_ok(leg["prop"], str(c.get("prop_type") or ""))
    ]
    if not hits:
        return None
    exact = [
        c
        for c in hits
        if str(c.get("pick_type") or "").lower() == want_pick
        and P._line_key(c.get("line")) == P._line_key(leg["line"])
    ]
    if exact:
        return exact[0]
    same_pick = [
        c for c in hits if str(c.get("pick_type") or "").lower() == want_pick
    ]
    if same_pick:
        if leg["direction"] == "UNDER":
            same_pick.sort(key=lambda c: -float(c.get("line") or 0))
        else:
            same_pick.sort(key=lambda c: float(c.get("line") or 99))
        return same_pick[0]
    return hits[0]


def add_leg_now(page, leg: dict) -> tuple[bool, str]:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    chip = P.prop_type_to_board_filter(leg["prop_type"])
    P._switch_board_filter(frame, page, chip)
    page.wait_for_timeout(500)
    react_search(page, "")
    page.wait_for_timeout(80)
    react_search(page, leg["player"])
    page.wait_for_timeout(1400)
    frame = P.find_prizepicks_frame(page)
    card = find_face(frame, leg)
    if card is None:
        return False, f"no_card {leg['player']}"

    want_pick = leg["pick_type"]
    need_cycle = (
        str(card.get("pick_type") or "").lower() != want_pick
        or P._line_key(card.get("line")) != P._line_key(leg["line"])
    )
    if need_cycle:
        P.cycle_card_to_pick_type(
            frame,
            card.get("more_btn"),
            player=leg["player"],
            prop=card.get("prop_type") or leg["prop"],
            want_pick=want_pick,
            want_line=leg["line"],
            require_line=True,
            more_index=card.get("more_index"),
            max_clicks=14,
        )
        page.wait_for_timeout(350)
        react_search(page, leg["player"])
        page.wait_for_timeout(1000)
        frame = P.find_prizepicks_frame(page)
        card = find_face(frame, leg)
        if card is None or str(card.get("pick_type") or "").lower() != want_pick:
            # accept any same-prop pick_type face
            P.cycle_card_to_pick_type(
                frame,
                (card or {}).get("more_btn") if card else None,
                player=leg["player"],
                prop=leg["prop"],
                want_pick=want_pick,
                want_line=None,
                require_line=False,
                more_index=(card or {}).get("more_index") if card else None,
                max_clicks=14,
            )
            page.wait_for_timeout(350)
            react_search(page, leg["player"])
            page.wait_for_timeout(1000)
            frame = P.find_prizepicks_frame(page)
            card = find_face(frame, leg)
            if card is None or str(card.get("pick_type") or "").lower() != want_pick:
                return False, f"no_{want_pick} {leg['player']} {leg['prop']}"

    live_line = card.get("line")
    if P._line_key(live_line) != P._line_key(leg["line"]):
        print(
            f"  ALT line {leg['player']} {leg['prop']} "
            f"{leg['line']} -> {live_line}",
            flush=True,
        )
        leg = {**leg, "line": float(live_line)}

    ok = P.click_leg(frame, card, leg["direction"])
    if not ok:
        return False, f"click_fail {leg['player']}"
    print(
        f"  OK {leg['sport']} {leg['player']} {leg['prop']} "
        f"{leg['direction']} {leg['line']} ({want_pick})",
        flush=True,
    )
    return True, ""


def build_one(page, ticket: dict) -> dict:
    tid = ticket["id"]
    legs = ticket["legs"]
    print(f"\n======== {tid} ========", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    first = legs[0]["sport"]
    P.navigate_board_for_sport(page, first, settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")

    used = []
    for i, leg in enumerate(legs):
        sp = leg["sport"]
        if i == 0:
            frame = P.find_prizepicks_frame(page)
        else:
            switched = P.switch_sport_tab(frame, page, sp, settle_ms=1400)
            if switched is None:
                P.navigate_board_for_sport(page, sp, settle_ms=1600)
            frame = P.find_prizepicks_frame(page)
        ok, why = add_leg_now(page, leg)
        if not ok:
            print(f"DROPPED: {why}", flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [why], "n": 0, "legs": used}
        used.append(
            (leg["sport"], leg["player"], leg["prop"], leg["direction"], leg["line"])
        )
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
        "ok": n == len(legs),
        "n": n,
        "want_n": len(legs),
        "px": px,
        "fails": [],
        "legs": used,
        "raw_head": raw[:400],
    }


def main() -> int:
    tickets = load_all_tickets()
    print(f"Building {len(tickets)} slips", flush=True)
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        pages = []
        for i, t in enumerate(tickets):
            if i == 0:
                pg = next(
                    (
                        p
                        for p in ctx.pages
                        if "prizepicks.com" in (p.url or "") and "board" in (p.url or "")
                    ),
                    None,
                )
                if pg is None:
                    pg = ctx.new_page()
                    pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
                    pg.wait_for_timeout(1000)
            else:
                pg = ctx.new_page()
                pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
                pg.wait_for_timeout(700)
            pages.append(pg)

        results = []
        for pg, ticket in zip(pages, tickets):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append({"id": ticket["id"], "ok": False, "fails": [str(e)], "n": 0})

        OUT.write_text(
            json.dumps({"results": results}, indent=2, default=str), encoding="utf-8"
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
        print(f"wrote {OUT}. Built {ok_n}/{len(results)}. No submit.", flush=True)
        return 0 if ok_n else 2


if __name__ == "__main__":
    raise SystemExit(main())
