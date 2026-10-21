"""Build tomorrow CFB L5=5 Goblin tickets in separate PP tabs from live-cycled faces."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.cfb_keep_gates import (
    cfb_ticket_fade,
    cfb_pass_yards_goblin_skip_d,
    cfb_list_eligible,
)
from utils.pp_manual_slip import (
    add_cycled_leg,
    n_correct_power,
    react_search,
    _norm,
    prop_ok,
)
from utils.prop_norm import canon_prop

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board?league_id=15"
STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx"
)
OUT = _REPO / "data" / "reports" / "cfb_sat_tabs_ready_2026-09-26.json"


def d_ok(side: str, od: str) -> bool:
    o = str(od).lower()
    if side == "OVER":
        return ("weak" in o) or ("below" in o)
    return ("elite" in o) or ("above" in o)


def yard_prop_match(want: str, got: str) -> bool:
    if prop_ok(want, got):
        return True
    wp, cp = _norm(want), _norm(got)
    pairs = (
        ("pass", "yard"),
        ("rush", "yard"),
        ("rec", "yard"),
    )
    for a, b in pairs:
        if a in wp and b in wp and a in cp and b in cp:
            return True
    return False


def load_goblin_clears() -> list[dict]:
    df = pd.read_excel(STEP8)
    d = df["Direction"].astype(str).str.upper()
    l5 = pd.to_numeric(df["L5 Over"], errors="coerce").where(
        d == "OVER", pd.to_numeric(df["L5 Under"], errors="coerce")
    )
    l10 = pd.to_numeric(df["L10 Over"], errors="coerce").where(
        d == "OVER", pd.to_numeric(df["L10 Under"], errors="coerce")
    )
    l5avg = pd.to_numeric(df["Last 5 Avg"], errors="coerce")
    line = pd.to_numeric(df["Line"], errors="coerce")
    rows = []
    for i, r in df.iterrows():
        side = str(r["Direction"]).upper()
        if float(l5.loc[i] or 0) < 5 or float(l10.loc[i] or 0) < 8:
            continue
        if "goblin" not in str(r["Pick Type"]).lower() or side != "OVER":
            continue
        prop = canon_prop("CFB", str(r["Prop"]))
        if prop not in {"pass_yds", "rush_yds", "rec_yds"}:
            continue
        rec = {
            "sport": "CFB",
            "Direction": side,
            "side": side,
            "pick_type": "Goblin",
            "Pick Type": "Goblin",
            "Prop": r["Prop"],
            "prop": r["Prop"],
        }
        if cfb_ticket_fade(rec) or not cfb_list_eligible(rec):
            continue
        skip = cfb_pass_yards_goblin_skip_d(rec)
        if not skip and not d_ok(side, str(r.get("Def Tier") or "")):
            continue
        avg, ln = l5avg.loc[i], line.loc[i]
        if pd.isna(avg) or pd.isna(ln):
            continue
        cover = float(avg) - float(ln)
        need = 15 if prop == "pass_yds" else 8
        if cover < need:
            continue
        label = {
            "pass_yds": "Pass Yards",
            "rush_yds": "Rush Yards",
            "rec_yds": "Rec Yards",
        }[prop]
        rows.append(
            {
                "player": str(r["Player"]),
                "team": str(r["Team"]).upper(),
                "prop": label,
                "prop_type": label,
                "line": float(r["Line"]),
                "l5": float(l5.loc[i]),
                "l10": float(l10.loc[i]),
                "cover": cover,
                "pick_type": "goblin",
                "direction": "OVER",
                "sport": "CFB",
                "search": str(r["Player"]),
            }
        )
    best: dict = {}
    for r in rows:
        k = (r["player"], r["prop"])
        if k not in best or r["line"] < best[k]["line"]:
            best[k] = r
    return sorted(best.values(), key=lambda z: (-z["l10"], -z["cover"], z["player"]))


def switch_yard_filter(frame, page, prop: str) -> None:
    if prop == "Rec Yards":
        if not P._switch_board_filter(frame, page, "Receiving Yards"):
            P._switch_board_filter(frame, page, "Rec Yards")
    else:
        P._switch_board_filter(frame, page, prop)


def probe_live(page, clears: list[dict]) -> list[dict]:
    live = []
    P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
    for rec in clears:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        switch_yard_filter(frame, page, rec["prop"])
        page.wait_for_timeout(400)
        react_search(page, "")
        page.wait_for_timeout(40)
        react_search(page, rec["search"])
        page.wait_for_timeout(1000)
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        pn = _norm(rec["player"])
        hits = [
            c
            for c in cards
            if (
                pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
            )
            and yard_prop_match(rec["prop"], str(c.get("prop_type") or ""))
        ]
        if not hits:
            print(f"MISS  {rec['player']} {rec['prop']} {rec['line']}", flush=True)
            continue
        card0 = hits[0]
        P.cycle_card_to_pick_type(
            frame,
            card0.get("more_btn"),
            player=rec["player"],
            prop=card0.get("prop_type") or rec["prop"],
            want_pick="goblin",
            want_line=rec["line"],
            require_line=True,
            more_index=card0.get("more_index"),
        )
        page.wait_for_timeout(350)
        frame = P.find_prizepicks_frame(page)
        exact = None
        for c in P.get_all_cards(frame):
            if not (
                pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
            ):
                continue
            if str(c.get("pick_type") or "").lower() != "goblin":
                continue
            if not yard_prop_match(rec["prop"], str(c.get("prop_type") or "")):
                continue
            if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                exact = c
                break
        if exact:
            print(
                f"LIVE  {rec['player']:24} {rec['prop']:12} {rec['line']} "
                f"L10={rec['l10']:.0f} ({rec['team']})",
                flush=True,
            )
            live.append(rec)
        else:
            print(
                f"MOVED {rec['player']:24} {rec['prop']:12} want={rec['line']}",
                flush=True,
            )
    return live


def pack(live: list[dict]) -> list[dict]:
    remaining = list(live)
    tickets: list[dict] = []
    while len(remaining) >= 3:
        n = 4 if (len(tickets) >= 2 and len(remaining) >= 4) else 3
        if n > len(remaining):
            n = 3
        chunk: list[dict] = []
        used_p: set[str] = set()
        used_t: set[str] = set()
        for r in remaining:
            if r["player"] in used_p:
                continue
            if r["team"] in used_t and len(chunk) < n - 1:
                continue
            chunk.append(r)
            used_p.add(r["player"])
            used_t.add(r["team"])
            if len(chunk) == n:
                break
        if len(chunk) < n:
            for r in remaining:
                if r["player"] in used_p:
                    continue
                chunk.append(r)
                used_p.add(r["player"])
                used_t.add(r["team"])
                if len(chunk) == n:
                    break
        if len(chunk) < 3 or len({c["team"] for c in chunk}) < 2:
            remaining = remaining[1:] + remaining[:1]
            if len(tickets) > 15:
                break
            continue
        mode = "flex" if len(chunk) == 4 else "power"
        n_mode = 1 + sum(1 for t in tickets if t["mode"] == mode)
        tid = f"{'F4' if mode == 'flex' else 'P3'}-{n_mode}"
        tickets.append({"id": tid, "mode": mode, "legs": chunk})
        taken = {c["player"] for c in chunk}
        remaining = [r for r in remaining if r["player"] not in taken]
    return tickets


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for leg in legs:
        print(
            f"  {leg['player']} ({leg['team']}) {leg['prop']} {leg['line']}",
            flush=True,
        )
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        switch_yard_filter(frame, page, leg["prop"])
        page.wait_for_timeout(550)
        react_search(page, "")
        page.wait_for_timeout(60)
        react_search(page, leg["search"])
        page.wait_for_timeout(1200)
        frame = P.find_prizepicks_frame(page)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(frame)
            except Exception:
                pass
            return {
                "id": tid,
                "ready": False,
                "n": 0,
                "px": None,
                "fails": [leg["player"]],
            }
        time.sleep(0.2)
    try:
        label = "Flex Play" if mode == "flex" else "Power Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(
            timeout=2000
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_cfb_{tid.lower()}_ready_2026-09-26.png"
    try:
        page.screenshot(path=str(shot), full_page=False)
    except Exception:
        pass
    return {
        "id": tid,
        "ready": ready,
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["player"], x["prop"], x["line"], x["team"]) for x in legs],
        "raw": raw[:500],
    }


def main() -> int:
    clears = load_goblin_clears()
    print(f"step8 clears={len(clears)}", flush=True)
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = next(
            (
                pg
                for pg in ctx.pages
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
            ),
            None,
        )
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1500)
        page.bring_to_front()
        live = probe_live(page, clears)
        print(f"\nlive exact Goblin pool={len(live)}", flush=True)
        tickets = pack(live)
        print(f"packed={[t['id'] for t in tickets]}", flush=True)
        if not tickets:
            OUT.write_text(
                json.dumps({"live": live, "tickets": [], "results": []}, indent=2),
                encoding="utf-8",
            )
            print("Not enough live Goblins for Power 3")
            return 2
        boards = [page]
        while len(boards) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(800)
            boards.append(pg)
            print(f"tab {len(boards)}", flush=True)
        results = []
        for pg, ticket in zip(boards, tickets):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]}
                )
        for pg, r in zip(boards, results):
            if r.get("ready"):
                pg.bring_to_front()
                print(f"front {r['id']}", flush=True)
                break
        payload = {
            "live": [
                {
                    "player": x["player"],
                    "team": x["team"],
                    "prop": x["prop"],
                    "line": x["line"],
                    "l10": x["l10"],
                }
                for x in live
            ],
            "tickets": [
                {
                    "id": t["id"],
                    "mode": t["mode"],
                    "legs": [
                        {
                            "player": l["player"],
                            "team": l["team"],
                            "prop": l["prop"],
                            "line": l["line"],
                        }
                        for l in t["legs"]
                    ],
                }
                for t in tickets
            ],
            "results": results,
        }
        OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
                flush=True,
            )
        print(f"wrote {OUT}", flush=True)
        print("No submit.", flush=True)
        return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
