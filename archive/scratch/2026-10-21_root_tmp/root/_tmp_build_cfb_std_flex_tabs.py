"""Join step8 L5=5 Standard/Goblin yard clears to live PP faces; pack Flex tabs."""
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
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, prop_ok, _norm
from utils.prop_norm import canon_prop

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board?league_id=15"
STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx"
)
OUT = _REPO / "data" / "reports" / "cfb_sat_std_flex_tabs_2026-09-26.json"


def d_ok(side: str, od: str) -> bool:
    o = str(od).lower()
    if side == "OVER":
        return ("weak" in o) or ("below" in o)
    return ("elite" in o) or ("above" in o)


def yard_ok(want: str, got: str) -> bool:
    if prop_ok(want, got):
        return True
    w, g = _norm(want), _norm(got)
    for a in ("pass", "rush", "rec"):
        if a in w and "yard" in w and a in g and "yard" in g:
            return True
    return False


def load_clears() -> list[dict]:
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
        pt = str(r["Pick Type"])
        low = pt.lower()
        if "demon" in low:
            continue
        if "goblin" in low:
            if side != "OVER":
                continue
            pick = "Goblin"
        elif "standard" in low:
            pick = "Standard"
        else:
            continue
        prop = canon_prop("CFB", str(r["Prop"]))
        if prop not in {"pass_yds", "rush_yds", "rec_yds"}:
            continue
        rec = {
            "sport": "CFB",
            "Direction": side,
            "side": side,
            "pick_type": pt,
            "Pick Type": pt,
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
        cover = (float(avg) - float(ln)) if side == "OVER" else (float(ln) - float(avg))
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
                "od": str(r.get("Def Tier") or ""),
                "pick": pick,
                "pick_type": pick.lower(),
                "direction": side,
                "side": side,
                "sport": "CFB",
                "search": str(r["Player"]),
            }
        )
    best: dict = {}
    for r in rows:
        k = (r["player"], r["prop"], r["pick"], r["side"])
        cur = best.get(k)
        if cur is None:
            best[k] = r
            continue
        if r["side"] == "OVER" and r["line"] < cur["line"]:
            best[k] = r
        elif r["side"] == "UNDER" and r["line"] > cur["line"]:
            best[k] = r
    return sorted(best.values(), key=lambda z: (-z["l10"], -z["cover"], z["player"]))


def hidden_search(page, player: str) -> list[dict]:
    page.evaluate(
        """(q) => {
          const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
          if (!inp) return false;
          const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
          setter.call(inp, '');
          inp.dispatchEvent(new Event('input', {bubbles:true}));
          setter.call(inp, q);
          inp.dispatchEvent(new Event('input', {bubbles:true}));
          inp.dispatchEvent(new Event('change', {bubbles:true}));
          return true;
        }""",
        player,
    )
    page.wait_for_timeout(1400)
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    return [
        c
        for c in cards
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]


def probe_live(page, clears: list[dict]) -> list[dict]:
    live = []
    P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    try:
        P._switch_board_filter(frame, page, "Popular")
    except Exception:
        pass
    for rec in clears:
        hits = hidden_search(page, rec["player"])
        if not hits:
            print(f"MISS {rec['pick'][0]} {rec['side'][0]} {rec['player']} {rec['prop']} {rec['line']}", flush=True)
            continue
        want_pick = rec["pick_type"]
        exact = None
        any_face = None
        for c in hits:
            if not yard_ok(rec["prop"], str(c.get("prop_type") or "")):
                continue
            any_face = c
            if str(c.get("pick_type") or "").lower() != want_pick:
                continue
            if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                exact = c
                break
        if exact is None and want_pick == "standard" and any_face is not None:
            # cycle toward standard exact line
            frame = P.find_prizepicks_frame(page)
            P.cycle_card_to_pick_type(
                frame,
                any_face.get("more_btn"),
                player=rec["player"],
                prop=any_face.get("prop_type") or rec["prop"],
                want_pick="standard",
                want_line=rec["line"],
                require_line=True,
                more_index=any_face.get("more_index"),
            )
            page.wait_for_timeout(350)
            hits2 = hidden_search(page, rec["player"])
            for c in hits2:
                if not yard_ok(rec["prop"], str(c.get("prop_type") or "")):
                    continue
                if str(c.get("pick_type") or "").lower() != "standard":
                    continue
                if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                    exact = c
                    break
        if exact is None and want_pick == "goblin":
            frame = P.find_prizepicks_frame(page)
            start = next(
                (c for c in hits if yard_ok(rec["prop"], str(c.get("prop_type") or ""))),
                hits[0],
            )
            P.cycle_card_to_pick_type(
                frame,
                start.get("more_btn"),
                player=rec["player"],
                prop=start.get("prop_type") or rec["prop"],
                want_pick="goblin",
                want_line=rec["line"],
                require_line=True,
                more_index=start.get("more_index"),
            )
            page.wait_for_timeout(350)
            hits2 = hidden_search(page, rec["player"])
            for c in hits2:
                if not yard_ok(rec["prop"], str(c.get("prop_type") or "")):
                    continue
                if str(c.get("pick_type") or "").lower() != "goblin":
                    continue
                if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                    exact = c
                    break
        if exact:
            print(
                f"LIVE {rec['pick'][0]} {rec['side'][0]} {rec['player']:22} "
                f"{rec['prop']:12} {rec['line']} L10={rec['l10']:.0f} ({rec['team']})",
                flush=True,
            )
            live.append(rec)
        else:
            shown = None if any_face is None else f"{any_face.get('line')}/{any_face.get('pick_type')}"
            print(
                f"MOVED {rec['pick'][0]} {rec['side'][0]} {rec['player']:22} "
                f"want={rec['line']} shown={shown}",
                flush=True,
            )
    return live


def pack(live: list[dict]) -> list[dict]:
    """Prefer Flex 4 Standards; allow Goblin leftovers. Unique players; 2+ teams."""
    # Prefer Standard first in pack order
    pool = sorted(
        live,
        key=lambda z: (0 if z["pick"] == "Standard" else 1, -z["l10"], -z["cover"]),
    )
    remaining = list(pool)
    tickets: list[dict] = []
    while len(remaining) >= 3:
        n = 4 if len(remaining) >= 4 else 3
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
            if len(tickets) > 20:
                break
            continue
        # Flex when any Standard; Power only if all Goblin
        all_g = all(c["pick"] == "Goblin" for c in chunk)
        mode = "power" if all_g and len(chunk) == 3 else "flex"
        if mode == "power" and len(chunk) == 4:
            mode = "flex"
        n_mode = 1 + sum(1 for t in tickets if t["mode"] == mode)
        tid = f"{'P3' if mode == 'power' else 'F4' if len(chunk) == 4 else 'F3'}-{n_mode}"
        tickets.append({"id": tid, "mode": mode, "legs": chunk})
        taken = {c["player"] for c in chunk}
        remaining = [r for r in remaining if r["player"] not in taken]
    return tickets


def add_leg(page, leg: dict) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    try:
        P._switch_board_filter(frame, page, "Popular")
    except Exception:
        pass
    hits = hidden_search(page, leg["player"])
    if not hits:
        print(f"NO HITS {leg['player']}", flush=True)
        return False
    start = next(
        (c for c in hits if yard_ok(leg["prop"], str(c.get("prop_type") or ""))),
        hits[0],
    )
    already = any(
        yard_ok(leg["prop"], str(c.get("prop_type") or ""))
        and str(c.get("pick_type") or "").lower() == leg["pick_type"]
        and P._line_key(c.get("line")) == P._line_key(leg["line"])
        for c in hits
    )
    if not already:
        P.cycle_card_to_pick_type(
            frame,
            start.get("more_btn"),
            player=leg["player"],
            prop=start.get("prop_type") or leg["prop"],
            want_pick=leg["pick_type"],
            want_line=leg["line"],
            require_line=True,
            more_index=start.get("more_index"),
        )
        page.wait_for_timeout(400)
        hidden_search(page, leg["player"])
    frame = P.find_prizepicks_frame(page)
    r = add_cycled_leg(P, frame, page, leg)
    if not r.ok:
        print(r.loud_fail(), flush=True)
        return False
    # For Standard UNDER, ensure under side selected if UI exposes it
    if leg["pick"] == "Standard" and leg["side"] == "UNDER":
        try:
            frame = P.find_prizepicks_frame(page)
            # best-effort: click Under on the last-added slip leg
            frame.get_by_text("Less", exact=True).last.click(timeout=1500)
        except Exception:
            try:
                frame.get_by_text("Under", exact=True).last.click(timeout=1500)
            except Exception:
                pass
    return True


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for leg in legs:
        print(
            f"  {leg['pick'][0]} {leg['side'][0]} {leg['player']} {leg['prop']} {leg['line']}",
            flush=True,
        )
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        if not add_leg(page, leg):
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "n": 0, "fails": [leg["player"]]}
        time.sleep(0.2)
    try:
        label = "Flex Play" if mode == "flex" else "Power Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(
            timeout=2500
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
    print(raw[:450], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_cfb_{tid.lower()}_std_2026-09-26.png"
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
        "legs": [
            (x["pick"], x["side"], x["player"], x["prop"], x["line"]) for x in legs
        ],
        "raw": raw[:600],
    }


def main() -> int:
    clears = load_clears()
    std_n = sum(1 for c in clears if c["pick"] == "Standard")
    gob_n = sum(1 for c in clears if c["pick"] == "Goblin")
    print(f"clears Standard={std_n} Goblin={gob_n}", flush=True)
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
        page.set_viewport_size({"width": 1400, "height": 900})
        live = probe_live(page, clears)
        print(f"\nlive exact pool={len(live)}", flush=True)
        # Prefer building Standard-heavy Flex tickets; keep Goblins as fill
        tickets = pack(live)
        print(f"packed={[t['id'] for t in tickets]}", flush=True)
        for t in tickets:
            print(
                f"  {t['id']}: "
                + ", ".join(
                    f"{l['pick'][0]}{l['side'][0]} {l['player']} {l['line']}"
                    for l in t["legs"]
                ),
                flush=True,
            )
        if not tickets:
            OUT.write_text(json.dumps({"live": live, "tickets": []}, indent=2), encoding="utf-8")
            print("No tickets")
            return 2
        boards = [page]
        while len(boards) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1000)
            boards.append(pg)
            print(f"tab {len(boards)}", flush=True)
        results = []
        for pg, ticket in zip(boards, tickets):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append({"id": ticket["id"], "ready": False, "fails": [str(e)]})
        for pg, r in zip(boards, results):
            if r.get("ready"):
                pg.bring_to_front()
                break
        OUT.write_text(
            json.dumps(
                {
                    "live": [
                        {
                            "pick": x["pick"],
                            "side": x["side"],
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
                                    "pick": l["pick"],
                                    "side": l["side"],
                                    "player": l["player"],
                                    "prop": l["prop"],
                                    "line": l["line"],
                                }
                                for l in t["legs"]
                            ],
                        }
                        for t in tickets
                    ],
                    "results": results,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
                flush=True,
            )
        print(f"wrote {OUT}", flush=True)
        print("No submit.", flush=True)
        return 0 if results and any(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
