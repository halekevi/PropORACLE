"""Rebuild CFB tabs as Flex 4–6 from live L5=5 Goblins only. Combine pool."""
from __future__ import annotations

import json
import re
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
from utils.pp_manual_slip import add_cycled_leg, prop_ok, _norm
from utils.prop_norm import canon_prop

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board?league_id=15"
# Prefer full board with Last 5 Avg (post-fetch parquet). Thin xlsx lacks cover cols.
STEP8_CANDIDATES = [
    Path(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.parquet"),
    Path(r"H:/PropORACLE/outputs/2026-09-26/cfb/step8_cfb_direction_clean_2026-09-26.xlsx"),
    Path(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx"),
]
OUT = _REPO / "data" / "reports" / "cfb_sat_flex46_2026-09-26.json"

# Already on user's open upcoming slips — do not reuse on new F4/F5/F6
USED = {
    ("jaylenraynor", "pass"),
    ("katinhouser", "pass"),
    ("camedwards", "rush"),
    ("emmettmosley", "rec"),
    ("terrancecarter", "rec"),
    ("archmanning", "pass"),
    ("juliansayin", "pass"),
}


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


def _col(df: pd.DataFrame, *names: str):
    for n in names:
        if n in df.columns:
            return n
    lower = {str(c).lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    raise KeyError(names)


def load_step8() -> pd.DataFrame:
    for p in STEP8_CANDIDATES:
        if not p.exists():
            continue
        df = pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_excel(p)
        if "Last 5 Avg" not in df.columns and "last5_avg" not in {
            str(c).lower() for c in df.columns
        }:
            print(f"skip thin {p} (no Last 5 Avg)", flush=True)
            continue
        print(f"using {p} rows={len(df)}", flush=True)
        return df
    raise FileNotFoundError("no full step8 with Last 5 Avg")


def is_used(player: str, prop: str) -> bool:
    pn = _norm(player)
    for a in ("pass", "rush", "rec"):
        if a not in _norm(prop):
            continue
        for up, ua in USED:
            if ua == a and (up in pn or pn in up or pn.startswith(up[:8])):
                return True
    return False


def load_l5eq5_goblins() -> list[dict]:
    df = load_step8()
    c_dir = _col(df, "Direction", "direction")
    c_l5o = _col(df, "L5 Over", "l5_over")
    c_l5u = _col(df, "L5 Under", "l5_under")
    c_l10o = _col(df, "L10 Over", "l10_over")
    c_l10u = _col(df, "L10 Under", "l10_under")
    c_pt = _col(df, "Pick Type", "pick_type")
    c_prop = _col(df, "Prop", "prop")
    c_player = _col(df, "Player", "player")
    c_team = _col(df, "Team", "team")
    c_line = _col(df, "Line", "line")
    c_od = _col(df, "Def Tier", "Opp Def", "def_tier")
    c_avg = _col(df, "Last 5 Avg", "last5_avg", "L5 Avg")

    d = df[c_dir].astype(str).str.upper()
    l5 = pd.to_numeric(df[c_l5o], errors="coerce").where(
        d == "OVER", pd.to_numeric(df[c_l5u], errors="coerce")
    )
    l10 = pd.to_numeric(df[c_l10o], errors="coerce").where(
        d == "OVER", pd.to_numeric(df[c_l10u], errors="coerce")
    )
    l5avg = pd.to_numeric(df[c_avg], errors="coerce")
    line = pd.to_numeric(df[c_line], errors="coerce")
    rows = []
    for i, r in df.iterrows():
        if "goblin" not in str(r[c_pt]).lower():
            continue
        if str(r[c_dir]).upper() != "OVER":
            continue
        if float(l5.loc[i] or 0) < 5:  # L5=5 only
            continue
        if float(l10.loc[i] or 0) < 8:
            continue
        prop = canon_prop("CFB", str(r[c_prop]))
        if prop not in {"pass_yds", "rush_yds", "rec_yds"}:
            continue
        rec = {
            "sport": "CFB",
            "Direction": "OVER",
            "side": "OVER",
            "pick_type": "Goblin",
            "Pick Type": "Goblin",
            "Prop": r[c_prop],
            "prop": r[c_prop],
        }
        if cfb_ticket_fade(rec) or not cfb_list_eligible(rec):
            continue
        skip = cfb_pass_yards_goblin_skip_d(rec)
        if not skip and not d_ok("OVER", str(r[c_od] if c_od else "")):
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
                "player": str(r[c_player]),
                "team": str(r[c_team]).upper(),
                "prop": label,
                "prop_type": label,
                "line": float(r[c_line]),
                "l5": float(l5.loc[i]),
                "l10": float(l10.loc[i]),
                "cover": cover,
                "pick_type": "goblin",
                "direction": "OVER",
                "side": "OVER",
                "sport": "CFB",
                "search": str(r[c_player]),
            }
        )
    best: dict = {}
    for r in rows:
        k = (r["player"], r["prop"])
        if k not in best or r["line"] < best[k]["line"]:
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
    P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
    for rec in clears:
        hits = hidden_search(page, rec["player"])
        if not hits:
            print(f"MISS {rec['player']} {rec['prop']} {rec['line']}", flush=True)
            continue
        start = next(
            (c for c in hits if yard_ok(rec["prop"], str(c.get("prop_type") or ""))),
            hits[0],
        )
        exact = any(
            yard_ok(rec["prop"], str(c.get("prop_type") or ""))
            and str(c.get("pick_type") or "").lower() == "goblin"
            and P._line_key(c.get("line")) == P._line_key(rec["line"])
            for c in hits
        )
        if not exact:
            frame = P.find_prizepicks_frame(page)
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
            hits = hidden_search(page, rec["player"])
            exact = any(
                yard_ok(rec["prop"], str(c.get("prop_type") or ""))
                and str(c.get("pick_type") or "").lower() == "goblin"
                and P._line_key(c.get("line")) == P._line_key(rec["line"])
                for c in hits
            )
        if exact:
            flag = "USED" if is_used(rec["player"], rec["prop"]) else "FREE"
            print(
                f"LIVE {flag} L5=5 {rec['player']:24} {rec['prop']:12} {rec['line']} "
                f"L10={rec['l10']:.0f} ({rec['team']})",
                flush=True,
            )
            live.append({**rec, "flag": flag})
        else:
            print(f"MOVED {rec['player']} want={rec['line']}", flush=True)
    return live


def _take_chunk(remaining: list[dict], n: int) -> list[dict] | None:
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
    if len(chunk) < n or len({c["team"] for c in chunk}) < 2:
        return None
    return chunk


def pack_flex_46(live: list[dict]) -> list[dict]:
    """FREE faces only → F4 + F5/F6 (or F4+F4). Unique players. 2+ teams."""
    remaining = [x for x in live if x.get("flag") != "USED"]
    tickets: list[dict] = []
    # Prefer a pair: F6+F4 / F5+F4 / F4+F4; else single largest Flex 4–6
    while len(remaining) >= 4:
        n_left = len(remaining)
        if n_left >= 10:
            sizes = [6, 4]
        elif n_left >= 9:
            sizes = [5, 4]
        elif n_left >= 8:
            sizes = [4, 4]
        elif n_left >= 6:
            sizes = [6]
        elif n_left >= 5:
            sizes = [5]
        else:
            sizes = [4]
        progressed = False
        for n in sizes:
            chunk = _take_chunk(remaining, n)
            if chunk is None:
                continue
            tid = f"F{len(chunk)}-{len(tickets) + 1}"
            tickets.append({"id": tid, "mode": "flex", "legs": chunk})
            taken = {c["player"] for c in chunk}
            remaining = [r for r in remaining if r["player"] not in taken]
            progressed = True
            break
        if not progressed:
            break
    return tickets


def add_leg(page, leg: dict) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    hits = hidden_search(page, leg["player"])
    if not hits:
        hits = hidden_search(page, leg["player"].split()[-1])
    if not hits:
        print(f"NO HITS {leg['player']}", flush=True)
        return False
    start = next(
        (c for c in hits if yard_ok(leg["prop"], str(c.get("prop_type") or ""))),
        hits[0],
    )
    already = any(
        yard_ok(leg["prop"], str(c.get("prop_type") or ""))
        and str(c.get("pick_type") or "").lower() == "goblin"
        and P._line_key(c.get("line")) == P._line_key(leg["line"])
        for c in hits
    )
    if not already:
        P.cycle_card_to_pick_type(
            frame,
            start.get("more_btn"),
            player=leg["player"],
            prop=start.get("prop_type") or leg["prop"],
            want_pick="goblin",
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
    return True


def n_correct_from_raw(raw: str, n: int) -> float | None:
    # Prefer "N correct pays Xx" — never 1st place
    m = re.search(rf"{n}\s*correct\s*pays\s*([\d.]+)\s*x", raw, re.I)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+)\s*correct\s*pays\s*([\d.]+)\s*x", raw, re.I)
    if m:
        return float(m.group(2))
    return None


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} flex n={len(legs)} ========", flush=True)
    for leg in legs:
        print(
            f"  L5=5 {leg['player']} ({leg['team']}) {leg['prop']} {leg['line']}",
            flush=True,
        )
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, "CFB", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "flex")
    for leg in legs:
        if not add_leg(page, leg):
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "n": 0, "fails": [leg["player"]]}
        time.sleep(0.15)
    try:
        P.find_prizepicks_frame(page).get_by_text("Flex Play", exact=True).first.click(
            timeout=2500
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type="flex"
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_from_raw(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
    print(raw[:450], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_cfb_{tid.lower()}_flex_2026-09-26.png"
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
        "raw": raw[:700],
    }


def main() -> int:
    clears = load_l5eq5_goblins()
    print(f"step8 L5=5 goblin yard clears={len(clears)}", flush=True)
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [
            pg
            for pg in ctx.pages
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
        ]
        if not boards:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            boards = [pg]
        page = boards[0]
        page.bring_to_front()
        page.set_viewport_size({"width": 1400, "height": 900})
        live = probe_live(page, clears)
        free_n = sum(1 for x in live if x.get("flag") != "USED")
        used_n = len(live) - free_n
        print(
            f"\nlive L5=5 Goblins={len(live)} used_on_open={used_n} free={free_n}",
            flush=True,
        )
        tickets = pack_flex_46(live)
        print(f"packed {[t['id'] for t in tickets]}", flush=True)
        for t in tickets:
            print(
                f"  {t['id']}: "
                + " | ".join(f"{l['player']} {l['line']}" for l in t["legs"]),
                flush=True,
            )
        if not tickets:
            print("Not enough live L5=5 for a Flex 4")
            return 2
        # Rebuild ALL board tabs for these combined slips
        while len(boards) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(900)
            boards.append(pg)
            print(f"opened tab {len(boards)}", flush=True)
        # Use first N boards; clear old P3s
        results = []
        for pg, ticket in zip(boards[: len(tickets)], tickets):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "fails": [str(e)], "n": 0}
                )
        for pg, r in zip(boards, results):
            if r.get("ready"):
                pg.bring_to_front()
                break
        OUT.write_text(
            json.dumps(
                {
                    "live": [
                        {
                            "player": x["player"],
                            "team": x["team"],
                            "prop": x["prop"],
                            "line": x["line"],
                            "l5": x["l5"],
                            "l10": x["l10"],
                        }
                        for x in live
                    ],
                    "tickets": [
                        {
                            "id": t["id"],
                            "mode": "flex",
                            "n": len(t["legs"]),
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
        print("No submit. Flex only. L5=5 Goblin only.", flush=True)
        return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
