"""For key L5=5 clears, cycle each live card to find Goblin yard face still on PP."""
from __future__ import annotations

import sys
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
from utils.pp_manual_slip import react_search, _norm, prop_ok
from utils.prop_norm import canon_prop

CDP = "http://127.0.0.1:9222"
STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx"
)


def d_ok(side: str, od: str) -> bool:
    o = str(od).lower()
    if side == "OVER":
        return ("weak" in o) or ("below" in o)
    if side == "UNDER":
        return ("elite" in o) or ("above" in o)
    return False


def load_goblin_clears():
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
                "cp": prop,
            }
        )
    best = {}
    for r in rows:
        k = (r["player"], r["prop"])
        if k not in best or r["line"] < best[k]["line"]:
            best[k] = r
    return sorted(best.values(), key=lambda z: (-z["l10"], -z["cover"]))


def main() -> int:
    clears = load_goblin_clears()
    print(f"clears={len(clears)}", flush=True)
    with sync_playwright() as p:
        b = p.chromium.connect_over_cdp(CDP, timeout=60_000)
        ctx = b.contexts[0]
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
            page.goto(
                "https://app.prizepicks.com/board?league_id=15",
                wait_until="domcontentloaded",
                timeout=60_000,
            )
            page.wait_for_timeout(1500)
        page.bring_to_front()
        P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
        found = []
        moved = []
        missing = []
        for rec in clears:
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            filt = rec["prop"]
            if filt == "Rec Yards":
                # try both chip labels
                if not P._switch_board_filter(frame, page, "Receiving Yards"):
                    P._switch_board_filter(frame, page, "Rec Yards")
            else:
                P._switch_board_filter(frame, page, filt)
            page.wait_for_timeout(450)
            react_search(page, "")
            page.wait_for_timeout(40)
            react_search(page, rec["player"])
            page.wait_for_timeout(1000)
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            pn = _norm(rec["player"])
            # any yard card for player
            hits = []
            for c in cards:
                cn = _norm(c.get("player"))
                if pn not in cn and cn not in pn:
                    continue
                if not prop_ok(rec["prop"], str(c.get("prop_type") or "")):
                    # Rec Yards alias
                    cp = _norm(c.get("prop_type"))
                    wp = _norm(rec["prop"])
                    if not (
                        ("rec" in wp and "yard" in wp and "rec" in cp and "yard" in cp)
                        or ("pass" in wp and "yard" in wp and "pass" in cp and "yard" in cp)
                        or ("rush" in wp and "yard" in wp and "rush" in cp and "yard" in cp)
                    ):
                        continue
                hits.append(c)
            if not hits:
                missing.append(rec)
                print(f"MISS {rec['player']} {rec['prop']} {rec['line']}", flush=True)
                continue
            # try cycle to goblin at wanted line
            card0 = hits[0]
            more = card0.get("more_btn")
            P.cycle_card_to_pick_type(
                frame,
                more,
                player=rec["player"],
                prop=card0.get("prop_type") or rec["prop"],
                want_pick="goblin",
                want_line=rec["line"],
                require_line=True,
                more_index=card0.get("more_index"),
            )
            page.wait_for_timeout(350)
            frame = P.find_prizepicks_frame(page)
            cards2 = P.get_all_cards(frame)
            live_g = None
            any_g = None
            for c in cards2:
                cn = _norm(c.get("player"))
                if pn not in cn and cn not in pn:
                    continue
                if str(c.get("pick_type") or "").lower() != "goblin":
                    continue
                cp = _norm(c.get("prop_type"))
                wp = _norm(rec["prop"])
                if not (
                    ("rec" in wp and "yard" in wp and "rec" in cp and "yard" in cp)
                    or ("pass" in wp and "yard" in wp and "pass" in cp and "yard" in cp)
                    or ("rush" in wp and "yard" in wp and "rush" in cp and "yard" in cp)
                ):
                    continue
                any_g = c
                if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                    live_g = c
                    break
            if live_g:
                found.append(rec)
                print(
                    f"LIVE {rec['player']} {rec['prop']} {rec['line']} L10={rec['l10']}",
                    flush=True,
                )
            else:
                alt = None if any_g is None else any_g.get("line")
                moved.append({**rec, "live_goblin": alt, "shown": hits[0].get("line"), "shown_pick": hits[0].get("pick_type")})
                print(
                    f"MOVED {rec['player']} want={rec['line']} shown={hits[0].get('line')}/{hits[0].get('pick_type')} live_g={alt}",
                    flush=True,
                )
        print(
            f"\nFOUND {len(found)} MOVED {len(moved)} MISS {len(missing)}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
