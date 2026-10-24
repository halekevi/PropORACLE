#!/usr/bin/env python3
"""Live PP Goblin Pass Yards faces for Saturday audit QBs + 2026 L5 re-test."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.cushion_z import l5_clears
from utils.pp_manual_slip import react_search
from scripts.espn_boxscore_cache import load_boxscore_cache

QBS = [
    "Michael Hawkins Jr.",
    "Conner Weigman",
    "Josh Hoover",
    "Alberto Mendoza",
    "Trinidad Chambliss",
    "Jayden Maiava",
    "Drew Mestemaker",
    "JC French IV",
]
# 2026 L5 from audit (current5)
CUR5 = {
    "Michael Hawkins Jr.": [464, 219, 227, 132, 190],
    "Conner Weigman": [343, 242, 239, 239, 283],
    "Josh Hoover": [296, 263, 251, 201, 273],
    "Alberto Mendoza": [335, 218, 268, 249, 221],
    "Trinidad Chambliss": [241, 213, 298, 363, 225],
    "Jayden Maiava": [301, 245, 326, 277, 261],
    "Drew Mestemaker": [365, 304, 389, 324, 317],
    "JC French IV": [389, 153, 165, 309, 245],
}
WANT = {
    "Michael Hawkins Jr.": 124.5,
    "Conner Weigman": 149.5,
    "Josh Hoover": 149.5,
    "Alberto Mendoza": 149.5,
    "Trinidad Chambliss": 159.5,
    "Jayden Maiava": 174.5,
    "Drew Mestemaker": 224.5,
    "JC French IV": 149.5,
}


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=60_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "online-metrix" not in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if not page:
            print("ERROR: no PP tab")
            return 1
        page.bring_to_front()
        P.navigate_board_for_sport(page, "CFB", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)

        survivors = []
        print("LIVE Pass Yards Goblin faces vs 2026 L5")
        print("=" * 72)
        for player in QBS:
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            try:
                react_search(page, "")
                page.wait_for_timeout(200)
                react_search(page, player)
                page.wait_for_timeout(900)
            except Exception:
                pass
            frame = P.find_prizepicks_frame(page)
            P._switch_board_filter(frame, page, "Pass Yards")
            page.wait_for_timeout(500)
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
            if not hit:
                # last-name fallback
                last = player.split()[-1].replace(".", "")
                if last.lower() in {"jr", "iv", "iii"}:
                    last = player.split()[-2]
                hit = [c for c in cards if last.lower() in str(c.get("player") or "").lower()]
            if not hit:
                print(f"{player}  → NOT ON PASS YARDS BOARD")
                print()
                continue

            # cycle all faces; collect goblins
            goblins = []
            seen = set()
            btn = hit[0].get("more_btn")
            idx = hit[0].get("more_index")
            for _ in range(12):
                st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) if btn or idx is not None else None
                if not st:
                    cards = P.get_all_cards(frame)
                    hit2 = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
                    if not hit2:
                        break
                    line, pt = hit2[0].get("line"), str(hit2[0].get("pick_type") or "").lower()
                else:
                    line, pt = st.get("line"), str(st.get("pickType") or "").lower()
                key = (pt, line)
                if key in seen:
                    break
                seen.add(key)
                if pt == "goblin" and line is not None:
                    goblins.append(float(line))
                P.cycle_card_to_pick_type(
                    frame, btn, player=player, prop="Pass Yards",
                    want_pick="goblin", want_line=None, require_line=False,
                    more_index=idx, max_clicks=1,
                )
                frame = P.find_prizepicks_frame(page)
                cards = P.get_all_cards(frame)
                hit2 = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
                if hit2:
                    btn, idx = hit2[0].get("more_btn"), hit2[0].get("more_index")
                time.sleep(0.3)

            want = WANT[player]
            vals = CUR5[player]
            print(f"{player}  built Goblin O{want}")
            print(f"  live Goblin faces: {goblins or '(none)'}")
            if not goblins:
                print("  → OUT (no Goblin face)")
                print()
                continue
            # deepest goblin that still clears 2026 L5
            deep_ok = None
            for g in sorted(goblins):
                if l5_clears(vals, g, "OVER"):
                    deep_ok = g
            # also report want status
            want_live = want in goblins
            want_clears = l5_clears(vals, want, "OVER")
            print(f"  2026 L5={vals}  want O{want} live?{want_live} clears?{want_clears}")
            if deep_ok is None:
                print(f"  → OUT_MOVED (no Goblin clears 2026 L5; shallowest={min(goblins)})")
                print()
                continue
            moved = deep_ok != want
            avg = sum(vals) / 5
            ratio = deep_ok / avg
            print(
                f"  → SURVIVOR live Goblin O{deep_ok}  "
                f"{'MOVED up' if deep_ok > want else ('same' if deep_ok == want else 'MOVED down')}  "
                f"line/avg={ratio:.3f}"
            )
            print()
            survivors.append((player, deep_ok, moved, ratio))

        print("=" * 72)
        print(f"SURVIVORS n={len(survivors)}")
        for s in sorted(survivors, key=lambda x: x[3]):
            print(f"  {s[0]} Goblin O{s[1]}  ratio={s[3]:.3f}  moved={s[2]}")
        out = _REPO / "outputs/2026-10-24/cfb/pass_yds_live_survivors.txt"
        out.write_text(
            "\n".join(f"CFB|{p}|Pass Yards|OVER|{ln}|goblin" for p, ln, *_ in survivors),
            encoding="utf-8",
        )
        print(f"Wrote {out}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
