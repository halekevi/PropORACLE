#!/usr/bin/env python3
"""Live-check CFB Standard L5=5 D-aligned candidates; write survivors for slip."""
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

# Demons intentionally omitted — no-demons rule.
# Standard Power candidates: D-aligned keep, no Fantasy, no Player TDs.
CANDS = [
    # player, prop, side, want_line, l5_vals
    ("JV Gibson", "Receiving Yards", "OVER", 52.5, [75, 66, 68, 79, 54]),
    ("Dylan Edwards", "Receiving Yards", "OVER", 15.5, [28, 29, 30, 16, 18]),
    ("Dylan Edwards", "Longest Reception", "OVER", 10.5, [17, 24, 30, 19, 18]),
    ("Lloyd Avant", "Rush Attempts", "UNDER", 13.5, [8, 10, 10, 8, 9]),
    ("Preston Rex", "Rush Attempts", "UNDER", 13.5, [10, 6, 8, 4, 1]),
    ("Jeremy Hecklinski", "Pass Attempts", "UNDER", 25.5, [19, 5, 11, 16, 1]),
    ("Jeremy Hecklinski", "Pass Completions", "UNDER", 16.0, [14, 5, 6, 7, 1]),
    ("Dierre Hill Jr.", "Longest Rush", "UNDER", 18.5, [12, 16, 14, 7, 14]),
    ("Rico Scott", "Longest Reception", "UNDER", 26.5, [20, 23, 25, 13, 19]),
    ("Alberto Mendoza", "Longest Completion", "UNDER", 37.5, [33, 21, 31, 26, 22]),
    ("DJ Lagway", "Longest Completion", "UNDER", 43.5, [21, 32, 34, 28, 34]),
]


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

        survivors: list[dict] = []
        print("CFB Standard live check (Demons skipped)")
        print("=" * 72)
        for player, prop, side, want, vals in CANDS:
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
            P._switch_board_filter(frame, page, prop)
            page.wait_for_timeout(500)
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
            if not hit:
                last = [t for t in player.replace(".", "").split() if t.lower() not in {"jr", "iv", "iii"}][-1]
                hit = [c for c in cards if last.lower() in str(c.get("player") or "").lower()]
            if not hit:
                print(f"{player} {prop} {side[0]}{want} → NOT ON BOARD")
                print()
                continue

            matched = P.cycle_card_to_pick_type(
                frame,
                hit[0].get("more_btn"),
                player=player,
                prop=prop,
                want_pick="standard",
                want_line=want,
                require_line=True,
                more_index=hit[0].get("more_index"),
            )
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
            # any standard face for this prop
            any_std = P.cycle_card_to_pick_type(
                frame,
                hit[0].get("more_btn") if hit else None,
                player=player,
                prop=prop,
                want_pick="standard",
                want_line=None,
                require_line=False,
                more_index=hit[0].get("more_index") if hit else None,
            )
            live = float(any_std["line"]) if any_std and any_std.get("line") is not None else None
            exact = matched is not None
            test = want if exact else live
            print(f"{player}  Standard {side[0]}{want}  {prop}")
            print(f"  live Standard: {live}  exact? {exact}")
            print(f"  L5={vals}")
            if test is None:
                print("  → OUT (no Standard face)")
                print()
                continue
            clears = bool(l5_clears(vals, float(test), side))
            hits = sum(
                1
                for v in vals
                if (v > float(test) if side == "OVER" else v < float(test))
            )
            moved = (not exact) or (live is not None and abs(float(live) - want) > 0.01)
            # if moved against us on OVER (line up) or UNDER (line down), retest already done via test=live
            status = (
                "OK_UNCHANGED"
                if exact and clears
                else ("OK_MOVED_CLEARS" if clears else "OUT_MOVED_FAILS")
            )
            print(f"  vs {side[0]}{test} → {hits}/5 clears={clears}  → {status}")
            print()
            if clears and live is not None:
                survivors.append(
                    {
                        "player": player,
                        "prop": prop,
                        "side": side,
                        "line": float(live if moved else want),
                        "want": want,
                        "moved": moved,
                    }
                )

        # one prop per player; prefer deepest / volume
        picked: list[dict] = []
        used = set()
        # prefer: Gibson Rec, Edwards Rec (not longest), Avant rush att, Hecklinski pass att
        prefer = [
            ("JV Gibson", "Receiving Yards"),
            ("Dylan Edwards", "Receiving Yards"),
            ("Lloyd Avant", "Rush Attempts"),
            ("Jeremy Hecklinski", "Pass Attempts"),
            ("Preston Rex", "Rush Attempts"),
            ("Dierre Hill Jr.", "Longest Rush"),
            ("Rico Scott", "Longest Reception"),
            ("Alberto Mendoza", "Longest Completion"),
            ("DJ Lagway", "Longest Completion"),
            ("Dylan Edwards", "Longest Reception"),
            ("Jeremy Hecklinski", "Pass Completions"),
        ]
        by_key = {(s["player"], s["prop"]): s for s in survivors}
        for key in prefer:
            if key in by_key and key[0] not in used:
                picked.append(by_key[key])
                used.add(key[0])
            if len(picked) >= 5:
                break

        print("=" * 72)
        print(f"SURVIVORS raw={len(survivors)}  picked(1/player)={len(picked)}")
        for s in picked:
            print(
                f"  {s['player']} {s['prop']} {s['side'][0]}{s['line']} "
                f"{'(moved)' if s['moved'] else '(same)'}"
            )
        out = _REPO / "outputs/2026-10-24/cfb/std_live_survivors.txt"
        out.write_text(
            "\n".join(
                f"CFB|{s['player']}|{s['prop']}|{s['side']}|{s['line']}|standard"
                for s in picked
            ),
            encoding="utf-8",
        )
        print(f"Wrote {out}")
        return 0 if len(picked) >= 2 else 2


if __name__ == "__main__":
    raise SystemExit(main())
