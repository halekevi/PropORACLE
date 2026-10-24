#!/usr/bin/env python3
"""Live PP face + card-window check for tonight's 6 Points Goblins."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.cushion_z import l5_clears
from utils.nba_early_l5 import l5_card_window, l5_preseason, preseason_flag, window_mix
from utils.nba_minutes_role import load_nba_box_frame

SLATE = "2026-10-23"
# built faces
WANT = [
    ("Brandon Williams", 4.5),
    ("Collin Sexton", 5.5),
    ("Yves Missi", 3.5),
    ("Kel'el Ware", 4.5),
    ("Sam Hauser", 3.5),
    ("Jamal Shead", 0.5),
]


def cycle_faces(frame, page, player: str) -> list[tuple[float | None, str]]:
    cards = P.get_all_cards(frame)
    hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
    if not hit:
        hit = [
            c
            for c in cards
            if player.split()[-1].lower() in str(c.get("player") or "").lower()
            and player.split()[0][:4].lower()
            in str(c.get("player") or "").lower().replace("'", "")
        ]
    if not hit:
        return []
    c0 = hit[0]
    btn = c0.get("more_btn")
    idx = c0.get("more_index")
    seen: list[tuple[float | None, str]] = []
    for _ in range(10):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) if btn or idx is not None else None
        if not st:
            cards = P.get_all_cards(frame)
            hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
            if not hit:
                break
            face = (hit[0].get("line"), str(hit[0].get("pick_type") or "standard").lower())
        else:
            face = (st.get("line"), str(st.get("pickType") or "standard").lower())
        if face in seen:
            break
        seen.append(face)
        # advance
        matched = P.cycle_card_to_pick_type(
            frame,
            btn,
            player=player,
            prop="Points",
            want_pick="goblin",
            want_line=None,
            require_line=False,
            more_index=idx,
            max_clicks=1,
        )
        # rebind
        frame = P.find_prizepicks_frame(page)
        cards = P.get_all_cards(frame)
        hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
        if hit:
            btn = hit[0].get("more_btn")
            idx = hit[0].get("more_index")
        time.sleep(0.35)
        _ = matched
    return seen


def main() -> int:
    print("Loading boxes…")
    boxes = load_nba_box_frame()

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
        if page is None:
            print("ERROR: no PrizePicks tab — launch CDP Chrome first")
            return 1
        page.bring_to_front()
        P.navigate_board_for_sport(page, "NBA", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        P._switch_board_filter(frame, page, "Points")
        page.wait_for_timeout(900)

        survivors: list[dict] = []
        print()
        print(f"LIVE CHECK  slate={SLATE}  built 6 Points Goblins")
        print("=" * 88)

        for player, want_line in WANT:
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            # search helps deep benches
            try:
                from utils.pp_manual_slip import react_search

                react_search(page, "")
                page.wait_for_timeout(300)
                react_search(page, player)
                page.wait_for_timeout(1000)
            except Exception:
                pass
            frame = P.find_prizepicks_frame(page)
            P._switch_board_filter(frame, page, "Points")
            page.wait_for_timeout(500)
            frame = P.find_prizepicks_frame(page)

            cards = P.get_all_cards(frame)
            hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
            if not hit:
                print(f"{player}  want Goblin O{want_line}  → NOT ON POINTS BOARD")
                print()
                continue

            # find exact goblin want
            exact = P.cycle_card_to_pick_type(
                frame,
                hit[0].get("more_btn"),
                player=player,
                prop="Points",
                want_pick="goblin",
                want_line=want_line,
                require_line=True,
                more_index=hit[0].get("more_index"),
            )
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
            live_line = hit[0].get("line") if hit else None
            live_pick = str(hit[0].get("pick_type") or "").lower() if hit else ""

            # any goblin face
            any_g = P.cycle_card_to_pick_type(
                frame,
                hit[0].get("more_btn") if hit else None,
                player=player,
                prop="Points",
                want_pick="goblin",
                want_line=None,
                require_line=False,
                more_index=hit[0].get("more_index") if hit else None,
            )
            gob_line = any_g.get("line") if any_g else None
            gob_pick = str(any_g.get("pick_type") or "") if any_g else ""

            moved = exact is None
            test_line = float(want_line) if not moved else (
                float(gob_line) if gob_line is not None else None
            )

            card = l5_card_window(boxes, player, "Points", n=5)
            pre = l5_preseason(boxes, player, "Points", as_of=SLATE)
            if card is None:
                print(f"{player}  want Goblin O{want_line}")
                print(f"  live: pick={live_pick} line={live_line}  goblin_any={gob_line}")
                print(f"  MOVED={moved}  card: MISSING")
                print()
                continue

            vals = [g["val"] for g in card]
            dates = [g["date"] for g in card]
            mix = window_mix(card)
            vals_s = ", ".join(f"{d}:{v:g}" for d, v in zip(dates, vals))

            if test_line is None:
                print(f"{player}  want Goblin O{want_line}")
                print(f"  live: no goblin face found (showing {live_pick} {live_line})")
                print(f"  card[{mix}] [{vals_s}]")
                print("  → OUT (no Goblin face)")
                print()
                continue

            clears = bool(l5_clears(vals, test_line, "OVER"))
            hits = sum(1 for v in vals if v > test_line)
            pre_flag = preseason_flag(
                boxes, player, "Points", test_line, "OVER", as_of=SLATE
            )
            status = "OK_UNCHANGED" if not moved and clears else (
                "OK_MOVED_CLEARS" if moved and clears else (
                    "OUT_MOVED_FAILS" if moved and not clears else "OUT_FAILS"
                )
            )
            print(f"{player}  want Goblin O{want_line}")
            print(
                f"  live Goblin: {gob_line}  "
                f"(board showing {live_pick} {live_line})  "
                f"exact O{want_line}? {exact is not None}"
            )
            print(f"  card[{mix}] [{vals_s}]  vs O{test_line} → {hits}/5 clears={clears}  pre={pre_flag}")
            print(f"  → {status}")
            print()
            if clears and gob_line is not None:
                survivors.append(
                    {
                        "player": player,
                        "line": float(gob_line if moved else want_line),
                        "want": want_line,
                        "moved": moved,
                        "mix": mix,
                        "vals": vals,
                    }
                )

        print("=" * 88)
        print(f"SURVIVORS for slip: n={len(survivors)}")
        for s in survivors:
            tag = "MOVED" if s["moved"] else "same"
            print(
                f"  {s['player']} Goblin O{s['line']} ({tag})  mix={s['mix']}  "
                f"vals={s['vals']}"
            )

        # write legs for builder
        out = _REPO / f"outputs/{SLATE}/nba/six_points_live_survivors.txt"
        out.write_text(
            "\n".join(
                f"NBA|{s['player']}|Points|OVER|{s['line']}|goblin"
                for s in survivors
            ),
            encoding="utf-8",
        )
        print(f"Wrote {out}")
        return 0 if survivors else 2


if __name__ == "__main__":
    raise SystemExit(main())
