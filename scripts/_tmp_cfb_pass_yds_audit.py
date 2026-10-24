#!/usr/bin/env python3
"""Pass Yards Goblin audit card — 2026 L5 dates + PRIOR_SEASON_FILL explain."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO)]

from scripts.espn_boxscore_cache import load_boxscore_cache

SLATE = "2026-10-24"
# deepest cushion faces from user list
WANT = [
    ("Michael Hawkins Jr.", "WVU", 124.5),
    ("Conner Weigman", "HOU", 149.5),
    ("Josh Hoover", "IND", 149.5),
    ("Alberto Mendoza", "GT", 149.5),
    ("Trinidad Chambliss", "MISS", 159.5),
    ("Jayden Maiava", "USC", 174.5),
    ("Drew Mestemaker", "OKST", 224.5),
    ("JC French IV", "CIN", 149.5),
]


def _norm(s: str) -> str:
    return "".join(ch for ch in str(s).lower() if ch.isalnum())


def main() -> int:
    rows, src = load_boxscore_cache("cfb", str(_REPO / "data/cache/cfb_boxscore_cache.csv"))
    print(f"box cache src={src} n={len(rows)}")

    step = pd.read_csv(_REPO / f"outputs/{SLATE}/cfb/step5b_cfb.csv", low_memory=False)
    tip = step[
        step["start_time"].astype(str).str.startswith(SLATE)
        & step["prop_type"].astype(str).str.contains("Pass Yard", case=False, na=False)
        & (step["pick_type"].astype(str).str.lower() == "goblin")
    ].copy()

    lines: list[str] = []
    lines.append(f"CFB Pass Yards Goblin audit  slate={SLATE}")
    lines.append(
        "PRIOR_SEASON_FILL = <8 current-season games in store (pads L8/L10)."
    )
    lines.append(
        "Face L5 can still be all-2026 when current_stat_g1..g5 are filled."
    )
    lines.append("One QB per game. Rush group left off.")
    lines.append("")

    for player, team, want_line in WANT:
        hit = tip[
            (tip["player"].astype(str) == player)
            & (pd.to_numeric(tip["line"], errors="coerce") == want_line)
        ]
        if hit.empty:
            hit = tip[tip["player"].astype(str) == player]
            hit = hit.sort_values("line")
        if hit.empty:
            msg = f"{player} ({team}) O{want_line} — NOT ON step5b board"
            print(msg)
            lines.append(msg)
            lines.append("")
            continue
        r = hit.iloc[0]
        live_line = float(r["line"])
        face_vals = [float(r[f"stat_g{i}"]) for i in range(1, 6)]
        cur_vals = [
            float(r[f"current_stat_g{i}"])
            for i in range(1, 11)
            if pd.notna(r.get(f"current_stat_g{i}"))
        ]
        flag = str(r.get("season_l5_flag") or "")
        sample_n = r.get("l5_sample_n")
        games_used = r.get("games_used")
        l10c = r.get("l10_current_n")
        opp = str(r.get("opp_team_abbr") or r.get("pp_opp_team") or "")
        tip_s = str(r.get("start_time") or "")[:16].replace("T", " ")
        avg = sum(face_vals) / 5
        ratio = live_line / avg if avg else None
        face_all_cur = face_vals == cur_vals[:5]
        clears = all(v > live_line for v in face_vals)
        clears_cur5 = (
            len(cur_vals) >= 5 and all(v > live_line for v in cur_vals[:5])
        )

        # 2026 logs from box cache
        pn = _norm(player)
        box = [
            x
            for x in rows
            if _norm(x.get("player_norm") or "") == pn
            or _norm(x.get("player_norm") or "").startswith(pn[:10])
            or pn in _norm(x.get("player_norm") or "")
        ]
        # also match last+first tokens
        if not box:
            toks = [t for t in player.replace(".", "").split() if t.lower() not in {"jr", "iv", "iii", "ii"}]
            last = toks[-1].lower() if toks else ""
            first = toks[0].lower() if toks else ""
            box = [
                x
                for x in rows
                if last in str(x.get("player_norm") or "").lower()
                and first[:4] in str(x.get("player_norm") or "").lower()
            ]
        box_2026 = []
        for x in box:
            gd = str(x.get("game_date") or "")[:10]
            if not gd.startswith("2026"):
                continue
            try:
                py = float(x.get("PASS_YDS") or 0)
            except (TypeError, ValueError):
                py = 0.0
            box_2026.append((gd, py, str(x.get("opp_team_abbr") or "")))
        box_2026.sort(key=lambda t: t[0], reverse=True)

        # French rush cross-check
        rush_note = ""
        if "French" in player:
            rush = tip  # noqa — use step5b rush
            rush_df = step[
                (step["start_time"].astype(str).str.startswith(SLATE))
                & (step["player"].astype(str) == player)
                & step["prop_type"].astype(str).str.contains("Rush Yard", case=False, na=False)
                & (step["pick_type"].astype(str).str.lower() == "goblin")
            ]
            if not rush_df.empty:
                rr = rush_df.sort_values("line").iloc[0]
                rv = [float(rr[f"stat_g{i}"]) for i in range(1, 6)]
                rush_note = f"  Rush Goblin O{float(rr['line']):g} L5={rv} (same player — dual-threat QB, not a mismatch)"

        block = []
        block.append(
            f"=== {player} ({team} vs {opp})  tip={tip_s}  want O{want_line:g} live O{live_line:g} ==="
        )
        block.append(
            f"  flag={flag or '(empty — 8+ current)'}  l5_sample_n={sample_n}  "
            f"games_used={games_used}  l10_current_n={l10c}"
        )
        block.append(
            f"  face L5={face_vals}  avg={avg:.1f}  line/avg={ratio:.3f}  "
            f"clears_face={clears}  face==current5={face_all_cur}"
        )
        block.append(f"  current_stat series ({len(cur_vals)}): {cur_vals}")
        block.append(f"  current5 clears O{live_line:g}? {clears_cur5}")
        if box_2026:
            show = box_2026[:8]
            block.append("  2026 PASS_YDS from box (newest first):")
            for gd, py, opp_b in show:
                mark = "  <—" if py in face_vals[:5] or (cur_vals and py in cur_vals[:5]) else ""
                block.append(f"    {gd} vs {opp_b}: {py:g}{mark}")
            # why PRIOR if L5 all current
            if flag == "PRIOR_SEASON_FILL" and len(cur_vals) >= 5:
                block.append(
                    f"  WHY PRIOR_SEASON_FILL: only {len(cur_vals)} current games "
                    f"(<8). Face L5 is current; prior pads L8/L10 only."
                )
        else:
            block.append("  2026 box: NO MATCH — check name")
        if rush_note:
            block.append(rush_note)
        status = "SURVIVOR" if clears_cur5 and len(cur_vals) >= 5 else "HOLD"
        if not clears_cur5:
            status = "OUT_2026_L5"
        block.append(f"  → {status}")
        block.append("")
        text = "\n".join(block)
        print(text)
        lines.extend(block)

    out = _REPO / f"outputs/{SLATE}/cfb/pass_yds_goblin_audit_{SLATE}.txt"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
