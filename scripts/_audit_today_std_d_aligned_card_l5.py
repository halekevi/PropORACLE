"""Card-window L5 + pre/minutes for today's D-aligned NBA Standard L5=5 faces.

Unders first (Elite|Above Avg D). Overs after (Weak|Below Avg D).
Combos skipped (no single-player card window).

  py -3.14 scripts/_audit_today_std_d_aligned_card_l5.py
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from proporacle.data.table_io import read_table  # noqa: E402
from utils.cushion_z import l5_clears  # noqa: E402
from utils.nba_early_l5 import (  # noqa: E402
    l5_card_window,
    l5_preseason,
    preseason_flag,
    window_mix,
)
from utils.nba_minutes_role import load_nba_box_frame  # noqa: E402

SLATE = "2026-10-23"
STEP8 = _REPO / f"outputs/{SLATE}/nba/step8_all_direction.csv"
OUT = _REPO / f"outputs/{SLATE}/nba/std_d_aligned_card_window.txt"

UNDER_D = {"Elite", "Above Avg"}
OVER_D = {"Weak", "Below Avg"}


def _f(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        if str(x).strip() == "":
            return None
        return float(x)
    except Exception:
        return None


def _s(x) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    return str(x).strip()


def _col(df, *names):
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def _side(row, c_dir) -> str:
    d = _s(row.get(c_dir)).upper()
    if "UNDER" in d:
        return "UNDER"
    if "OVER" in d:
        return "OVER"
    return ""


def _d_pass(side: str, tier: str) -> bool:
    t = _s(tier)
    if side == "UNDER":
        return t in UNDER_D
    if side == "OVER":
        return t in OVER_D
    return False


def main() -> int:
    df = read_table(STEP8)
    c_pt = _col(df, "pick_type", "Pick Type")
    c_prop = _col(df, "prop_type", "Prop", "prop")
    c_l5o = _col(df, "l5_over", "L5 Over")
    c_l5u = _col(df, "l5_under", "L5 Under")
    c_player = _col(df, "player", "Player")
    c_line = _col(df, "line", "Line")
    c_avg = _col(df, "stat_last5_avg", "l5_avg", "Intel L5 Avg")
    c_team = _col(df, "team", "Team")
    c_opp = _col(df, "opp_team", "opponent", "opp", "Opp")
    c_clean = _col(df, "clean_minutes", "Clean Minutes")
    c_starter = _col(df, "starter_flag", "Starter Flag")
    c_starter_pre = _col(df, "starter_flag_preseason", "Starter Pre")
    c_min_tier = _col(df, "minutes_tier", "Minutes Tier", "min_tier_l5")
    c_def = _col(df, "DEF_TIER", "def_tier", "Opp Def")
    c_dir = _col(df, "final_bet_direction", "bet_direction", "side")
    c_combo = _col(df, "is_combo_player")

    pt = df[c_pt].astype(str)
    mask_std = pt.str.contains("Standard", case=False, na=False)
    sub = df.loc[mask_std].copy()
    sub["_side"] = sub.apply(lambda r: _side(r, c_dir), axis=1)
    sub["_l5o"] = sub[c_l5o].map(_f)
    sub["_l5u"] = sub[c_l5u].map(_f)
    sub["_line"] = sub[c_line].map(_f)
    sub["_def"] = sub[c_def].map(_s) if c_def else ""
    sub["_prop"] = sub[c_prop].map(_s)
    sub["_player"] = sub[c_player].map(_s)

    # directional L5=5
    ok_l5 = (
        ((sub["_side"] == "OVER") & (sub["_l5o"] == 5.0))
        | ((sub["_side"] == "UNDER") & (sub["_l5u"] == 5.0))
    )
    sub = sub.loc[ok_l5 & sub["_line"].notna()].copy()
    # D-aligned
    sub = sub[sub.apply(lambda r: _d_pass(r["_side"], r["_def"]), axis=1)].copy()
    # drop combos ('0'/'1' strings — bool('0') is True, so don't astype(bool))
    name_combo = sub["_player"].str.contains(r"\+", regex=True, na=False)
    if c_combo:
        flag_combo = sub[c_combo].map(
            lambda x: str(x).strip().lower() in {"1", "true", "yes", "y"}
        )
        sub = sub.loc[~(flag_combo | name_combo)].copy()
    else:
        sub = sub.loc[~name_combo].copy()

    # Unders first, then Overs; within side by gap (L5avg distance)
    if c_avg:
        sub["_avg"] = sub[c_avg].map(_f)
    else:
        sub["_avg"] = None
    sub["_gap"] = sub.apply(
        lambda r: (
            (r["_avg"] - r["_line"])
            if r["_side"] == "OVER" and r["_avg"] is not None
            else (
                (r["_line"] - r["_avg"])
                if r["_side"] == "UNDER" and r["_avg"] is not None
                else 0.0
            )
        ),
        axis=1,
    )
    sub["_ord"] = sub["_side"].map({"UNDER": 0, "OVER": 1}).fillna(2)
    sub = sub.sort_values(["_ord", "_gap"], ascending=[True, False])

    print(f"D-aligned Standard L5=5 (non-combo): n={len(sub)}")
    print("Loading NBA box frame…")
    boxes = load_nba_box_frame()

    lines: list[str] = []
    lines.append(f"NBA Standard D-aligned L5=5 card-window audit  slate={SLATE}")
    lines.append(f"Pool: Standard + directional L5=5 + D pass; combos excluded. n={len(sub)}")
    lines.append("UNDER D = Elite|Above Avg; OVER D = Weak|Below Avg.")
    lines.append("Standard 3-leg Power @ 6x BE ≈ 55%/leg; 2-leg @ 3x ≈ 58%. 84.5% is Goblin-only.")
    lines.append("Unders listed first. Survivors = card window L5=5 on the play side.")
    lines.append("=" * 88)

    survivors: list[dict] = []
    for i, (_, r) in enumerate(sub.iterrows(), 1):
        player = r["_player"]
        prop = r["_prop"]
        side = r["_side"]
        line = float(r["_line"])
        team = _s(r.get(c_team))
        opp = _s(r.get(c_opp)) if c_opp else ""
        clean = _s(r.get(c_clean)) if c_clean else ""
        starter = _s(r.get(c_starter)) if c_starter else ""
        starter_pre = _s(r.get(c_starter_pre)) if c_starter_pre else starter
        min_tier = _s(r.get(c_min_tier)) if c_min_tier else ""
        tier = r["_def"]
        avg = r["_avg"]

        card = l5_card_window(boxes, player, prop, n=5)
        pre = l5_preseason(boxes, player, prop, as_of=SLATE, board_team=team)
        pre_flag = preseason_flag(
            boxes, player, prop, line, side, as_of=SLATE, board_team=team
        )

        if card is None:
            card_ok = False
            card_vals: list = []
            card_dates: list = []
            mix = ""
            card_hits = None
        else:
            card_vals = [g["val"] for g in card]
            card_dates = [g["date"] for g in card]
            mix = window_mix(card)
            card_ok = bool(l5_clears(card_vals, line, side))
            if side == "UNDER":
                card_hits = sum(1 for v in card_vals if v < line)
            else:
                card_hits = sum(1 for v in card_vals if v > line)

        pre_vals = [g["val"] for g in pre] if pre else []
        pre_dates = [g["date"] for g in pre] if pre else []
        pre_mix = window_mix(pre) if pre else ""

        status = (
            "CARD_OK"
            if card_ok
            else ("NO_CARD_WINDOW" if card is None else f"CARD_{card_hits}/5")
        )
        side_ch = "U" if side == "UNDER" else "O"
        avg_s = f"{avg:.1f}" if avg is not None else "?"

        if card_ok:
            survivors.append(
                {
                    "player": player,
                    "prop": prop,
                    "side": side,
                    "side_ch": side_ch,
                    "line": line,
                    "team": team,
                    "opp": opp,
                    "pre_flag": pre_flag,
                    "clean": clean,
                    "starter_pre": starter_pre or starter,
                    "mix": mix,
                    "tier": tier,
                    "gap": float(r["_gap"] or 0),
                    "card_vals": card_vals,
                    "card_dates": card_dates,
                }
            )

        lines.append(
            f"{i:>2}. {player}  {prop}  {side_ch}{line}  ({team} vs {opp})  "
            f"L5avg={avg_s}  D={tier}  → {status}"
        )
        if card is not None:
            vals_s = ", ".join(f"{d}:{v:g}" for d, v in zip(card_dates, card_vals))
            lines.append(
                f"    card[{mix}]  [{vals_s}]  clears L5=5 {side}? {card_ok}"
            )
        else:
            lines.append("    card: MISSING (<5 RS+playoff boxes with minutes>0)")
        if pre_flag == "pre_retired":
            lines.append("    pre[pre_retired]  (≥5 current-season RS — not shown)")
        elif pre_vals:
            pre_s = ", ".join(f"{d}:{v:g}" for d, v in zip(pre_dates, pre_vals))
            lines.append(f"    pre[{pre_flag}/{pre_mix}]  [{pre_s}]")
        else:
            lines.append(f"    pre[{pre_flag}]  (none)")
        lines.append(
            f"    Starter Pre={starter_pre or starter or '?'}  "
            f"Clean Minutes={clean or '?'}  min_tier={min_tier or '?'}"
        )
        lines.append("")

    lines.append("=" * 88)
    lines.append(f"SURVIVORS (card window L5=5 on play side): n={len(survivors)}")
    # one per game for suggested 3-leg: prefer Unders, then gap
    under_s = [s for s in survivors if s["side"] == "UNDER"]
    over_s = [s for s in survivors if s["side"] == "OVER"]
    for label, group in (("UNDER", under_s), ("OVER", over_s)):
        lines.append(f"--- {label} survivors n={len(group)} ---")
        for s in group:
            vals = ", ".join(f"{v:g}" for v in s["card_vals"])
            lines.append(
                f"  {s['player']}  {s['prop']}  {s['side_ch']}{s['line']}  "
                f"({s['team']} vs {s['opp']})  D={s['tier']}  pre={s['pre_flag']}  "
                f"StarterPre={s['starter_pre'] or '?'}  CleanMin={s['clean'] or '?'}  "
                f"mix={s['mix']}  card=[{vals}]"
            )

    # suggest distinct-game 3-leg (Unders preferred)
    picked: list[dict] = []
    used_games: set[str] = set()

    def _game_key(s: dict) -> str:
        teams = sorted({s["team"].upper(), s["opp"].upper()} - {""})
        return "|".join(teams) if teams else s["team"]

    for s in under_s + over_s:
        gk = _game_key(s)
        if gk in used_games:
            continue
        used_games.add(gk)
        picked.append(s)
        if len(picked) >= 3:
            break

    lines.append("")
    lines.append("SUGGESTED 3-leg Standard Power (one per game, Unders first):")
    if len(picked) < 3:
        lines.append(f"  only {len(picked)} distinct-game survivors — not enough for 3-leg")
    for s in picked:
        lines.append(
            f"  {s['player']} | {s['prop']} | {s['side']} | {s['line']} | "
            f"{s['team']} vs {s['opp']} | D={s['tier']} | mix={s['mix']}"
        )

    text = "\n".join(lines)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text, encoding="utf-8")
    print(text)
    print(f"\nWrote {OUT}")

    # machine-readable pick for slip builder
    pick_path = OUT.with_name("std_d_aligned_suggested_3leg.txt")
    pick_path.write_text(
        "\n".join(
            f"NBA|{s['player']}|{s['prop']}|{s['side']}|{s['line']}|standard"
            for s in picked
        ),
        encoding="utf-8",
    )
    print(f"Wrote {pick_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
