"""Card-window L5 + pre/minutes flags for today's top 30 Points Goblins.

Ranks by line/L5avg (lowest = deepest Goblin). Prints card 5 values+dates,
window_mix, pre_ flag, Starter Pre, Clean Minutes.

  py -3.14 scripts/_audit_today_points_goblin_card_l5.py
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from proporacle.data.table_io import read_table  # noqa: E402
from utils.cushion_z import l5_clears  # noqa: E402
from utils.nba_early_l5 import (  # noqa: E402
    format_side_by_side,
    l5_card_window,
    l5_preseason,
    preseason_flag,
    window_mix,
)
from utils.nba_minutes_role import load_nba_box_frame  # noqa: E402

SLATE = "2026-10-23"
STEP8 = _REPO / f"outputs/{SLATE}/nba/step8_all_direction.csv"
OUT = _REPO / f"outputs/{SLATE}/nba/points_goblin_top30_card_window.txt"
TOP_N = 30


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


def main() -> int:
    df = read_table(STEP8)
    c_pt = _col(df, "pick_type", "Pick Type")
    c_prop = _col(df, "prop_type", "Prop", "prop")
    c_l5o = _col(df, "l5_over", "L5 Over")
    c_player = _col(df, "player", "Player")
    c_line = _col(df, "line", "Line")
    c_avg = _col(df, "stat_last5_avg", "l5_avg", "Intel L5 Avg")
    c_team = _col(df, "team", "Team")
    c_opp = _col(df, "opponent", "opp", "Opp")
    c_clean = _col(df, "clean_minutes", "Clean Minutes")
    c_starter = _col(df, "starter_flag", "starter_flag_preseason", "Starter Flag")
    c_starter_pre = _col(df, "starter_flag_preseason", "Starter Pre")
    c_min_tier = _col(df, "minutes_tier", "Minutes Tier", "min_tier_l5")

    pt = df[c_pt].astype(str)
    prop = df[c_prop].astype(str)
    l5o = df[c_l5o].map(_f)
    mask = (
        pt.str.contains("Goblin", case=False, na=False)
        & prop.str.lower().isin(["points", "pts"])
        & (l5o == 5.0)
    )
    sub = df.loc[mask].copy()
    sub["_line"] = sub[c_line].map(_f)
    sub["_avg"] = sub[c_avg].map(_f)
    sub = sub[sub["_line"].notna() & sub["_avg"].notna() & (sub["_avg"] > 0)]
    sub["_ratio"] = sub["_line"] / sub["_avg"]
    # one row per player+line (keep deepest / lowest ratio if dup lines)
    sub = sub.sort_values("_ratio")
    # unique players for top 30 shortlist candidates — still show top 30 rows by ratio
    # (user asked top 30 Points Goblins; may include multiple lines per player)
    top = sub.head(TOP_N).copy()

    # Acuff duplicate note
    acuff = df[df[c_player].astype(str).str.contains("Acuff", case=False, na=False)]
    acuff_names = sorted({_s(x) for x in acuff[c_player].tolist()})

    print("Loading NBA box frame…")
    boxes = load_nba_box_frame()

    lines: list[str] = []
    lines.append(f"NBA Points Goblin L5=5 card-window audit  slate={SLATE}")
    lines.append(f"Pool size (sheet L5_over==5 Points Goblin): {len(sub)}")
    lines.append(f"Showing top {TOP_N} by line/L5avg (lowest = deepest Goblin).")
    lines.append("")
    lines.append("84.5% BAR CLARIFICATION:")
    lines.append("  That bar is per-leg break-even for short all-Goblin Power at logged quotes:")
    lines.append("    2-leg @ 1.40x median  → 1.40^(-1/2) ≈ 84.5%")
    lines.append("    3-leg @ 1.65x (25th pct scrape) → 1.65^(-1/3) ≈ 84.6%")
    lines.append("  It is NOT a 6-leg bar. 6-leg Power @ 10.5x → ≈67.6%/leg; Flex 6 @ 17x → ≈62.4%.")
    lines.append("  So a ~76% Points cell can clear a 6-leg Power BE; it does not clear 2-/3-leg Goblin Power.")
    lines.append("  Prefer Points over Assists (Oct 20–22 graded: Pts ~78% Wlow~71%; Ast ~68.5% Wlow~55%).")
    lines.append("")
    if acuff_names:
        lines.append(f"ACUFF DUP NOTE: board has {acuff_names} — treat as one player; do not double-count.")
        lines.append("")
    lines.append(
        "SHORTLIST ONLY — card window re-check below. Survivors = card L5=5 clears "
        "+ (optional) clean_minutes / starter context."
    )
    lines.append("=" * 88)

    survivors = []
    for i, (_, r) in enumerate(top.iterrows(), 1):
        player = _s(r.get(c_player))
        line = float(r["_line"])
        avg = float(r["_avg"])
        ratio = float(r["_ratio"])
        team = _s(r.get(c_team))
        opp = _s(r.get(c_opp))
        clean = _s(r.get(c_clean)) if c_clean else ""
        starter = _s(r.get(c_starter)) if c_starter else ""
        starter_pre = _s(r.get(c_starter_pre)) if c_starter_pre else starter
        min_tier = _s(r.get(c_min_tier)) if c_min_tier else ""

        card = l5_card_window(boxes, player, "Points", n=5)
        pre = l5_preseason(boxes, player, "Points", as_of=SLATE, board_team=team)
        pre_flag = preseason_flag(
            boxes, player, "Points", line, "OVER", as_of=SLATE, board_team=team
        )

        if card is None:
            card_ok = False
            card_vals = []
            card_dates = []
            mix = ""
            card_hits = None
        else:
            card_vals = [g["val"] for g in card]
            card_dates = [g["date"] for g in card]
            mix = window_mix(card)
            card_ok = bool(l5_clears(card_vals, line, "OVER"))
            card_hits = sum(1 for v in card_vals if v > line)

        pre_vals = [g["val"] for g in pre] if pre else []
        pre_dates = [g["date"] for g in pre] if pre else []
        pre_mix = window_mix(pre) if pre else ""

        status = "CARD_OK" if card_ok else ("NO_CARD_WINDOW" if card is None else f"CARD_{card_hits}/5")
        if card_ok:
            survivors.append(
                {
                    "player": player,
                    "line": line,
                    "avg": avg,
                    "ratio": ratio,
                    "team": team,
                    "opp": opp,
                    "pre_flag": pre_flag,
                    "clean": clean,
                    "starter_pre": starter_pre,
                    "mix": mix,
                }
            )

        lines.append(
            f"{i:>2}. {player}  Points O{line}  ({team} vs {opp})  "
            f"L5avg={avg:.1f}  line/avg={ratio:.3f}  → {status}"
        )
        if card is not None:
            vals_s = ", ".join(f"{d}:{v:g}" for d, v in zip(card_dates, card_vals))
            lines.append(f"    card[{mix}]  [{vals_s}]  clears L5=5 OVER? {card_ok}")
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
    lines.append(f"SURVIVORS (card window L5=5 OVER): n={len(survivors)}")
    for s in survivors:
        lines.append(
            f"  {s['player']}  O{s['line']}  ({s['team']} vs {s['opp']})  "
            f"ratio={s['ratio']:.3f}  pre={s['pre_flag']}  "
            f"StarterPre={s['starter_pre'] or '?'}  CleanMin={s['clean'] or '?'}  "
            f"mix={s['mix']}"
        )
    if not survivors:
        lines.append("  (none — sheet L5=5 did not hold on card window for this top-30 cut)")

    text = "\n".join(lines)
    OUT.write_text(text, encoding="utf-8")
    print(text)
    print(f"\nWrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
