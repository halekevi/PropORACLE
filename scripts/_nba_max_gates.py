"""NBA "max gates": best filter combination per prop type and side, then today's board legs that pass.

Filters searched (all directional, card-window L5/L10, as-of ranks with no lookahead):
  L5 hits      any | >=4 | =5
  L10 hits     any | >=7 | >=8 | >=9
  season avg   any | right side | right side by >=5% | by >=10%
  prop offense any | favorable | not unfavorable   (player's team rank for that stat)
  prop defense any | favorable | not unfavorable   (opponent allowed rank for that stat)
  minutes      any | not down 2+ | down 2+          (L5 minutes vs season minutes)
Favorable OVER = Elite|Above offense, Below|Weak defense; UNDER is the reverse.

Pick per (book, prop, side): highest Wilson lower bound on train (slates <= TRAIN_END) with
n >= MIN_N; the holdout (later slates) is the honest rate. Observe-only: not wired to tickets.

  py -3.14 scripts/_nba_max_gates.py [--slate 2026-11-28]
"""
from __future__ import annotations

import argparse
import itertools
import json
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "Sports" / "NBA" / "scripts"))
from defense_report import norm_team_abbr  # noqa: E402
from _nba_matchup_tier_backtest import tier, wilson_lo  # noqa: E402
from _nba_prop_matchup_backtest import LEG_TO_RANK, attach_prop, prop_ranks_asof  # noqa: E402
from _nba_std_season_gap_backtest import PROP_COL, SEASON_START, load_box  # noqa: E402

TRAIN_END = "2026-11-12"
MIN_N = {"Standard": 25, "Goblin": 40}
MIN_HOLD_N = 15
# Standard: 2-pick Power at 3x. Goblin bar is below the ~84.5% 2-leg 1.4x break-even; Goblin gates are a ranking aid.
BREAK_EVEN = {"Standard": 57.7, "Goblin": 75.0}
REPORT = ROOT / "data" / "reports" / "nba_max_gates.json"
FAV_OFF = {"OVER": ("Elite", "Above"), "UNDER": ("Below", "Weak")}
FAV_DEF = {"OVER": ("Below", "Weak"), "UNDER": ("Elite", "Above")}

LEVELS = {
    "l5": ["any", ">=4", "=5"],
    "l10": ["any", ">=7", ">=8", ">=9"],
    "season": ["any", "right side", ">=5%", ">=10%"],
    "off": ["any", "favorable", "not unfavorable"],
    "def": ["any", "favorable", "not unfavorable"],
    "min": ["any", "not down 2+", "down 2+"],
}


def masks(d: pd.DataFrame, side: str) -> dict[str, dict[str, np.ndarray]]:
    up = side == "OVER"
    l5 = d.l5_over if up else d.l5_under
    l10 = d.l10_over if up else d.l10_under
    rel = d.rel_gap if up else -d.rel_gap
    off_f, def_f = d.own_off_pt.isin(FAV_OFF[side]), d.opp_def_pt.isin(FAV_DEF[side])
    other = "UNDER" if up else "OVER"
    off_u, def_u = d.own_off_pt.isin(FAV_OFF[other]), d.opp_def_pt.isin(FAV_DEF[other])
    mdelta = d.min_l5 - d.min_season
    t = np.ones(len(d), dtype=bool)
    return {
        "l5": {"any": t, ">=4": (l5 >= 4).values, "=5": (l5 == 5).values},
        "l10": {"any": t, ">=7": (l10 >= 7).values, ">=8": (l10 >= 8).values, ">=9": (l10 >= 9).values},
        "season": {"any": t, "right side": (rel > 0).values, ">=5%": (rel >= 0.05).values, ">=10%": (rel >= 0.10).values},
        "off": {"any": t, "favorable": off_f.values, "not unfavorable": (~off_u).values},
        "def": {"any": t, "favorable": def_f.values, "not unfavorable": (~def_u).values},
        "min": {"any": t, "not down 2+": (mdelta > -2).values, "down 2+": (mdelta <= -2).values},
    }


def search(d: pd.DataFrame, side: str, book: str) -> dict | None:
    hit = (d.over_hit if side == "OVER" else d.under_hit).values.astype(int)
    tr = (d.slate <= TRAIN_END).values
    mk = masks(d, side)
    best = None
    for combo in itertools.product(*[LEVELS[k] for k in LEVELS]):
        m = np.ones(len(d), dtype=bool)
        for k, lv in zip(LEVELS, combo):
            m &= mk[k][lv]
        mt = m & tr
        n = int(mt.sum())
        if n < MIN_N[book]:
            continue
        k_ = int(hit[mt].sum())
        lo = wilson_lo(k_, n)
        if best is None or lo > best["train_low"] + 1e-9:
            best = {"gate": dict(zip(LEVELS, combo)), "train_n": n, "train_hit": 100 * k_ / n, "train_low": lo, "mask": m}
    if best is None:
        return None
    mh = best.pop("mask") & ~tr
    hn = int(mh.sum())
    best["hold_n"] = hn
    best["hold_hit"] = 100 * hit[mh].mean() if hn else None
    best["train_low"] = round(100 * best["train_low"], 1)
    best["train_hit"] = round(best["train_hit"], 1)
    best["hold_hit"] = round(best["hold_hit"], 1) if best["hold_hit"] is not None else None
    base = hit[tr].mean() * 100
    best["base_train"] = round(base, 1)
    best["holds"] = bool(hn >= MIN_HOLD_N and best["hold_hit"] >= BREAK_EVEN[book])
    return best


def load_legs(book: str) -> pd.DataFrame:
    f = "nba_std_season_gap_backtest.csv" if book == "Standard" else "nba_goblin_season_gap_backtest.csv"
    legs = pd.read_csv(ROOT / "data" / "reports" / f)
    legs = legs[(legs.push == 0) & (legs.l5_n == 5) & (legs.l10_n == 10)].copy()
    legs["team_n"] = legs.team.map(norm_team_abbr)
    legs["opp_n"] = legs.opp.map(norm_team_abbr)
    return attach_prop(legs, prop_ranks_asof(sorted(legs.slate.unique())))


def slate_legs(slate: str) -> pd.DataFrame:
    f = ROOT / "outputs" / slate / "nba" / "step4_with_stats.csv"
    b = pd.read_csv(f, low_memory=False)
    b = b[b.pick_type.astype(str).isin(["Standard", "Goblin"]) & b.prop_norm.isin(PROP_COL)]
    if "is_combo_player" in b:
        b = b[pd.to_numeric(b.is_combo_player, errors="coerce").fillna(0).astype(int) == 0]
    b = b[b.game_date.astype(str).str[:10] == slate].copy()
    blank = b.opp_team.isna() | b.opp_team.astype(str).isin(["", "UNKNOWN_OPP", "nan"])
    if blank.any():
        from step2_attach_picktypes import _espn_opponents, _pp_abbr
        espn = _espn_opponents(slate) or {}
        b.loc[blank, "opp_team"] = b.loc[blank, "team"].map(lambda t: espn.get(_pp_abbr(t)))
    b["espn"] = b.ESPN_ATHLETE_ID.astype(str).str.replace(r"\.0$", "", regex=True)
    b["line"] = pd.to_numeric(b.line, errors="coerce")
    b = b.dropna(subset=["line"]).drop_duplicates(["espn", "prop_norm", "line", "pick_type"])
    box = load_box()
    by_player = {k: g.sort_values("game_date") for k, g in box.groupby("espn_athlete_id")}
    rows = []
    for r in b.itertuples(index=False):
        g = by_player.get(r.espn)
        if g is None:
            continue
        col = PROP_COL[r.prop_norm]
        played = g[(g.game_date < slate) & (pd.to_numeric(g.minutes, errors="coerce").fillna(0) > 0)]
        season_vals = pd.to_numeric(played.loc[played.game_date >= SEASON_START, col], errors="coerce").dropna()
        recent = pd.to_numeric(played[col], errors="coerce").dropna().iloc[::-1]
        if len(season_vals) < 3 or len(recent) < 10:
            continue
        l5, l10 = recent.iloc[:5], recent.iloc[:10]
        mins = pd.to_numeric(played.minutes, errors="coerce").dropna().iloc[::-1]
        season = float(season_vals.mean())
        rows.append({
            "slate": slate, "player": r.player, "prop": r.prop_norm, "book": r.pick_type, "line": r.line,
            "team": r.team, "opp": r.opp_team, "season_avg": round(season, 2), "rel_gap": (season - r.line) / r.line,
            "l5_over": int((l5 > r.line).sum()), "l5_under": int((l5 < r.line).sum()),
            "l10_over": int((l10 > r.line).sum()), "l10_under": int((l10 < r.line).sum()),
            "l5_vals": "/".join(f"{v:g}" for v in l5), "last_game": str(played.game_date.iloc[-1]),
            "min_l5": round(float(mins.iloc[:5].mean()), 1),
            "min_season": round(float(pd.to_numeric(played.loc[played.game_date >= SEASON_START, "minutes"], errors="coerce").mean()), 1),
        })
    d = pd.DataFrame(rows)
    d["team_n"] = d.team.map(norm_team_abbr)
    d["opp_n"] = d.opp.map(norm_team_abbr)
    return attach_prop(d, prop_ranks_asof([slate]))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate", default="2026-11-28")
    args = ap.parse_args()
    gates = []
    for book in ("Goblin", "Standard"):
        legs = load_legs(book)
        for prop, grp in legs.groupby("prop"):
            for side in (["OVER"] if book == "Goblin" else ["OVER", "UNDER"]):
                g = search(grp.reset_index(drop=True), side, book)
                if g:
                    gates.append({"book": book, "prop": prop, "side": side, **g})
    gates.sort(key=lambda g: (g["book"], -(g["hold_hit"] or 0)))
    print(f"{'book':<9}{'prop':<22}{'side':<6}{'train':>14}{'holdout':>14}  holds  gate")
    for g in gates:
        gate = ", ".join(f"{k} {v}" for k, v in g["gate"].items() if v != "any") or "(none)"
        hh = f"{g['hold_hit']:.1f}%" if g["hold_hit"] is not None else "-"
        print(f"{g['book']:<9}{g['prop']:<22}{g['side']:<6}{g['train_hit']:>7.1f}% n={g['train_n']:<4}"
              f"{hh:>7} n={g['hold_n']:<4}  {'YES' if g['holds'] else '   '}    {gate}")

    s = slate_legs(args.slate)
    passing = []
    for g in [g for g in gates if g["holds"]]:
        sub = s[(s.book == g["book"]) & (s.prop == g["prop"])].reset_index(drop=True)
        if sub.empty:
            continue
        mk = masks(sub, g["side"])
        m = np.ones(len(sub), dtype=bool)
        for k, lv in g["gate"].items():
            m &= mk[k][lv]
        for r in sub[m].itertuples(index=False):
            passing.append({
                "book": g["book"], "player": r.player, "team": r.team_n, "opp": r.opp_n, "prop": r.prop,
                "side": g["side"], "line": r.line, "season_avg": r.season_avg,
                "l5": r.l5_over if g["side"] == "OVER" else r.l5_under,
                "l10": r.l10_over if g["side"] == "OVER" else r.l10_under,
                "l5_vals": r.l5_vals, "own_off_rank": int(r.own_off_p), "opp_def_rank": int(r.opp_def_p),
                "min_l5": r.min_l5, "min_season": r.min_season, "last_game": r.last_game,
                "gate_hold_hit": g["hold_hit"], "gate_hold_n": g["hold_n"], "gate_train_hit": g["train_hit"],
            })
    p = pd.DataFrame(passing)
    out_csv = ROOT / "outputs" / args.slate / "nba" / f"nba_max_gates_{args.slate}.csv"
    if not p.empty:
        p = p.sort_values(["book", "gate_hold_hit", "player"], ascending=[True, False, True])
        p.to_csv(out_csv, index=False)
    print(f"\nSlate {args.slate}: {len(s)} board legs scored, {len(p)} pass a holding gate -> {out_csv if len(p) else '(none)'}")
    if not p.empty:
        print(p[["book", "player", "team", "opp", "prop", "side", "line", "season_avg", "l5", "l10", "own_off_rank",
                 "opp_def_rank", "gate_hold_hit"]].to_string(index=False))
    REPORT.write_text(json.dumps({"slate": args.slate, "train_end": TRAIN_END, "break_even": BREAK_EVEN,
                                  "gates": gates, "passing": passing}, indent=1, default=str), encoding="utf-8")
    print(f"Saved -> {REPORT}")


if __name__ == "__main__":
    main()
