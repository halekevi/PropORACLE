"""Search NBA Standard gates on a train window, confirm on a later holdout window.

Reads data/reports/nba_std_season_gap_backtest.csv (run _nba_std_season_gap_backtest.py first).
Conditions are written in the pick's direction (OVER or UNDER). Single conditions
and pairs are tried for Scoring / Non-scoring / All. A gate is reported only if it
clears --train-min on train and is then scored on holdout it never saw.

  py -3.14 scripts/_nba_std_gate_search.py [--split 2026-11-12] [--train-min 0.58]
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCORING = {"pts", "pra", "pr", "pa", "fgm", "3ptmade", "freethrowsmade", "twopointersmade", "fantasy"}
BREAK_EVEN_2PICK = 0.577


def wilson_lo(k: int, n: int, z: float = 1.96) -> float:
    if not n:
        return float("nan")
    p = k / n
    return (p + z * z / (2 * n) - z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n)


def directional(d: pd.DataFrame, side: str) -> pd.DataFrame:
    sgn = 1 if side == "OVER" else -1
    x = pd.DataFrame(index=d.index)
    x["hit"] = d["over_hit"] if side == "OVER" else d["under_hit"]
    x["season_gap"] = sgn * d["rel_gap"]
    x["l5avg_gap"] = sgn * (d["l5_avg"] - d["line"]) / d["line"]
    x["l5_hits"] = d["l5_over"] if side == "OVER" else d["l5_under"]
    x["l10_hits"] = d["l10_over"] if side == "OVER" else d["l10_under"]
    good_d = {"Weak", "Below Avg"} if side == "OVER" else {"Elite", "Above Avg"}
    bad_d = {"Elite", "Above Avg"} if side == "OVER" else {"Weak", "Below Avg"}
    x["d_align"] = d["def_tier"].isin(good_d)
    x["d_against"] = d["def_tier"].isin(bad_d)
    x["min_trend"] = sgn * (d["min_l5"] - d["min_season"])
    return x


CONDS = {
    "season avg 10%+ toward pick": lambda x: x.season_gap >= 0.10,
    "season avg 20%+ toward pick": lambda x: x.season_gap >= 0.20,
    "season avg 10%+ against pick": lambda x: x.season_gap <= -0.10,
    "season avg within 10%": lambda x: x.season_gap.abs() < 0.10,
    "L5 avg 10%+ toward pick": lambda x: x.l5avg_gap >= 0.10,
    "L5 avg 10%+ against pick": lambda x: x.l5avg_gap <= -0.10,
    "L5 5/5": lambda x: x.l5_hits == 5,
    "L5 <= 2/5": lambda x: x.l5_hits <= 2,
    "L5 3-4/5": lambda x: x.l5_hits.between(3, 4),
    "L10 >= 8": lambda x: x.l10_hits >= 8,
    "L10 <= 4": lambda x: x.l10_hits <= 4,
    "L10 5-7": lambda x: x.l10_hits.between(5, 7),
    "D aligned": lambda x: x.d_align,
    "D against": lambda x: x.d_against,
    "minutes trending toward pick (2+)": lambda x: x.min_trend >= 2,
    "minutes trending against pick (2+)": lambda x: x.min_trend <= -2,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--split", default="2026-11-12", help="last train slate (inclusive)")
    ap.add_argument("--train-min", type=float, default=0.58)
    ap.add_argument("--min-n", type=int, default=60)
    args = ap.parse_args()

    d = pd.read_csv(ROOT / "data" / "reports" / "nba_std_season_gap_backtest.csv")
    d = d[(d.push == 0) & (d.l5_n == 5) & (d.l10_n == 10) & (d.line > 0)].copy()
    d["kind"] = d.prop.map(lambda p: "Scoring" if p in SCORING else "Non-scoring")
    train_mask = d.slate <= args.split

    results = []
    for side in ("OVER", "UNDER"):
        x = directional(d, side)
        masks = {k: f(x).fillna(False) for k, f in CONDS.items()}
        combos = [(k,) for k in CONDS] + list(itertools.combinations(CONDS, 2))
        for kind in ("All", "Scoring", "Non-scoring"):
            km = pd.Series(True, index=d.index) if kind == "All" else (d.kind == kind)
            for combo in combos:
                m = km.copy()
                for c in combo:
                    m &= masks[c]
                tr, te = m & train_mask, m & ~train_mask
                ntr, nte = int(tr.sum()), int(te.sum())
                if ntr < args.min_n or nte < 25:
                    continue
                htr = x.hit[tr].mean()
                if htr < args.train_min:
                    continue
                kte = int(x.hit[te].sum())
                kall = int(x.hit[m].sum())
                results.append({
                    "side": side, "kind": kind, "gate": " + ".join(combo),
                    "train_n": ntr, "train_hit": round(100 * htr, 1),
                    "test_n": nte, "test_hit": round(100 * kte / nte, 1),
                    "all_n": int(m.sum()), "all_hit": round(100 * kall / int(m.sum()), 1),
                    "all_low": round(100 * wilson_lo(kall, int(m.sum())), 1),
                })
    r = pd.DataFrame(results)
    print(f"train <= {args.split} | holdout after | {len(d)} legs | candidates passing train: {len(r)}")
    if r.empty:
        return
    held = r[r.test_hit >= 100 * BREAK_EVEN_2PICK].sort_values(["all_low", "all_n"], ascending=False)
    print(f"held up on holdout (>= {BREAK_EVEN_2PICK:.1%}): {len(held)}\n")
    with pd.option_context("display.width", 250, "display.max_colwidth", 80):
        print(held.head(25).to_string(index=False))
        print("\nfailed holdout (top by train):")
        print(r[r.test_hit < 100 * BREAK_EVEN_2PICK].sort_values("train_hit", ascending=False).head(10).to_string(index=False))
    out = ROOT / "data" / "reports" / "nba_std_gate_search.json"
    out.write_text(json.dumps({"split": args.split, "held": held.to_dict("records"), "all": r.to_dict("records")}, indent=2), encoding="utf-8")
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
