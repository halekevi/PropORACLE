#!/usr/bin/env python3
"""Walk-forward logistic regression on nfl_leg_training_rows.csv.

Train weeks 1..N, score week N+1. Never score training rows.
Writes Sports/NFL/data/nfl_leg_model_oos.csv and nfl_leg_model_report.md.
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

_REPO = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(_REPO)]

TRAIN = _REPO / "Sports" / "NFL" / "data" / "nfl_leg_training_rows.csv"
OOS_OUT = _REPO / "Sports" / "NFL" / "data" / "nfl_leg_model_oos.csv"
REPORT = _REPO / "Sports" / "NFL" / "data" / "nfl_leg_model_report.md"

NUM_FEATURES = [
    "l5_hits",
    "l10_hits",
    "min_m_pct",
    "cover_pct",
    "snap_l3",
    "not_elite_off",
]
CAT_FEATURES = [
    "opp_d_tier",
    "pace_tier",
    "pick_type",
    "prop_norm",
    "side",
]
P_TICKET = 0.75


def _bin_label(p: float) -> str:
    if p < 0.50:
        return "<50"
    if p < 0.60:
        return "50-60"
    if p < 0.70:
        return "60-70"
    if p < 0.80:
        return "70-80"
    return "80+"


def _make_model() -> Pipeline:
    num = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    cat = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "onehot",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
            ),
        ]
    )
    pre = ColumnTransformer(
        transformers=[
            ("num", num, NUM_FEATURES),
            ("cat", cat, CAT_FEATURES),
        ]
    )
    clf = LogisticRegression(
        max_iter=2000,
        class_weight=None,
        solver="lbfgs",
    )
    return Pipeline(steps=[("pre", pre), ("clf", clf)])


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["prop_norm"] = out.get("prop_norm", out.get("prop")).astype(str)
    out["pick_type"] = out["pick_type"].astype(str)
    out["side"] = out["side"].astype(str).str.upper()
    out["opp_d_tier"] = out["opp_d_tier"].fillna("").astype(str).replace({"": "UNK"})
    out["pace_tier"] = out["pace_tier"].fillna("").astype(str).replace({"": "UNK"})
    for c in NUM_FEATURES:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["hit"] = pd.to_numeric(out["hit"], errors="coerce").astype(int)
    out["week"] = pd.to_numeric(out["week"], errors="coerce").astype(int)
    out["gate70"] = pd.to_numeric(out.get("gate70"), errors="coerce").fillna(0).astype(int)
    return out


def walk_forward(df: pd.DataFrame) -> pd.DataFrame:
    weeks = sorted(int(w) for w in df["week"].dropna().unique())
    feats = NUM_FEATURES + CAT_FEATURES
    oos_parts: list[pd.DataFrame] = []
    for n in weeks:
        test_week = n + 1
        if test_week not in weeks:
            continue
        train = df[df["week"] <= n]
        test = df[df["week"] == test_week]
        if len(train) < 50 or len(test) < 10:
            print(f"skip train..W{n} -> W{test_week}: train={len(train)} test={len(test)}")
            continue
        # need both classes
        if train["hit"].nunique() < 2:
            print(f"skip W{n}: single class in train")
            continue
        model = _make_model()
        model.fit(train[feats], train["hit"])
        proba = model.predict_proba(test[feats])[:, 1]
        scored = test.copy()
        scored["p_hat"] = proba
        scored["train_through_week"] = n
        scored["test_week"] = test_week
        oos_parts.append(scored)
        print(
            f"train W1..W{n} (n={len(train)}) -> score W{test_week} "
            f"(n={len(test)}) mean_p={proba.mean():.3f} actual={test['hit'].mean():.3f}"
        )
    if not oos_parts:
        raise SystemExit("No OOS folds produced")
    return pd.concat(oos_parts, ignore_index=True)


def direction_accuracy(oos: pd.DataFrame) -> dict[str, Any]:
    """Among player-game-prop-pick pairs with both sides, did higher p match the hit side?"""
    keys = ["slate_date", "player", "prop_norm", "pick_type", "train_through_week"]
    have = oos.copy()
    # only pairs present both ways
    counts = have.groupby(keys)["side"].nunique()
    both = counts[counts >= 2].index
    if len(both) == 0:
        return {"n_pairs": 0, "correct": 0, "accuracy": None, "thin": True}
    sub = have.set_index(keys).loc[both].reset_index()
    correct = 0
    n = 0
    for _, g in sub.groupby(keys):
        if set(g["side"].astype(str).str.upper()) < {"OVER", "UNDER"}:
            continue
        # pick higher p side
        idx = g["p_hat"].idxmax()
        chosen = g.loc[idx]
        # the side that hit: if exactly one hit, that side; if both/neither, skip
        hits = g[g["hit"] == 1]
        if len(hits) != 1:
            continue
        n += 1
        if str(chosen["side"]).upper() == str(hits.iloc[0]["side"]).upper():
            correct += 1
    acc = (correct / n) if n else None
    return {"n_pairs": n, "correct": correct, "accuracy": acc, "thin": n < 30}


def calibration_table(oos: pd.DataFrame) -> pd.DataFrame:
    rows = []
    oos = oos.copy()
    oos["bin"] = oos["p_hat"].map(_bin_label)
    order = ["50-60", "60-70", "70-80", "80+"]
    for b in order:
        g = oos[oos["bin"] == b]
        n = len(g)
        if n == 0:
            rows.append(
                {
                    "bin": b,
                    "n": 0,
                    "mean_predicted": None,
                    "actual_hr": None,
                    "gap_pp": None,
                    "flag": "thin",
                }
            )
            continue
        mp = float(g["p_hat"].mean())
        ah = float(g["hit"].mean())
        gap = abs(mp - ah) * 100
        flag = "thin" if n < 30 else ("ok" if not (b == "70-80" and gap > 5) else "miscal")
        if b == "70-80" and n >= 30 and gap > 5:
            flag = "FAIL_CALIBRATION_>5pp"
        elif n < 30:
            flag = "thin"
        else:
            flag = "ok"
        rows.append(
            {
                "bin": b,
                "n": n,
                "mean_predicted": round(mp, 4),
                "actual_hr": round(ah, 4),
                "gap_pp": round(gap, 2),
                "flag": flag,
            }
        )
    return pd.DataFrame(rows)


def benchmark(oos: pd.DataFrame) -> dict[str, Any]:
    gate = oos[oos["gate70"] == 1]
    model = oos[oos["p_hat"] >= P_TICKET - 1e-12]

    def _hr(df: pd.DataFrame) -> dict[str, Any]:
        n = len(df)
        h = int(df["hit"].sum()) if n else 0
        return {"hits": h, "n": n, "hr": (h / n if n else None), "thin": n < 30}

    out: dict[str, Any] = {
        "gate": _hr(gate),
        "model_p075": _hr(model),
    }
    g_n = out["gate"]["n"]
    m_n = out["model_p075"]["n"]
    out["volume_ratio"] = (m_n / g_n) if g_n else None
    g_hr = out["gate"]["hr"]
    m_hr = out["model_p075"]["hr"]
    hr_vol_ok = False
    if g_hr is not None and m_hr is not None and out["volume_ratio"] is not None:
        hr_vol_ok = (m_hr >= g_hr - 1e-12) and (out["volume_ratio"] >= 0.70)
    out["hr_vol_ok"] = hr_vol_ok
    # Final replace still requires calibration trust (set by caller).
    out["replace_gate"] = False
    # by family
    fam = {}
    for f, g in oos.groupby("prop_family"):
        fam[str(f)] = {
            "gate": _hr(g[g["gate70"] == 1]),
            "model_p075": _hr(g[g["p_hat"] >= P_TICKET - 1e-12]),
        }
    out["by_family"] = fam
    return out


def write_report(oos: pd.DataFrame, cal: pd.DataFrame, bench: dict[str, Any], direction: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# NFL leg probability model - walk-forward report")
    lines.append("")
    lines.append("Model: logistic regression. Train weeks 1..N, score week N+1 only.")
    lines.append(f"Ticket threshold: p >= {P_TICKET}. Demons excluded from training.")
    lines.append("")
    lines.append("## Decision rules (fixed before results)")
    lines.append("")
    lines.append("- Replace gate only if OOS hit rate >= gate **and** volume >= 70% of gate.")
    lines.append("- 70-80% calibration bin must be within 5 points (predicted vs actual).")
    lines.append("- Bins with n < 30 flagged thin.")
    lines.append("- Calibration fail => model is not trusted to replace the gate.")
    lines.append("")
    lines.append("## Calibration")
    lines.append("")
    lines.append("| Bin | n | Mean predicted | Actual HR | gap pp | Flag |")
    lines.append("|---|---:|---:|---:|---:|---|")
    for _, r in cal.iterrows():
        lines.append(
            f"| {r['bin']} | {r['n']} | {r['mean_predicted']} | {r['actual_hr']} | "
            f"{r['gap_pp']} | {r['flag']} |"
        )
    cal_70 = cal[cal["bin"] == "70-80"]
    cal_ok = False
    if len(cal_70):
        row = cal_70.iloc[0]
        cal_ok = (
            int(row["n"]) >= 30
            and row["gap_pp"] is not None
            and float(row["gap_pp"]) <= 5.0 + 1e-9
            and not str(row["flag"]).startswith("FAIL")
        )
    lines.append("")
    lines.append(f"Calibration trust (70-80 within 5pp, n>=30): **{'PASS' if cal_ok else 'FAIL'}**")
    replace_ok = bool(bench.get("hr_vol_ok")) and cal_ok
    bench["replace_gate"] = replace_ok
    lines.append("")
    lines.append("## Model vs gate (same OOS weeks)")
    lines.append("")
    g, m = bench["gate"], bench["model_p075"]
    lines.append("| Rule | hits | n | HR |")
    lines.append("|---|---:|---:|---:|")
    lines.append(
        f"| Gate (L5=5 + L10>=8) | {g['hits']} | {g['n']} | "
        f"{(100*g['hr']):.1f}%{' (thin)' if g['thin'] else ''} |"
        if g["hr"] is not None
        else f"| Gate (L5=5 + L10>=8) | {g['hits']} | {g['n']} | n/a |"
    )
    lines.append(
        f"| Model p>=0.75 | {m['hits']} | {m['n']} | "
        f"{(100*m['hr']):.1f}%{' (thin)' if m['thin'] else ''} |"
        if m["hr"] is not None
        else f"| Model p>=0.75 | {m['hits']} | {m['n']} | n/a |"
    )
    vr = bench.get("volume_ratio")
    lines.append("")
    lines.append(
        f"Volume ratio (model/gate): **{(100*vr):.1f}%**"
        if vr is not None
        else "Volume ratio: n/a"
    )
    lines.append(
        f"HR+volume bar: **{'PASS' if bench.get('hr_vol_ok') else 'FAIL'}**"
    )
    lines.append(
        f"Replace gate?: **{'YES' if replace_ok else 'NO - keep gate; model is packing tiebreaker only'}**"
        + ("" if cal_ok else " (blocked by calibration)")
    )
    lines.append("")
    lines.append(
        "Note: `min_m_pct` was all-null in this baseline (Stat G1-G5 rarely joined); "
        "cover_pct often falls back to projection-line. Odds totals still missing."
    )
    lines.append("")
    lines.append("## By prop family")
    lines.append("")
    lines.append("| Family | Gate hits/n (HR) | Model p>=0.75 hits/n (HR) |")
    lines.append("|---|---|---|")
    for fam in ("pass", "rush", "receiving", "kicking", "tackles"):
        block = (bench.get("by_family") or {}).get(fam)
        if not block:
            lines.append(f"| {fam} | — | — |")
            continue

        def _fmt(b: dict[str, Any]) -> str:
            if not b["n"]:
                return "0/0"
            hr = f"{100*b['hr']:.1f}%" if b["hr"] is not None else "n/a"
            thin = " thin" if b["thin"] else ""
            return f"{b['hits']}/{b['n']} ({hr}{thin})"

        lines.append(f"| {fam} | {_fmt(block['gate'])} | {_fmt(block['model_p075'])} |")
    lines.append("")
    lines.append("## Direction accuracy")
    lines.append("")
    if direction.get("accuracy") is None:
        lines.append("Too few two-sided pairs to tell.")
    else:
        thin = " (thin)" if direction.get("thin") else ""
        lines.append(
            f"Higher-p side matched the unique hitting side on "
            f"**{direction['correct']}/{direction['n_pairs']}** = "
            f"**{100*direction['accuracy']:.1f}%**{thin}."
        )
    lines.append("")
    lines.append("## Fold sizes")
    lines.append("")
    fold = oos.groupby("test_week").size().reset_index(name="n")
    lines.append("| Test week | n |")
    lines.append("|---:|---:|")
    for _, r in fold.iterrows():
        lines.append(f"| {int(r['test_week'])} | {int(r['n'])} |")
    lines.append("")
    lines.append("## Known gaps")
    lines.append("")
    lines.append("- No implied totals / game_total features (Odds API).")
    lines.append("- Injury blank — not used as a feature.")
    lines.append("- Gate proxy here is directional L5=5 + L10>=8 (NFL Goblin ticket flag still off).")
    lines.append("")
    text = "\n".join(lines) + "\n"
    REPORT.write_text(text, encoding="utf-8")
    print(f"WROTE {REPORT}")
    return text


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--train", type=Path, default=TRAIN)
    ap.add_argument("--oos-out", type=Path, default=OOS_OUT)
    args = ap.parse_args()
    if not args.train.exists():
        raise SystemExit(f"Missing training file {args.train} — run build_nfl_leg_training_rows.py first")
    df = _prepare(pd.read_csv(args.train))
    print(f"loaded {args.train} rows={len(df)} weeks={sorted(df['week'].unique().tolist())}")
    oos = walk_forward(df)
    args.oos_out.parent.mkdir(parents=True, exist_ok=True)
    oos.to_csv(args.oos_out, index=False)
    print(f"WROTE {args.oos_out} rows={len(oos)}")
    cal = calibration_table(oos)
    bench = benchmark(oos)
    direction = direction_accuracy(oos)
    text = write_report(oos, cal, bench, direction)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
