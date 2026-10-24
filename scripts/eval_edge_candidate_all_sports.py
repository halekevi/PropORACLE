#!/usr/bin/env python3
"""Per-sport / pick_type / prop holdout: production vs isolated candidate.

Uses each sport's own last 20% by game_date so NBA/NHL get a number even when
the global temporal split is summer-only.

  py -3.14 scripts/eval_edge_candidate_all_sports.py
"""
from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

_REPO = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO / "scripts"
sys.path.insert(0, str(_SCRIPTS))
sys.path.insert(0, str(_REPO))

from train_edge_model import _load_retrain_csv_as_raw, _prepare_features  # noqa: E402

MIN_AUC_N = 50
MIN_PROP_N = 80
MIN_GOBLIN_N = 40


def _auc(y, p) -> float | None:
    y = np.asarray(y)
    p = np.asarray(p, dtype=float)
    if len(y) < MIN_AUC_N or len(np.unique(y)) < 2:
        return None
    try:
        return float(roc_auc_score(y, p))
    except Exception:
        return None


def _corr(y, p) -> float | None:
    p = np.asarray(p, dtype=float)
    y = np.asarray(y, dtype=float)
    if len(y) < MIN_GOBLIN_N or np.std(p) < 1e-9:
        return None
    return float(np.corrcoef(p, y)[0, 1])


def _score(model, feats: list[str], df: pd.DataFrame) -> np.ndarray:
    X = df.copy()
    for c in feats:
        if c not in X.columns:
            X[c] = np.nan
        ser = pd.to_numeric(X[c], errors="coerce")
        med = float(np.nanmedian(ser.to_numpy(dtype=float))) if ser.notna().any() else 0.0
        if np.isnan(med):
            med = 0.0
        X[c] = ser.fillna(med)
    return np.asarray(model.predict_proba(X[feats].astype(float))[:, 1], dtype=float)


def _fmt(a: float | None) -> str:
    return f"{a:.3f}" if a is not None else "  — "


def main() -> int:
    csv_path = _REPO / "data" / "retrain_dataset.csv"
    mdir = _REPO / "models"
    print(f"[eval] loading {csv_path}")
    raw = _load_retrain_csv_as_raw(csv_path, None, source_label=str(csv_path))
    raw = raw.loc[raw["_hit_y"].isin([0.0, 1.0])].copy()
    print(f"[eval] rows={len(raw):,}  building features…")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        feat = _prepare_features(raw, skip_median_fill=True)
    if "y" not in feat.columns:
        feat["y"] = pd.to_numeric(feat["_hit_y"], errors="coerce")
    feat = feat.loc[feat["y"].isin([0.0, 1.0])].copy()
    dt = pd.to_datetime(feat.get("game_date"), errors="coerce")
    if dt.isna().all() and "file_date" in feat.columns:
        dt = pd.to_datetime(feat["file_date"], errors="coerce")
    feat["_dt"] = dt

    prod = joblib.load(mdir / "edge_model_unified.pkl")
    cand = joblib.load(mdir / "edge_model_candidate.pkl")
    prod_feats = json.loads((mdir / "edge_model_features.json").read_text(encoding="utf-8"))
    cand_feats = json.loads((mdir / "edge_model_candidate_features.json").read_text(encoding="utf-8"))

    print(
        "\nPer-sport temporal holdout (last 20% of that sport by date)\n"
        f"{'Sport':<8} {'n_te':>7} {'prod':>7} {'cand':>7} {'dAUC':>7}  "
        f"{'g_n':>6} {'g_prod':>7} {'g_cand':>7} {'g_corrP':>8} {'g_corrC':>8}"
    )
    print("-" * 92)
    prop_rows: list[str] = []
    for sp in sorted(feat["sport"].astype(str).str.upper().unique()):
        sub = feat.loc[feat["sport"].astype(str).str.upper() == sp].copy()
        dated = sub.dropna(subset=["_dt"]).sort_values("_dt", kind="mergesort")
        if len(dated) < 80:
            print(f"{sp:<8} {len(dated):7d}  (skip: too few dated rows)")
            continue
        k = int(len(dated) * 0.8)
        te = dated.iloc[k:]
        y = te["y"].to_numpy()
        p_prod = _score(prod, prod_feats, te)
        p_cand = _score(cand, cand_feats, te)
        a_p, a_c = _auc(y, p_prod), _auc(y, p_cand)
        dlt = (a_c - a_p) if (a_p is not None and a_c is not None) else None
        pt = te.get("pick_type", pd.Series("", index=te.index)).astype(str).str.strip().str.lower()
        g = pt.eq("goblin").to_numpy()
        yg, n_g = y[g], int(g.sum())
        gp, gc = (_auc(yg, p_prod[g]) if n_g else None), (_auc(yg, p_cand[g]) if n_g else None)
        cp, cc = (_corr(yg, p_prod[g]) if n_g else None), (_corr(yg, p_cand[g]) if n_g else None)
        print(
            f"{sp:<8} {len(te):7d} {_fmt(a_p):>7} {_fmt(a_c):>7} {_fmt(dlt):>7}  "
            f"{n_g:6d} {_fmt(gp):>7} {_fmt(gc):>7} {_fmt(cp):>8} {_fmt(cc):>8}"
        )

        prop = te.get("prop", te.get("prop_type", pd.Series("", index=te.index))).astype(str)
        for pr, grp in te.groupby(prop, dropna=False):
            if len(grp) < MIN_PROP_N:
                continue
            m = grp.index
            # align to te positional — use loc
            mask = te.index.isin(m)
            yy = y[mask]
            pp, pc = p_prod[mask], p_cand[mask]
            ap, ac = _auc(yy, pp), _auc(yy, pc)
            if ap is None and ac is None:
                continue
            d = (ac - ap) if (ap is not None and ac is not None) else None
            hr = float(np.mean(yy))
            prop_rows.append(
                f"  {sp:<7} {str(pr)[:28]:<28} n={len(grp):5d} HR={hr:.2f}  "
                f"prod={_fmt(ap)} cand={_fmt(ac)} d={_fmt(d)}"
            )

        for ptk in ("goblin", "standard", "demon"):
            m = pt.eq(ptk).to_numpy()
            if int(m.sum()) < MIN_AUC_N:
                continue
            ap, ac = _auc(y[m], p_prod[m]), _auc(y[m], p_cand[m])
            d = (ac - ap) if (ap is not None and ac is not None) else None
            print(
                f"         {ptk:<9} n={int(m.sum()):5d} HR={float(np.mean(y[m])):.2f}  "
                f"prod={_fmt(ap)} cand={_fmt(ac)} d={_fmt(d)}"
            )

    print("\nProps with n>={0} on that sport's holdout:".format(MIN_PROP_N))
    for line in prop_rows:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
