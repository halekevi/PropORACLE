#!/usr/bin/env python3
"""Goblin-OVER / Standard specialist models vs production and vs L5/cover/D gates.

Compares ranking quality inside each book (not mixed Demon/Goblin difficulty).

  py -3.14 scripts/eval_book_specialist_models.py
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
MIN_CORR_N = 40
COVER_FLOOR = {"WNBA": 2.0, "TENNIS": 2.0, "MLB": 1.0, "SOCCER": 1.0}


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
    if len(y) < MIN_CORR_N or np.std(p) < 1e-9:
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


def _hr(y, mask=None) -> tuple[float | None, int]:
    y = np.asarray(y, dtype=float)
    if mask is not None:
        y = y[np.asarray(mask, dtype=bool)]
    n = int(len(y))
    if n == 0:
        return None, 0
    return float(np.mean(y)), n


def _first_num(df: pd.DataFrame, names: tuple[str, ...]) -> pd.Series:
    out = pd.Series(np.nan, index=df.index, dtype=float)
    for name in names:
        if name not in df.columns:
            continue
        ser = pd.to_numeric(df[name], errors="coerce")
        out = out.where(out.notna(), ser)
    return out


def _attach_gates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dirs = out.get("bet_direction", out.get("direction", pd.Series("", index=out.index)))
    is_over = dirs.astype(str).str.strip().str.upper().eq("OVER")
    if "direction_encoded" in out.columns:
        de = pd.to_numeric(out["direction_encoded"], errors="coerce")
        is_over = is_over | de.eq(1.0)
    l5o = _first_num(out, ("last5_over", "l5_over", "line_hits_over_5"))
    l5u = _first_num(out, ("last5_under", "l5_under", "line_hits_under_5"))
    l5 = l5o.where(is_over, l5u)
    if l5.isna().any() and "hit_rate_L5" in out.columns:
        hr5 = pd.to_numeric(out["hit_rate_L5"], errors="coerce")
        l5 = l5.where(l5.notna(), (hr5 * 5.0).round())
    avg = _first_num(out, ("avg_L5", "stat_last5_avg", "last5_avg"))
    line = _first_num(out, ("line_score", "line"))
    cover = avg - line
    # Feature pipeline stores (avg-line)/line; recover points when raw avg is missing.
    vs = pd.to_numeric(out.get("avg_L5_vs_line"), errors="coerce") if "avg_L5_vs_line" in out.columns else None
    if vs is not None:
        cover = cover.where(cover.notna(), vs * line)
    play_cover = cover.where(is_over, -cover)
    sport = out["sport"].astype(str).str.strip().str.upper()
    floor = sport.map(lambda s: COVER_FLOOR.get(str(s), 1.0))
    dte = pd.to_numeric(out.get("stat_def_tier_encoded", out.get("def_tier_encoded")), errors="coerce")
    if "def_tier_encoded" in out.columns:
        dte = dte.where(dte.notna(), pd.to_numeric(out["def_tier_encoded"], errors="coerce"))
    d_pass = np.where(is_over, dte <= 0.5, dte >= 2.0)
    d_pass = pd.Series(d_pass, index=out.index)
    if "defense_known" in out.columns:
        known = pd.to_numeric(out["defense_known"], errors="coerce").fillna(0.0).eq(1.0)
        d_pass = d_pass.fillna(False) & known
    else:
        d_pass = d_pass.fillna(False) & dte.notna()
    out["_l5"] = l5
    out["_play_cover"] = play_cover
    out["_l5_ge4"] = l5 >= 4
    out["_l5_eq5"] = l5 >= 5
    out["_cover"] = play_cover > 0
    out["_delta"] = play_cover >= floor
    out["_d_pass"] = d_pass
    out["_ticket"] = out["_l5_ge4"] & out["_delta"]
    return out


def _top_decile_hr(y, scores, mask=None, frac: float = 0.10) -> tuple[float | None, int]:
    y = np.asarray(y, dtype=float)
    s = np.asarray(scores, dtype=float)
    if mask is not None:
        m = np.asarray(mask, dtype=bool)
        y, s = y[m], s[m]
    n = int(len(y))
    if n < 20:
        return None, n
    k = max(10, int(round(n * frac)))
    k = min(k, n)
    order = np.argsort(-s, kind="mergesort")
    return float(np.mean(y[order[:k]])), k


def _print_gate_block(title: str, te: pd.DataFrame, y, p_spec, p_prod) -> None:
    print(f"\n=== {title}  n={len(te):,}  base_HR={float(np.mean(y)):.3f} ===")
    dfill = 0.0
    if "stat_def_tier_encoded" in te.columns:
        dfill = float(pd.to_numeric(te["stat_def_tier_encoded"], errors="coerce").notna().mean())
    elif "def_tier_encoded" in te.columns:
        dfill = float(pd.to_numeric(te["def_tier_encoded"], errors="coerce").notna().mean())
    print(
        f"  fill  L5={float(te['_l5'].notna().mean()):.1%}  "
        f"cover={float(te['_play_cover'].notna().mean()):.1%}  "
        f"D={dfill:.1%}"
    )
    auc_s, auc_p = _auc(y, p_spec), _auc(y, p_prod)
    print(f"  ranking AUC   specialist={_fmt(auc_s)}  production={_fmt(auc_p)}")
    print(f"  corr(p, hit)  specialist={_fmt(_corr(y, p_spec))}  production={_fmt(_corr(y, p_prod))}")

    rows = [
        ("all", np.ones(len(te), dtype=bool)),
        ("L5>=4", te["_l5_ge4"].to_numpy()),
        ("L5=5", te["_l5_eq5"].to_numpy()),
        ("L5>=4 + Cover", (te["_l5_ge4"] & te["_cover"]).to_numpy()),
        ("L5>=4 + Delta (ticket floor)", te["_ticket"].to_numpy()),
        ("L5>=4 + D", (te["_l5_ge4"] & te["_d_pass"]).to_numpy()),
        ("L5>=4 + Cover + D", (te["_l5_ge4"] & te["_cover"] & te["_d_pass"]).to_numpy()),
        ("ticket + D", (te["_ticket"] & te["_d_pass"]).to_numpy()),
    ]
    print(f"  {'gate':<32} {'HR':>6} {'n':>7}")
    for lab, mask in rows:
        hr, n = _hr(y, mask)
        print(f"  {lab:<32} {_fmt(hr)} {n:7d}")

    print(f"  {'ranker (top 10%)':<32} {'HR':>6} {'k':>7}")
    for lab, scores, mask in (
        ("spec top-decile (all)", p_spec, None),
        ("prod top-decile (all)", p_prod, None),
        ("spec top-decile | L5>=4", p_spec, te["_l5_ge4"].to_numpy()),
        ("prod top-decile | L5>=4", p_prod, te["_l5_ge4"].to_numpy()),
        ("cover top-decile | L5>=4", te["_play_cover"].to_numpy(), te["_l5_ge4"].to_numpy()),
        ("spec top-decile | ticket", p_spec, te["_ticket"].to_numpy()),
        ("prod top-decile | ticket", p_prod, te["_ticket"].to_numpy()),
    ):
        hr, k = _top_decile_hr(y, scores, mask)
        print(f"  {lab:<32} {_fmt(hr)} {k:7d}")


def _print_sport_table(te: pd.DataFrame, y, p_spec, p_prod) -> None:
    print(f"\n  {'sport':<8} {'n':>6} {'HR':>6} {'L5>=4':>6} {'n4':>6} {'tickHR':>6} {'nt':>6} {'sAUC':>6} {'pAUC':>6} {'sTop4':>6} {'pTop4':>6}")
    for sp in sorted(te["sport"].astype(str).str.upper().unique()):
        m = te["sport"].astype(str).str.upper().eq(sp).to_numpy()
        if int(m.sum()) < 30:
            continue
        hr, n = _hr(y, m)
        l4 = m & te["_l5_ge4"].to_numpy()
        hr4, n4 = _hr(y, l4)
        tick = m & te["_ticket"].to_numpy()
        hrt, nt = _hr(y, tick)
        s_top, _ = _top_decile_hr(y, p_spec, l4)
        p_top, _ = _top_decile_hr(y, p_prod, l4)
        print(
            f"  {sp:<8} {n:6d} {_fmt(hr)} {_fmt(hr4)} {n4:6d} {_fmt(hrt)} {nt:6d} "
            f"{_fmt(_auc(y[m], p_spec[m]))} {_fmt(_auc(y[m], p_prod[m]))} {_fmt(s_top)} {_fmt(p_top)}"
        )


def _unwrap_estimator(model):
    est = model
    cals = getattr(model, "calibrated_classifiers_", None)
    if cals:
        cc0 = cals[0]
        for attr in ("estimator", "base_estimator", "unwrapped_estimator"):
            inner = getattr(cc0, attr, None)
            if inner is not None:
                est = inner
                break
    return est


def _importance(model, feats: list[str], n: int = 12) -> None:
    est = _unwrap_estimator(model)
    imp = getattr(est, "feature_importances_", None)
    if imp is None and hasattr(est, "booster"):
        imp = getattr(est.booster, "feature_importances_", None)
    if imp is None:
        print(f"  (no feature_importances_ on {type(est).__name__})")
        return
    order = np.argsort(-np.asarray(imp))[:n]
    print("  top features:")
    for i in order:
        if i >= len(feats):
            continue
        print(f"    {feats[i]:<28} {float(imp[i]):.4f}")


def _temporal_tail(df: pd.DataFrame, frac: float = 0.20) -> pd.DataFrame:
    dated = df.dropna(subset=["_dt"]).sort_values("_dt", kind="mergesort")
    if dated.empty:
        return dated
    k = int(len(dated) * (1.0 - frac))
    return dated.iloc[k:]


def _eval_book(
    title: str,
    feat: pd.DataFrame,
    spec_path: Path,
    feat_path: Path,
    prod,
    prod_feats: list[str],
) -> None:
    if not spec_path.is_file() or not feat_path.is_file():
        print(f"\n[{title}] missing {spec_path.name} — skip")
        return
    spec = joblib.load(spec_path)
    spec_feats = json.loads(feat_path.read_text(encoding="utf-8"))
    te = _temporal_tail(feat)
    if len(te) < MIN_AUC_N:
        print(f"\n[{title}] too few dated test rows ({len(te)})")
        return
    te = _attach_gates(te)
    y = te["y"].to_numpy(dtype=float)
    p_spec = _score(spec, spec_feats, te)
    p_prod = _score(prod, prod_feats, te)
    _print_gate_block(title, te, y, p_spec, p_prod)
    _print_sport_table(te, y, p_spec, p_prod)
    print("\n  specialist importances")
    _importance(spec, spec_feats)


def main() -> int:
    csv_path = _REPO / "data" / "retrain_dataset.csv"
    mdir = _REPO / "models"
    print(f"[eval] loading {csv_path}")
    prod = joblib.load(mdir / "edge_model_unified.pkl")
    prod_feats = json.loads((mdir / "edge_model_features.json").read_text(encoding="utf-8"))

    goblin_raw = _load_retrain_csv_as_raw(
        csv_path, None, source_label=str(csv_path), pick_type_filter="goblin", direction_filter="OVER"
    )
    std_raw = _load_retrain_csv_as_raw(
        csv_path, None, source_label=str(csv_path), pick_type_filter="standard"
    )
    print("[eval] building features…")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        goblin = _prepare_features(goblin_raw, skip_median_fill=True)
        standard = _prepare_features(std_raw, skip_median_fill=True)
    for name, df in (("goblin", goblin), ("standard", standard)):
        if "y" not in df.columns:
            df["y"] = pd.to_numeric(df["_hit_y"], errors="coerce")
        keep = df["y"].isin([0.0, 1.0])
        df.drop(index=df.index[~keep], inplace=True)
        dt = pd.to_datetime(df.get("game_date"), errors="coerce")
        if dt.isna().all() and "file_date" in df.columns:
            dt = pd.to_datetime(df["file_date"], errors="coerce")
        df["_dt"] = dt
        print(f"[eval] {name}: {len(df):,} rows  dated={int(df['_dt'].notna().sum()):,}")

    _eval_book(
        "Goblin OVER only",
        goblin,
        mdir / "edge_model_goblin_over.pkl",
        mdir / "edge_model_goblin_over_features.json",
        prod,
        prod_feats,
    )
    _eval_book(
        "Standard (both directions)",
        standard,
        mdir / "edge_model_standard.pkl",
        mdir / "edge_model_standard_features.json",
        prod,
        prod_feats,
    )
    for sp, stem in (
        ("WNBA", "edge_model_goblin_over_wnba"),
        ("MLB", "edge_model_goblin_over_mlb"),
    ):
        sub = goblin.loc[goblin["sport"].astype(str).str.upper().eq(sp)].copy()
        _eval_book(
            f"{sp} Goblin OVER (sport model)",
            sub,
            mdir / f"{stem}.pkl",
            mdir / f"{stem}_features.json",
            prod,
            prod_feats,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
