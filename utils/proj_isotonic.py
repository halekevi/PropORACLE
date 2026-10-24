"""Walk-forward isotonic calibration of a probability column.

Fit strictly on dates < as_of, per stat × direction, backing off to per-stat
then global when the group is thin. Used to un-kink the overconfident tail
before P − breakeven.
"""
from __future__ import annotations

import math
from collections import defaultdict

import numpy as np
import pandas as pd

MIN_ISO_N = 80


def _iso_fit(p: np.ndarray, y: np.ndarray):
    from sklearn.isotonic import IsotonicRegression

    ir = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
    ir.fit(p, y)
    return ir


def _predict(model, p: float) -> float:
    if model is None or not math.isfinite(p):
        return p
    out = model.predict(np.asarray([p], dtype=float))
    return float(out[0])


def calibrate_walk_forward(
    df: pd.DataFrame,
    *,
    p_col: str = "proj_p_play",
    y_col: str = "_y",
    date_col: str = "file_date",
    stat_col: str = "prop",
    dir_col: str = "direction",
    min_n: int = MIN_ISO_N,
) -> pd.Series:
    """Return calibrated p aligned to ``df.index``. No future grades in the map."""
    from utils.expanding_marginals import _fold_dir, _fold_stat

    p = pd.to_numeric(df[p_col], errors="coerce")
    y = pd.to_numeric(df[y_col], errors="coerce")
    dates = df[date_col].astype(str).str[:10]
    stats = df[stat_col].map(_fold_stat) if stat_col in df.columns else pd.Series("all", index=df.index)
    dirs = df[dir_col].map(_fold_dir) if dir_col in df.columns else pd.Series("OVER", index=df.index)

    hist_p: dict[tuple[str, str], list[float]] = defaultdict(list)
    hist_y: dict[tuple[str, str], list[float]] = defaultdict(list)
    out = pd.Series(np.nan, index=df.index, dtype=float)

    for d in sorted(dates.unique()):
        mask = dates.eq(d)
        models: dict[tuple[str, str], object] = {}
        stat_models: dict[str, object] = {}
        global_model = None

        def _fit_key(key: tuple[str, str]):
            pp = np.asarray(hist_p[key], dtype=float)
            yy = np.asarray(hist_y[key], dtype=float)
            if pp.size < min_n:
                return None
            return _iso_fit(pp, yy)

        for key in list(hist_p.keys()):
            models[key] = _fit_key(key)
        by_stat: dict[str, tuple[list[float], list[float]]] = defaultdict(lambda: ([], []))
        all_p: list[float] = []
        all_y: list[float] = []
        for (st, di), plist in hist_p.items():
            by_stat[st][0].extend(plist)
            by_stat[st][1].extend(hist_y[(st, di)])
            all_p.extend(plist)
            all_y.extend(hist_y[(st, di)])
        for st, (pp, yy) in by_stat.items():
            if len(pp) >= min_n:
                stat_models[st] = _iso_fit(np.asarray(pp, dtype=float), np.asarray(yy, dtype=float))
        if len(all_p) >= min_n:
            global_model = _iso_fit(np.asarray(all_p, dtype=float), np.asarray(all_y, dtype=float))

        day = df.loc[mask]
        for idx in day.index:
            raw = float(p.loc[idx]) if pd.notna(p.loc[idx]) else float("nan")
            key = (str(stats.loc[idx]), str(dirs.loc[idx]))
            model = models.get(key) or stat_models.get(key[0]) or global_model
            out.loc[idx] = _predict(model, raw) if model is not None else raw

        for idx in day.index:
            yi = y.loc[idx]
            pi = p.loc[idx]
            if yi not in (0.0, 1.0) or pd.isna(pi):
                continue
            hist_p[(str(stats.loc[idx]), str(dirs.loc[idx]))].append(float(pi))
            hist_y[(str(stats.loc[idx]), str(dirs.loc[idx]))].append(float(yi))

    return out
