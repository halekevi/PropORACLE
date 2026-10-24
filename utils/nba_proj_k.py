"""Per-stat shrinkage strengths for NBA box-score means / variances.

``k=150`` was fit on player_prior_n counted in *props*. Box-score n is games.
Rebounds and minutes stabilize fast; 3PM / steals stay noisy. Fit on games
strictly before ``fit_until`` (default 2026-01-01) so Jan–Apr eval does not
choose k.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_K_PATH = REPO_ROOT / "data" / "cache" / "nba_proj_k.json"

# Games, not props. Frozen until a pre-cutoff fit replaces them.
K_MEAN_DEFAULT: dict[str, float] = {
    "minutes": 8.0,
    "reb": 10.0,
    "ast": 18.0,
    "pts": 22.0,
    "pra": 22.0,
    "pr": 22.0,
    "pa": 22.0,
    "ra": 18.0,
    "fg3m": 45.0,
    "stl": 50.0,
    "blk": 50.0,
    "tov": 28.0,
}
K_VAR_MULT_DEFAULT = 2.5
K_GRID = (4.0, 6.0, 8.0, 10.0, 12.0, 15.0, 18.0, 22.0, 28.0, 35.0, 45.0, 55.0, 70.0, 90.0)
MIN_PLAYERS_TO_FIT = 40
MIN_GAMES_PLAYER = 16
MIN_MINUTES_RATE = 8.0
LAST_SEASON_WEIGHT = 0.5


def k_mean_for(col: str, table: dict[str, float] | None = None) -> float:
    src = table if table else K_MEAN_DEFAULT
    return float(src.get(col, src.get("pts", 22.0)))


def k_var_for(col: str, table: dict[str, float] | None = None, *, k_mean: float | None = None) -> float:
    km = float(k_mean) if k_mean is not None else k_mean_for(col, table)
    if table and f"{col}__var" in table:
        return float(table[f"{col}__var"])
    return float(km * K_VAR_MULT_DEFAULT)


def load_k_table(path: Path | None = None) -> dict[str, float]:
    p = path or DEFAULT_K_PATH
    out = dict(K_MEAN_DEFAULT)
    if p.is_file():
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            means = raw.get("k_mean") or raw
            if isinstance(means, dict):
                for k, v in means.items():
                    try:
                        out[str(k)] = float(v)
                    except (TypeError, ValueError):
                        continue
            kvars = raw.get("k_var") or {}
            if isinstance(kvars, dict):
                for k, v in kvars.items():
                    try:
                        out[f"{k}__var"] = float(v)
                    except (TypeError, ValueError):
                        continue
        except (OSError, json.JSONDecodeError):
            pass
    return out


def save_k_table(payload: dict[str, Any], path: Path | None = None) -> Path:
    p = path or DEFAULT_K_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return p


def _rates(vals: np.ndarray, minutes: np.ndarray) -> np.ndarray:
    m = np.asarray(minutes, dtype=float)
    v = np.asarray(vals, dtype=float)
    ok = np.isfinite(v) & np.isfinite(m) & (m >= MIN_MINUTES_RATE)
    return v[ok] / m[ok]


def _empirical_bayes_k(series_list: list[np.ndarray]) -> tuple[float, dict[str, float]]:
    """k = within-player var / between-player var of means (classic EB).

    Next-game RMSE is too noisy and pins k at the grid edge. This is the
    shrinkage that actually matches 'rebounds settle, steals don't'.
    """
    mus: list[float] = []
    s2s: list[float] = []
    ns: list[float] = []
    for arr in series_list:
        if arr.size < 10:
            continue
        mus.append(float(np.mean(arr)))
        s2s.append(float(np.var(arr, ddof=1)))
        ns.append(float(arr.size))
    if len(mus) < MIN_PLAYERS_TO_FIT:
        return float("nan"), {"n_players": float(len(mus))}
    mu = np.asarray(mus, dtype=float)
    s2 = np.asarray(s2s, dtype=float)
    nn = np.asarray(ns, dtype=float)
    within = float(np.average(s2, weights=nn))
    var_mu = float(np.var(mu, ddof=1))
    between = var_mu - within * float(np.mean(1.0 / nn))
    between = max(between, 1e-12)
    k = within / between
    k = max(4.0, min(90.0, k))
    return k, {
        "n_players": float(len(mus)),
        "within": within,
        "between": between,
        "var_mu": var_mu,
        "k_raw": within / between,
    }


def fit_k_from_boxes(
    boxes: pd.DataFrame,
    *,
    fit_until: str = "2026-01-01",
    cols: tuple[str, ...] = ("pts", "reb", "ast", "fg3m", "stl", "blk", "tov", "pra", "minutes"),
) -> dict[str, Any]:
    """Walk-forward next-game RMSE on per-minute rates (minutes: raw). Pre-cutoff only."""
    before = str(fit_until)[:10]
    df = boxes.copy()
    df["game_date"] = df["game_date"].astype(str).str[:10]
    df = df.loc[df["game_date"] < before].copy()
    payload: dict[str, Any] = {
        "fit_until": before,
        "k_mean": dict(K_MEAN_DEFAULT),
        "k_var": {c: K_MEAN_DEFAULT.get(c, 22.0) * K_VAR_MULT_DEFAULT for c in K_MEAN_DEFAULT},
        "rmse": {},
        "n_players": {},
        "source": "default",
    }
    if df.empty or "espn_athlete_id" not in df.columns:
        payload["note"] = "no pre-cutoff boxes; using frozen defaults"
        return payload

    id_col = "espn_athlete_id"
    df[id_col] = df[id_col].astype(str)
    k_mean: dict[str, float] = dict(K_MEAN_DEFAULT)
    k_var: dict[str, float] = {c: K_MEAN_DEFAULT.get(c, 22.0) * K_VAR_MULT_DEFAULT for c in K_MEAN_DEFAULT}
    rmse: dict[str, float] = {}
    n_players: dict[str, int] = {}

    for col in cols:
        if col not in df.columns:
            continue
        series_list: list[np.ndarray] = []
        var_series: list[np.ndarray] = []
        n_pl = 0
        for _, g in df.groupby(id_col, sort=False):
            g = g.sort_values("game_date")
            if col == "minutes":
                arr = pd.to_numeric(g["minutes"], errors="coerce").to_numpy(dtype=float)
                arr = arr[np.isfinite(arr) & (arr >= MIN_MINUTES_RATE)]
            else:
                arr = _rates(
                    pd.to_numeric(g[col], errors="coerce").to_numpy(dtype=float),
                    pd.to_numeric(g["minutes"], errors="coerce").to_numpy(dtype=float),
                )
            if arr.size < MIN_GAMES_PLAYER:
                continue
            series_list.append(arr)
            var_series.append(arr)
            n_pl += 1
        n_players[col] = n_pl
        if n_pl < MIN_PLAYERS_TO_FIT:
            continue
        best_k, diag = _empirical_bayes_k(series_list)
        if not math.isfinite(best_k):
            continue
        k_mean[col] = float(best_k)
        rmse[col] = diag
        # Variance k: shrink sample var toward pool var; score next squared error.
        pool_var = float(np.nanvar(np.concatenate(series_list), ddof=1)) if series_list else 1.0
        best_kv = best_k * K_VAR_MULT_DEFAULT
        best_vrmse = float("inf")
        for mult in (1.5, 2.0, 2.5, 3.0, 4.0):
            kv = best_k * mult
            sq = 0.0
            n = 0
            for arr in var_series:
                for t in range(10, arr.size):
                    hist = arr[:t]
                    s2 = float(np.var(hist, ddof=1))
                    w = t / (t + kv)
                    vhat = w * s2 + (1.0 - w) * pool_var
                    target = (arr[t] - float(np.mean(hist))) ** 2
                    sq += (vhat - target) ** 2
                    n += 1
            if n < 50:
                continue
            vr = math.sqrt(sq / n)
            if vr < best_vrmse:
                best_vrmse = vr
                best_kv = kv
        k_var[col] = float(best_kv)

    payload["k_mean"] = k_mean
    payload["k_var"] = k_var
    payload["rmse"] = rmse
    payload["n_players"] = n_players
    payload["source"] = "fit" if rmse else "default"
    if not rmse:
        payload["note"] = f"fewer than {MIN_PLAYERS_TO_FIT} players per stat before {before}"
    return payload
