"""NBA player-stat projection: per-minute rate × minutes, per-stat shrinkage.

Layer 2 of the edge stack. Does not replace step7 ``projection`` (L5 point estimate).

Shrinkage k is per-stat (games, not props). Variance is shrunk separately.
Position labels, boxes, and positional baselines are strictly as-of the slate
date (latest pre-slate game — not end-of-season roster). Combos (PRA/PR/PA/RA)
use the stored combo column so component covariance is in the empirical var.

Last-season per-minute rates seed n<8 so opening weeks are not all-hold.
"""
from __future__ import annotations

import math
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from utils.nba_proj_k import (
    LAST_SEASON_WEIGHT,
    MIN_MINUTES_RATE,
    k_mean_for,
    k_var_for,
    load_k_table,
)
from utils.player_prob_shrinkage import shrink_toward_baseline
from utils.stat_distributions import Family, line_probs

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "data" / "cache" / "proporacle_ref.db"
# Legacy alias — live scoring still accepts --shrink-k override; default is per-stat.
DEFAULT_SHRINK_K = 22.0
MIN_GAMES_NO_HOLD = 8
MIN_BASE_RATE_N = 20
L5_MIN_BLEND = 0.6
LAST_SEASON_START_MONTH_DAY = (10, 15)

_STAT_COL = {
    "points": "pts",
    "pts": "pts",
    "rebounds": "reb",
    "reb": "reb",
    "assists": "ast",
    "ast": "ast",
    "steals": "stl",
    "stl": "stl",
    "blocks": "blk",
    "blk": "blk",
    "turnovers": "tov",
    "threes": "fg3m",
    "3ptmade": "fg3m",
    "3pm": "fg3m",
    "pra": "pra",
    "pr": "pr",
    "pa": "pa",
    "ra": "ra",
}

_ALLOWED_COLS = frozenset(_STAT_COL.values())
_COUNT_LIKE = frozenset({"reb", "ast", "stl", "blk", "tov", "fg3m"})
_POINTS_COMBO = frozenset({"pts", "pra", "pr", "pa", "ra"})


def _fold_stat(prop: object) -> str:
    s = str(prop or "").strip().lower().replace(" ", "").replace("_", "").replace("-", "")
    aliases = {
        "ptsrebsasts": "pra",
        "pointsreboundsassists": "pra",
        "ptsrebs": "pr",
        "ptsasts": "pa",
        "rebsasts": "ra",
        "threepointsmade": "threes",
        "3ptmade": "threes",
        "fg3m": "threes",
    }
    key = aliases.get(s, s)
    return key


def _stat_col(prop: object) -> str | None:
    key = _fold_stat(prop)
    return _STAT_COL.get(key)


def _fold_player(name: object) -> str:
    s = str(name or "").strip().lower()
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def pos_group(position: object) -> str:
    p = str(position or "").strip().upper()
    if not p or p in ("NAN", "NONE", "UNKNOWN"):
        return "UNK"
    if p in ("PG", "SG", "G", "PG-SG", "SG-PG"):
        return "G"
    if p in ("SF", "PF", "F", "SF-PF", "PF-SF"):
        return "F"
    if p in ("C", "C-F", "F-C", "PF-C", "C-PF"):
        return "C"
    if "G" in p and "F" not in p:
        return "G"
    if "C" in p:
        return "C"
    if "F" in p:
        return "F"
    return "UNK"


def nba_season_start(slate_date: str) -> str:
    s = str(slate_date or "")[:10]
    if len(s) != 10:
        return "2025-10-15"
    y = int(s[:4])
    m = int(s[5:7])
    if m >= 10:
        return f"{y}-10-15"
    return f"{y - 1}-10-15"


def pick_type_breakeven(pick_type: object, *, n_legs: int = 3) -> float:
    """Per-leg P needed for N-correct Power-n (never 1st-place)."""
    pt = str(pick_type or "").strip().lower()
    n = max(int(n_legs), 1)
    if "demon" in pt:
        x = 10.0
    elif "goblin" in pt:
        x = 2.5
    else:
        x = 6.0
    return float(x ** (-1.0 / n))


@dataclass(frozen=True)
class PlayerStatProjection:
    mu_raw: float
    mu_shrunk: float
    n: float
    var: float
    baseline: float
    family: Family
    p_over: float
    p_under: float
    p_push: float
    hold: bool
    position: str
    proj_minutes: float
    mu_rate: float
    k_mean: float
    k_var: float
    n_current: float
    n_prior: float


def _finite_mean(vals: np.ndarray) -> float:
    if vals.size == 0:
        return float("nan")
    return float(np.nanmean(vals))


def _finite_var(vals: np.ndarray) -> float:
    if vals.size < 2:
        return float("nan")
    return float(np.nanvar(vals, ddof=1))


def _blend_l5_l10(vals: np.ndarray) -> float:
    if vals.size == 0:
        return float("nan")
    l5 = float(np.mean(vals[:5])) if vals.size else float("nan")
    if vals.size < 10:
        return l5
    l10 = float(np.mean(vals[:10]))
    return L5_MIN_BLEND * l5 + (1.0 - L5_MIN_BLEND) * l10


class NbaBoxIndex:
    """In-memory as-of box scores. Built once per scoring pass."""

    def __init__(self, db_path: str | Path, k_table: dict[str, float] | None = None):
        self.db_path = str(db_path)
        self.k_table = k_table if k_table is not None else load_k_table()
        self.df = pd.DataFrame()
        self._by_espn: dict[str, pd.DataFrame] = {}
        self._by_name: dict[str, pd.DataFrame] = {}
        self._pos_cache: dict[tuple[str, str, str], tuple[float, float, float]] = {}
        self._load()

    @classmethod
    def from_frame(cls, df: pd.DataFrame, k_table: dict[str, float] | None = None) -> "NbaBoxIndex":
        obj = cls.__new__(cls)
        obj.db_path = ""
        obj.k_table = k_table if k_table is not None else load_k_table()
        obj.df = df.copy()
        obj._by_espn = {}
        obj._by_name = {}
        obj._pos_cache = {}
        obj._index()
        return obj

    def _load(self) -> None:
        if not Path(self.db_path).is_file():
            return
        cols = (
            "game_date, player, team, position, espn_athlete_id, minutes, "
            "pts, reb, ast, stl, blk, tov, fg3m, pra, pr, pa, ra"
        )
        try:
            con = sqlite3.connect(self.db_path)
            try:
                self.df = pd.read_sql_query(f"SELECT {cols} FROM nba", con)
            finally:
                con.close()
        except Exception:
            self.df = pd.DataFrame()
            return
        self._index()

    def _index(self) -> None:
        if self.df.empty:
            return
        self.df["game_date"] = self.df["game_date"].astype(str).str[:10]
        if "espn_athlete_id" in self.df.columns:
            self.df["espn_athlete_id"] = self.df["espn_athlete_id"].astype(str)
        else:
            self.df["espn_athlete_id"] = ""
        if "player" in self.df.columns:
            self.df["_name"] = self.df["player"].map(_fold_player)
        else:
            self.df["_name"] = ""
        self.df = self.df.sort_values("game_date")
        self._by_espn = {}
        self._by_name = {}
        for eid, g in self.df.groupby("espn_athlete_id", sort=False):
            if eid and eid not in ("None", "nan", ""):
                self._by_espn[str(eid)] = g
        for nm, g in self.df.groupby("_name", sort=False):
            if nm:
                self._by_name[str(nm)] = g

    def empty(self) -> bool:
        return self.df.empty

    def player_games(self, *, espn_id: str = "", player: str = "") -> pd.DataFrame:
        eid = str(espn_id or "").strip()
        if eid and eid in self._by_espn:
            return self._by_espn[eid]
        nm = _fold_player(player)
        if nm and nm in self._by_name:
            return self._by_name[nm]
        return pd.DataFrame()

    def games_before(
        self,
        *,
        espn_id: str = "",
        player: str = "",
        before_date: str = "",
        min_minutes: float = MIN_MINUTES_RATE,
    ) -> pd.DataFrame:
        g = self.player_games(espn_id=espn_id, player=player)
        if g.empty:
            return g
        before = str(before_date)[:10]
        out = g.loc[g["game_date"] < before]
        if min_minutes > 0 and "minutes" in out.columns:
            mins = pd.to_numeric(out["minutes"], errors="coerce")
            out = out.loc[mins >= float(min_minutes)]
        return out.sort_values("game_date", ascending=False)

    def position_as_of(self, *, espn_id: str = "", player: str = "", before_date: str = "") -> str:
        g = self.games_before(espn_id=espn_id, player=player, before_date=before_date, min_minutes=0.0)
        if g.empty or "position" not in g.columns:
            return ""
        ser = g["position"].dropna().astype(str)
        return str(ser.iloc[0]) if not ser.empty else ""

    def positional_rate_baseline(
        self,
        col: str,
        pos: str,
        before_date: str,
        *,
        min_minutes: float = MIN_MINUTES_RATE,
    ) -> tuple[float, float, float]:
        """(mean per-minute rate, var of rate, n) for pos_group as-of before_date."""
        group = pos_group(pos)
        before = str(before_date)[:10]
        key = (col, group, before)
        cached = self._pos_cache.get(key)
        if cached is not None:
            return cached
        nan = (float("nan"), float("nan"), 0.0)
        if self.df.empty or col not in self.df.columns:
            self._pos_cache[key] = nan
            return nan
        sub = self.df.loc[self.df["game_date"] < before].copy()
        if sub.empty:
            self._pos_cache[key] = nan
            return nan
        sub["_min"] = pd.to_numeric(sub["minutes"], errors="coerce")
        sub = sub.loc[sub["_min"] >= float(min_minutes)]
        if sub.empty:
            self._pos_cache[key] = nan
            return nan
        sub["_rate"] = pd.to_numeric(sub[col], errors="coerce") / sub["_min"]
        sub["_pg"] = sub["position"].map(pos_group) if "position" in sub.columns else "UNK"
        rates = sub.loc[sub["_pg"].eq(group), "_rate"] if group != "UNK" else sub["_rate"]
        if rates.size < 30:
            rates = sub["_rate"]
        arr = pd.to_numeric(rates, errors="coerce").to_numpy(dtype=float)
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            self._pos_cache[key] = nan
            return nan
        var = float(np.var(arr, ddof=1)) if arr.size >= 2 else float("nan")
        rec = (float(np.mean(arr)), var, float(arr.size))
        self._pos_cache[key] = rec
        return rec


def _split_current_prior(games: pd.DataFrame, slate_date: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    start = nba_season_start(slate_date)
    if games.empty or "game_date" not in games.columns:
        empty = games.iloc[0:0] if not games.empty else pd.DataFrame()
        return empty, empty
    d = games["game_date"].astype(str).str[:10]
    current = games.loc[d >= start]
    prior = games.loc[d < start]
    return current, prior


def _rate_and_minutes(games: pd.DataFrame, col: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if games.empty or col not in games.columns:
        z = np.array([], dtype=float)
        return z, z, z
    vals = pd.to_numeric(games[col], errors="coerce").to_numpy(dtype=float)
    mins = pd.to_numeric(games["minutes"], errors="coerce").to_numpy(dtype=float)
    ok = np.isfinite(vals) & np.isfinite(mins) & (mins >= MIN_MINUTES_RATE)
    return vals[ok], mins[ok], (vals[ok] / mins[ok])


def project_player_stat(
    samples: np.ndarray,
    line: float,
    *,
    baseline: float,
    n: float | None = None,
    position: str = "",
    count_like: bool = True,
    shrink_k: float | None = None,
    base_rate_n: float = 0.0,
    minutes: np.ndarray | None = None,
    baseline_rate: float | None = None,
    baseline_rate_var: float | None = None,
    prior_rates: np.ndarray | None = None,
    prior_minutes: np.ndarray | None = None,
    stat_col: str = "",
    k_var: float | None = None,
    seed_prior: bool = True,
) -> PlayerStatProjection:
    """Shrink per-minute rate (fallback: totals) then µ = rate × projected minutes."""
    col = str(stat_col or "").strip().lower()
    k_m = float(shrink_k) if shrink_k is not None else k_mean_for(col or "pts")
    k_v = float(k_var) if k_var is not None else k_var_for(col or "pts", k_mean=k_m)

    totals = np.asarray(samples, dtype=float)
    totals = totals[np.isfinite(totals)]
    mins = None if minutes is None else np.asarray(minutes, dtype=float)
    rates = np.array([], dtype=float)
    if mins is not None and mins.size == totals.size and mins.size:
        ok = np.isfinite(mins) & (mins >= MIN_MINUTES_RATE) & np.isfinite(totals)
        rates = totals[ok] / mins[ok]
        play_mins = mins[ok]
        totals = totals[ok]
    else:
        play_mins = np.array([], dtype=float)

    prior_r = np.array([], dtype=float)
    prior_m = np.array([], dtype=float)
    if seed_prior and prior_rates is not None:
        prior_r = np.asarray(prior_rates, dtype=float)
        prior_r = prior_r[np.isfinite(prior_r)]
    if seed_prior and prior_minutes is not None:
        prior_m = np.asarray(prior_minutes, dtype=float)
        prior_m = prior_m[np.isfinite(prior_m) & (prior_m >= MIN_MINUTES_RATE)]

    n_cur = float(n) if n is not None else float(rates.size if rates.size else totals.size)
    n_pri = float(prior_r.size * LAST_SEASON_WEIGHT) if prior_r.size else 0.0
    n_eff = n_cur + n_pri

    use_rate = rates.size > 0 or prior_r.size > 0
    pos = pos_group(position)

    if use_rate:
        rate_raw = _finite_mean(rates) if rates.size else _finite_mean(prior_r)
        if prior_r.size and rates.size:
            rate_raw = (n_cur * rate_raw + n_pri * float(np.mean(prior_r))) / max(n_eff, 1e-9)
        elif prior_r.size and not rates.size:
            rate_raw = float(np.mean(prior_r))
        br = float(baseline_rate) if baseline_rate is not None and math.isfinite(float(baseline_rate)) else float("nan")
        if not math.isfinite(br):
            br = float(baseline) if math.isfinite(baseline) else rate_raw
        rate_hat = shrink_toward_baseline(rate_raw, n_eff, br, k_m)
        rate_var = _finite_var(rates) if rates.size >= 2 else _finite_var(prior_r)
        br_var = float(baseline_rate_var) if baseline_rate_var is not None else float("nan")
        if not math.isfinite(br_var) or br_var <= 0:
            br_var = rate_var if math.isfinite(rate_var) else 0.01
        if not math.isfinite(rate_var) or rate_var <= 0:
            rate_var = br_var
        rate_var = shrink_toward_baseline(rate_var, n_eff, br_var, k_v)
        proj_min = _blend_l5_l10(play_mins) if play_mins.size else _blend_l5_l10(prior_m)
        if not math.isfinite(proj_min):
            proj_min = 24.0
        min_var = _finite_var(play_mins) if play_mins.size >= 2 else _finite_var(prior_m)
        if not math.isfinite(min_var) or min_var < 0:
            min_var = 0.0
        mu_raw = rate_raw * proj_min if math.isfinite(rate_raw) else float("nan")
        mu = rate_hat * proj_min if math.isfinite(rate_hat) else float("nan")
        # Var(R×M) ≈ m² var(R) + r² var(M) (minutes independent of rate — first-order)
        var = (proj_min ** 2) * max(rate_var, 0.0) + (rate_hat ** 2) * min_var
        mu_rate = rate_hat
    else:
        mu_raw = _finite_mean(totals)
        if not math.isfinite(mu_raw):
            mu_raw = baseline if math.isfinite(baseline) else float("nan")
        mu = shrink_toward_baseline(mu_raw, n_eff if n_eff else n_cur, baseline, k_m)
        var = _finite_var(totals)
        if not math.isfinite(var) or var <= 0:
            var = max(mu, 1.0) if math.isfinite(mu) else 1.0
        else:
            pos_var = max(baseline, 1.0) if math.isfinite(baseline) else var
            var = shrink_toward_baseline(var, n_eff if n_eff else n_cur, pos_var, k_v)
        proj_min = float("nan")
        mu_rate = float("nan")

    if not math.isfinite(var) or var <= 0:
        var = max(mu, 1.0) if math.isfinite(mu) else 1.0

    count_like_eff = col in _COUNT_LIKE and col not in _POINTS_COMBO
    p_over, p_under, p_push, family = line_probs(
        mu,
        line,
        var=var,
        n=n_eff,
        count_like=count_like_eff if col else count_like,
        stat_col=col or ("pts" if not count_like else "stl"),
    )
    hold = n_eff < MIN_GAMES_NO_HOLD or float(base_rate_n) < MIN_BASE_RATE_N
    return PlayerStatProjection(
        mu_raw=mu_raw,
        mu_shrunk=mu,
        n=n_eff,
        var=var,
        baseline=float(baseline_rate) if use_rate and baseline_rate is not None else baseline,
        family=family,
        p_over=p_over,
        p_under=p_under,
        p_push=p_push,
        hold=hold,
        position=pos,
        proj_minutes=proj_min,
        mu_rate=mu_rate,
        k_mean=k_m,
        k_var=k_v,
        n_current=n_cur,
        n_prior=n_pri,
    )


def project_from_index(
    index: NbaBoxIndex,
    *,
    espn_id: str,
    player: str,
    prop: object,
    line: float,
    before_date: str,
    position_hint: str = "",
    base_rate_n: float = 0.0,
    shrink_k: float | None = None,
) -> PlayerStatProjection | None:
    col = _stat_col(prop)
    if not col:
        return None
    try:
        line = float(line)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(line):
        return None
    games = index.games_before(espn_id=espn_id, player=player, before_date=before_date)
    pos = index.position_as_of(espn_id=espn_id, player=player, before_date=before_date) or position_hint
    current, prior = _split_current_prior(games, before_date)
    cur_tot, cur_min, cur_rate = _rate_and_minutes(current, col)
    _pt, pri_min, pri_rate = _rate_and_minutes(prior, col)
    br, br_var, _bn = index.positional_rate_baseline(col, pos, before_date)
    # Totals baseline for the no-minutes fallback (DNP / missing minutes).
    tot_baseline = br * 24.0 if math.isfinite(br) else float("nan")
    k_m = shrink_k if shrink_k is not None else k_mean_for(col, index.k_table)
    k_v = k_var_for(col, index.k_table, k_mean=float(k_m))
    samples = cur_tot if cur_tot.size else np.array([], dtype=float)
    return project_player_stat(
        samples,
        line,
        baseline=tot_baseline,
        n=float(cur_rate.size),
        position=pos,
        count_like=col in _COUNT_LIKE,
        shrink_k=float(k_m),
        base_rate_n=base_rate_n,
        minutes=cur_min if cur_min.size else None,
        baseline_rate=br,
        baseline_rate_var=br_var,
        prior_rates=pri_rate if pri_rate.size else None,
        prior_minutes=pri_min if pri_min.size else None,
        stat_col=col,
        k_var=k_v,
    )


def fetch_player_games(
    con: sqlite3.Connection,
    *,
    espn_id: str,
    col: str,
    before_date: str,
    min_minutes: float = MIN_MINUTES_RATE,
) -> pd.DataFrame:
    """Legacy SQL helper (single-row live scoring without an index)."""
    if col not in _ALLOWED_COLS:
        return pd.DataFrame(columns=["game_date", "val", "position", "minutes"])
    before = str(before_date or "")[:10]
    if not espn_id or len(before) != 10:
        return pd.DataFrame(columns=["game_date", "val", "position", "minutes"])
    sql = f"""
        SELECT game_date, {col} AS val, position, minutes
        FROM nba
        WHERE espn_athlete_id = ?
          AND game_date < ?
          AND {col} IS NOT NULL
          AND coalesce(minutes, 0) >= ?
        ORDER BY game_date DESC
    """
    try:
        df = pd.read_sql_query(sql, con, params=(str(espn_id), before, float(min_minutes)))
    except Exception:
        return pd.DataFrame(columns=["game_date", "val", "position", "minutes"])
    return df


def project_row(
    row: Any,
    *,
    con: sqlite3.Connection | None = None,
    db_path: str = "",
    before_date: str,
    shrink_k: float | None = None,
    base_rate_n: float = 0.0,
    index: NbaBoxIndex | None = None,
) -> PlayerStatProjection | None:
    getter = row.get if hasattr(row, "get") else lambda k, d=None: getattr(row, k, d)
    prop = getter("prop_type") or getter("prop") or getter("stat_type") or getter("stat_norm")
    line = getter("line")
    if line is None:
        line = getter("line_score")
    try:
        line_f = float(line)
    except (TypeError, ValueError):
        return None
    espn_id = str(getter("ESPN_ATHLETE_ID") or getter("espn_athlete_id") or getter("espn_id") or "").strip()
    player = str(getter("player_name") or getter("player") or getter("pp_player") or "")
    pos_hint = str(getter("position") or getter("pos") or "")
    box = index
    if box is None:
        path = db_path or str(DEFAULT_DB)
        box = NbaBoxIndex(path)
    return project_from_index(
        box,
        espn_id=espn_id,
        player=player,
        prop=prop,
        line=line_f,
        before_date=before_date,
        position_hint=pos_hint,
        base_rate_n=base_rate_n,
        shrink_k=shrink_k,
    )


def directional_l5_l10(
    games: pd.DataFrame,
    col: str,
    line: float,
    direction: str,
    *,
    n5: int = 5,
    n10: int = 10,
) -> tuple[int | None, int | None]:
    """Hits vs *today's* line on the last 5/10 pre-slate boxes. Push ≠ hit."""
    if games.empty or col not in games.columns or not math.isfinite(line):
        return None, None
    vals = pd.to_numeric(games[col], errors="coerce").to_numpy(dtype=float)
    vals = vals[np.isfinite(vals)]
    side = str(direction or "OVER").strip().upper()
    if side == "UNDER":
        hits = vals < line
    else:
        hits = vals > line
    if hits.size < n5:
        return None, None
    l5 = int(hits[:n5].sum())
    l10 = int(hits[:n10].sum()) if hits.size >= n10 else int(hits.sum())
    return l5, l10
