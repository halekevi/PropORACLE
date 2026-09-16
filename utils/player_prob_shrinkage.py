"""Player-history shrinkage for Standard leg probabilities.

Shrinks model/leg prob toward a cell baseline when the player has thin history:
    w = n / (n + prior_strength)
    shrunk = w * p + (1 - w) * baseline

Validated in May–June backtest (rank_by=player_shrunk): cross-sport Standard
3–4 leg clears breakeven; MLB-only still thin. Goblin uses a hard floor instead —
do not apply this to Goblin/Demon.
"""

from __future__ import annotations

import glob
import json
import math
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GRADED_DIR = REPO_ROOT / "ui_runner" / "templates"
DEFAULT_PRIOR_STRENGTH = float(os.getenv("PROPORACLE_STANDARD_PLAYER_SHRINK_PRIOR", "150"))
SHRINK_ENABLED = os.getenv("PROPORACLE_STANDARD_PLAYER_SHRINK", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)


def shrink_prob_toward_baseline(
    model_prob: float,
    prior_n: float,
    baseline: float,
    prior_strength: float = DEFAULT_PRIOR_STRENGTH,
) -> float:
    """w=n/(n+k); shrunk = w*p + (1-w)*baseline. Same form as correlation shrunk_lift."""
    try:
        p = float(model_prob)
        n = max(0.0, float(prior_n))
        b = float(baseline)
        k = max(0.0, float(prior_strength))
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(p):
        return 0.0
    if k <= 0:
        return max(0.0, min(1.0, p))
    w = n / (n + k)
    return max(0.0, min(1.0, w * p + (1.0 - w) * b))


def is_standard_pick(pick_type: object) -> bool:
    pt = str(pick_type or "").strip().lower()
    if not pt:
        return True  # missing often means Standard on older rows
    if "goblin" in pt or "demon" in pt:
        return False
    return "standard" in pt or pt in {"std", "s"}


def _fold_player(name: object) -> str:
    s = str(name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _to_prob(v: object) -> float | None:
    try:
        if v is None or (isinstance(v, float) and not math.isfinite(v)):
            return None
        x = float(v)
        if x > 1.0 and x <= 100.0:
            x = x / 100.0
        if 0.0 <= x <= 1.0:
            return x
    except (TypeError, ValueError):
        return None
    return None


def resolve_player_prior_n(row: Any) -> float:
    """Best available player history depth on a slate / graded row."""
    if hasattr(row, "get"):
        getter = row.get
    else:
        getter = lambda k, default=None: getattr(row, k, default)

    for col in (
        "player_prior_n",
        "player_prior_count",
        "strat_n",
        "n_games",
        "games_played",
        "props_seen",
        "distribution_n",
    ):
        raw = getter(col)
        try:
            if raw is None or (isinstance(raw, float) and math.isnan(raw)):
                continue
            n = float(raw)
            if math.isfinite(n) and n >= 0:
                return n
        except (TypeError, ValueError):
            continue
    return 0.0


def resolve_shrink_baseline(row: Any) -> float:
    """Cell / directional baseline for shrinkage (defaults to 0.5).

    Prefer category / composite HR and directional hit-rate fields that are already
    probabilities. Do not use raw L5/L10 hit counts (0–5 / 0–10).
    """
    if hasattr(row, "get"):
        getter = row.get
    else:
        getter = lambda k, default=None: getattr(row, k, default)

    direction = str(
        getter("bet_direction")
        or getter("direction_used")
        or getter("direction")
        or "OVER"
    ).strip().upper()

    candidates: list[object] = [
        getter("category_hr"),
        getter("composite_hit_rate"),
    ]
    if "UNDER" in direction:
        candidates.extend(
            [
                getter("under_hit_rate"),
                getter("hit_prob_under"),
            ]
        )
    else:
        candidates.extend(
            [
                getter("over_hit_rate"),
                getter("hit_prob_over"),
            ]
        )
    candidates.extend(
        [
            getter("hit_prob_selected"),
            getter("hit_rate"),
        ]
    )
    for c in candidates:
        p = _to_prob(c)
        if p is not None:
            return p
    return 0.5


def apply_standard_player_shrinkage(
    prob: float,
    source: str,
    row: Any,
    *,
    prior_strength: float | None = None,
    enabled: bool | None = None,
) -> tuple[float, str]:
    """Shrink Standard leg probs only. Goblin/Demon pass through unchanged."""
    if enabled is None:
        enabled = SHRINK_ENABLED
    if not enabled:
        return prob, source

    pick = None
    if hasattr(row, "get"):
        pick = row.get("pick_type")
    else:
        pick = getattr(row, "pick_type", None)
    if not is_standard_pick(pick):
        return prob, source

    prior_n = resolve_player_prior_n(row)
    baseline = resolve_shrink_baseline(row)
    k = DEFAULT_PRIOR_STRENGTH if prior_strength is None else float(prior_strength)
    shrunk = shrink_prob_toward_baseline(prob, prior_n, baseline, prior_strength=k)
    return shrunk, f"{source}|player_shrunk(n={int(prior_n)},k={int(k)})"


def _player_key_from_prop(p: dict) -> str:
    pid = str(p.get("player_id") or p.get("espn_id") or "").strip()
    if pid:
        return f"id:{pid}"
    return f"name:{_fold_player(p.get('player'))}"


@lru_cache(maxsize=4)
def _player_prior_counts_cached(graded_dir: str, before_date: str) -> dict[str, int]:
    """Count graded prop appearances per player with date < before_date."""
    counts: dict[str, int] = {}
    pattern = os.path.join(graded_dir, "graded_props_*.json")
    for fp in sorted(glob.glob(pattern)):
        m = re.search(r"graded_props_(\d{4}-\d{2}-\d{2})", os.path.basename(fp))
        file_date = m.group(1) if m else ""
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        props = data if isinstance(data, list) else (data.get("props") or [])
        for p in props:
            if not isinstance(p, dict):
                continue
            d = str(p.get("date") or p.get("match_date") or p.get("game_date") or file_date)[:10]
            if before_date and d and d >= before_date:
                continue
            key = _player_key_from_prop(p)
            if key in ("id:", "name:"):
                continue
            counts[key] = counts.get(key, 0) + 1
    return counts


def attach_player_prior_n(
    df: pd.DataFrame | None,
    *,
    graded_dir: str | Path | None = None,
    slate_date: str | None = None,
) -> pd.DataFrame:
    """Attach walk-forward ``player_prior_n`` from graded_props history.

    Counts appearances strictly before ``slate_date`` (or before each row's date
    when slate_date is omitted — uses min row date as shared cutoff for speed).
    """
    if df is None or len(df) == 0:
        return df if df is not None else pd.DataFrame()
    out = df.copy()
    gdir = str(graded_dir or DEFAULT_GRADED_DIR)
    if "player_prior_n" in out.columns and pd.to_numeric(out["player_prior_n"], errors="coerce").notna().any():
        # Already populated (e.g. persisted) — fill gaps only
        missing = pd.to_numeric(out["player_prior_n"], errors="coerce").isna()
        if not missing.any():
            return out
    else:
        missing = pd.Series(True, index=out.index)

    if slate_date:
        before = str(slate_date)[:10]
    elif "game_date" in out.columns:
        dates = out["game_date"].astype(str).str[:10]
        before = str(dates.min())
    elif "date" in out.columns:
        dates = out["date"].astype(str).str[:10]
        before = str(dates.min())
    else:
        before = "9999-99-99"

    counts = _player_prior_counts_cached(gdir, before)

    def _row_key(r: pd.Series) -> str:
        pid = str(r.get("player_id") or r.get("espn_id") or "").strip()
        if pid:
            return f"id:{pid}"
        return f"name:{_fold_player(r.get('player'))}"

    priors = []
    for idx, r in out.iterrows():
        if not missing.loc[idx]:
            priors.append(float(out.at[idx, "player_prior_n"]))
            continue
        priors.append(float(counts.get(_row_key(r), 0)))
    out["player_prior_n"] = priors
    return out
