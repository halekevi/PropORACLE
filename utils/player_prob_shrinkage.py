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


def _env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() not in ("0", "false", "no", "off")


# Ranking (sort keys): default OFF until shadow validates on live slates.
SHRINK_RANK_ENABLED = _env_flag("PROPORACLE_STANDARD_PLAYER_SHRINK", "0")
# Legacy alias — same as ranking flag (EV is a separate opt-in).
SHRINK_ENABLED = SHRINK_RANK_ENABLED
# est_win_prob / leg_prob_used / EV / Kelly: default OFF — more honest only when signed off.
SHRINK_EV_ENABLED = _env_flag("PROPORACLE_STANDARD_PLAYER_SHRINK_EV", "0")
# Shadow compare sidecar: default ON so live packs collect rank-vs-raw diffs without affecting construction.
SHRINK_SHADOW_ENABLED = _env_flag("PROPORACLE_STANDARD_PLAYER_SHRINK_SHADOW", "1")



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
    mode: str = "rank",
) -> tuple[float, str]:
    """Shrink Standard leg probs only. Goblin/Demon pass through unchanged.

    mode:
      - ``rank`` (default): gated by ``PROPORACLE_STANDARD_PLAYER_SHRINK``
      - ``ev``: gated by ``PROPORACLE_STANDARD_PLAYER_SHRINK_EV`` (pricing/display)
    Pass ``enabled=True/False`` to override either gate (shadow forced-on path).
    """
    if enabled is None:
        m = str(mode or "rank").strip().lower()
        enabled = SHRINK_EV_ENABLED if m == "ev" else SHRINK_RANK_ENABLED
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


def _leg_identity(row: Any) -> str:
    if hasattr(row, "get"):
        g = row.get
    else:
        g = lambda k, d=None: getattr(row, k, d)
    sport = str(g("sport") or "").strip().upper()
    player = str(g("player") or "").strip()
    prop = str(g("prop_type") or g("prop") or "").strip()
    direction = str(g("direction") or g("bet_direction") or "OVER").strip().upper()
    line = g("line")
    try:
        line_s = f"{float(line):g}" if line is not None and str(line) != "" else ""
    except (TypeError, ValueError):
        line_s = str(line or "")
    return f"{sport}|{player}|{prop}|{direction}|{line_s}"


def build_standard_shrink_shadow_report(
    df: pd.DataFrame | None,
    *,
    date_str: str | None = None,
    sort_mode: str = "winrate",
    top_n: int = 40,
    attach_sort_fn=None,
) -> dict[str, Any]:
    """Compare Standard top-N order with ranking shrink forced on vs raw (production).

    Does not mutate live construction. ``attach_sort_fn(df, mode) -> df`` should add
    ``__ts_pri`` without applying production shrink (or with it — we recompute both).
    """
    report: dict[str, Any] = {
        "date": str(date_str or "")[:10] or None,
        "shadow_track": True,
        "ticket_track": "standard_player_shrink_shadow",
        "production_rank_shrink": bool(SHRINK_RANK_ENABLED),
        "production_ev_shrink": bool(SHRINK_EV_ENABLED),
        "prior_strength": float(DEFAULT_PRIOR_STRENGTH),
        "sort_mode": sort_mode,
        "top_n": int(top_n),
        "note_mlb": (
            "Mixer MAIN/FINAL already hard-bans MLB Standard via "
            "_leg_mlb_keep_banned; shadow measures pre-ban Standard pool "
            "to see whether shrink alone would deprioritize MLB."
        ),
    }
    if df is None or len(df) == 0:
        report["status"] = "empty_slate"
        return report

    work = df.copy()
    if "pick_type" in work.columns:
        pt = work["pick_type"].astype(str)
        work = work[pt.map(is_standard_pick)].copy()
    if work.empty:
        report["status"] = "no_standard_rows"
        return report

    if attach_sort_fn is not None:
        ranked = attach_sort_fn(work, sort_mode)
    else:
        ranked = work
        if "__ts_pri" not in ranked.columns:
            ml = pd.to_numeric(ranked.get("ml_prob"), errors="coerce")
            ranked = ranked.copy()
            ranked["__ts_pri"] = ml.fillna(0.5)

    ranked = ranked.copy()
    # Caller should pass raw (unshrunk) __ts_pri; treat current __ts_pri as raw baseline.
    ranked["__ts_pri_raw"] = pd.to_numeric(ranked["__ts_pri"], errors="coerce")

    raw_pri = []
    shrunk_pri = []
    for idx, row in ranked.iterrows():
        try:
            pri = float(row.get("__ts_pri_raw"))
        except (TypeError, ValueError):
            pri = float("nan")
        if not math.isfinite(pri) or pri < 0:
            raw_pri.append(pri)
            shrunk_pri.append(pri)
            continue
        raw_pri.append(pri)
        s, _ = apply_standard_player_shrinkage(pri, "sort_pri", row, enabled=True)
        shrunk_pri.append(float(s))

    ranked["__ts_pri_raw"] = raw_pri
    ranked["__ts_pri_shrunk"] = shrunk_pri

    raw_sorted = ranked.sort_values("__ts_pri_raw", ascending=False, na_position="last")
    shrunk_sorted = ranked.sort_values("__ts_pri_shrunk", ascending=False, na_position="last")
    n = min(int(top_n), len(ranked))
    raw_top = raw_sorted.head(n)
    shrunk_top = shrunk_sorted.head(n)

    def _mix(sub: pd.DataFrame) -> dict[str, float]:
        if sub.empty or "sport" not in sub.columns:
            return {}
        vc = sub["sport"].astype(str).str.upper().str.strip().value_counts(normalize=True)
        return {str(k): round(float(v), 4) for k, v in vc.items()}

    def _mlb_share(sub: pd.DataFrame) -> float:
        if sub.empty or "sport" not in sub.columns:
            return 0.0
        sp = sub["sport"].astype(str).str.upper().str.strip()
        return round(float(sp.eq("MLB").mean()), 4)

    raw_ids = [_leg_identity(r) for _, r in raw_top.iterrows()]
    shrunk_ids = [_leg_identity(r) for _, r in shrunk_top.iterrows()]
    set_r, set_s = set(raw_ids), set(shrunk_ids)
    jaccard = (
        round(len(set_r & set_s) / len(set_r | set_s), 4) if (set_r or set_s) else None
    )

    def _summarize_rows(sub: pd.DataFrame, pri_col: str) -> list[dict]:
        rows = []
        for _, r in sub.iterrows():
            rows.append(
                {
                    "id": _leg_identity(r),
                    "sport": str(r.get("sport") or "").upper(),
                    "player": str(r.get("player") or ""),
                    "prop": str(r.get("prop_type") or r.get("prop") or ""),
                    "player_prior_n": float(resolve_player_prior_n(r)),
                    "pri": round(float(r.get(pri_col) or 0), 4),
                    "pri_raw": round(float(r.get("__ts_pri_raw") or 0), 4),
                    "pri_shrunk": round(float(r.get("__ts_pri_shrunk") or 0), 4),
                }
            )
        return rows

    report.update(
        {
            "status": "ok",
            "n_standard": int(len(ranked)),
            "mean_prior_n": round(
                float(pd.to_numeric(ranked.apply(resolve_player_prior_n, axis=1), errors="coerce").mean()),
                2,
            ),
            "mean_pri_raw_top": round(float(pd.to_numeric(raw_top["__ts_pri_raw"], errors="coerce").mean()), 4),
            "mean_pri_shrunk_top": round(
                float(pd.to_numeric(shrunk_top["__ts_pri_shrunk"], errors="coerce").mean()), 4
            ),
            "mean_pri_raw_all": round(float(pd.to_numeric(ranked["__ts_pri_raw"], errors="coerce").mean()), 4),
            "mean_pri_shrunk_all": round(
                float(pd.to_numeric(ranked["__ts_pri_shrunk"], errors="coerce").mean()), 4
            ),
            "jaccard_top": jaccard,
            "sport_mix_raw_top": _mix(raw_top),
            "sport_mix_shrunk_top": _mix(shrunk_top),
            "mlb_share_raw_top": _mlb_share(raw_top),
            "mlb_share_shrunk_top": _mlb_share(shrunk_top),
            "mlb_share_pool": _mlb_share(ranked),
            "mlb_mean_prior_n": round(
                float(
                    pd.to_numeric(
                        ranked.loc[
                            ranked["sport"].astype(str).str.upper().str.strip().eq("MLB")
                        ].apply(resolve_player_prior_n, axis=1),
                        errors="coerce",
                    ).mean()
                )
                if "sport" in ranked.columns
                and ranked["sport"].astype(str).str.upper().str.strip().eq("MLB").any()
                else 0.0,
                2,
            ),
            "raw_top": _summarize_rows(raw_top, "__ts_pri_raw"),
            "shrunk_top": _summarize_rows(shrunk_top, "__ts_pri_shrunk"),
        }
    )
    return report


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
