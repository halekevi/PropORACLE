"""Soccer prop-aware defense metric selection (goals vs shots conceded).

Shot / SoT / shot-related props prefer ``SHOTS_DEF_RANK`` / ``SHOTS_DEF_TIER``
when available; goals and everything else keep goals-conceded overall tier.

Alignment for badges (handled by callers / rank_best_props):
  OVER → Weak | Below Avg
  UNDER → Elite | Above Avg
  Avg never passes D.
"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from utils.defense_tiers import def_tier_from_overall_rank, normalize_def_tier_label

SHOT_RELATED_PROPS = frozenset(
    {
        "shots",
        "shots_on_target",
        "shots_assisted",
        "shot",
        "sot",
        "sh",
        "sog",
    }
)


def _norm_prop(raw: object) -> str:
    s = str(raw or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def prop_def_metric(prop: object) -> str:
    """Return ``shots_conceded`` or ``goals_conceded`` for this prop."""
    p = _norm_prop(prop)
    if p in SHOT_RELATED_PROPS:
        return "shots_conceded"
    # Any prop whose normalized name is shot-centric (e.g. total_shots).
    if "shot" in p and "goal" not in p:
        return "shots_conceded"
    return "goals_conceded"


def _first_valid_rank(row: pd.Series | dict, keys: Iterable[str]) -> float:
    for k in keys:
        v = row.get(k) if hasattr(row, "get") else None
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f == f and f > 0:
            return f
    return float("nan")


def _first_valid_tier(row: pd.Series | dict, keys: Iterable[str]) -> str:
    for k in keys:
        v = row.get(k) if hasattr(row, "get") else None
        label = normalize_def_tier_label(v)
        if label and label not in ("N/A",):
            return label
    return ""


def select_prop_aware_rank_tier(
    row: pd.Series | dict,
    *,
    prop: object | None = None,
    n_teams: int | None = None,
) -> tuple[float, str, str]:
    """Return (rank, tier, metric) for this prop row.

    Prefers shots ranks for shot props when present; otherwise goals overall.
    """
    if prop is None:
        prop = (
            row.get("prop_norm")
            or row.get("prop_type_normalized")
            or row.get("prop_type")
            or row.get("Prop")
        )
    metric = prop_def_metric(prop)

    if metric == "shots_conceded":
        rank = _first_valid_rank(
            row,
            (
                "SHOTS_DEF_RANK",
                "shots_def_rank",
                "opp_shots_def_rank",
            ),
        )
        tier = _first_valid_tier(
            row,
            (
                "SHOTS_DEF_TIER",
                "shots_def_tier",
                "opp_shots_def_tier",
            ),
        )
        if rank == rank:
            if not tier and n_teams and n_teams >= 2:
                try:
                    tier = def_tier_from_overall_rank(int(rank), int(n_teams))
                except (TypeError, ValueError):
                    tier = ""
            if tier:
                return rank, tier, metric
        # Fall through to goals when shots ranks missing.
        metric = "goals_conceded"

    rank = _first_valid_rank(
        row,
        (
            "OVERALL_DEF_RANK",
            "GOALS_DEF_RANK",
            "goals_def_rank",
            "def_rank",
            "Def Rank",
        ),
    )
    tier = _first_valid_tier(
        row,
        (
            "DEF_TIER",
            "def_tier",
            "GOALS_DEF_TIER",
            "goals_def_tier",
            "Def Tier",
        ),
    )
    if rank == rank and not tier and n_teams and n_teams >= 2:
        try:
            tier = def_tier_from_overall_rank(int(rank), int(n_teams))
        except (TypeError, ValueError):
            tier = ""
    return rank, (tier or ""), metric


def assign_prop_aware_def_tier(
    df: pd.DataFrame,
    *,
    prop_col: str | None = None,
    out_rank_col: str = "OVERALL_DEF_RANK",
    out_tier_col: str = "DEF_TIER",
    metric_col: str = "def_metric",
) -> pd.DataFrame:
    """Overwrite display rank/tier with prop-aware selection; keep source cols intact."""
    out = df.copy()
    if prop_col is None:
        for c in ("prop_norm", "prop_type_normalized", "prop_type", "Prop", "stat_type"):
            if c in out.columns:
                prop_col = c
                break

    # Preserve goals overall before overwrite when not already snapshotted.
    if "GOALS_DEF_RANK" not in out.columns and "OVERALL_DEF_RANK" in out.columns:
        out["GOALS_DEF_RANK"] = out["OVERALL_DEF_RANK"]
    if "GOALS_DEF_TIER" not in out.columns:
        src_tier = "DEF_TIER" if "DEF_TIER" in out.columns else ("def_tier" if "def_tier" in out.columns else None)
        if src_tier:
            out["GOALS_DEF_TIER"] = out[src_tier]

    props = out[prop_col] if prop_col and prop_col in out.columns else pd.Series([""] * len(out), index=out.index)

    # League-scoped n_teams for quintile rebuild when only rank is present.
    n_by_league: dict[str, int] = {}
    if "league" in out.columns and "SHOTS_DEF_RANK" in out.columns:
        for lg, grp in out.groupby(out["league"].astype(str)):
            ranks = pd.to_numeric(grp["SHOTS_DEF_RANK"], errors="coerce")
            n_by_league[str(lg)] = int(ranks.max()) if ranks.notna().any() else 0

    ranks: list[object] = []
    tiers: list[str] = []
    metrics: list[str] = []
    for i in range(len(out)):
        r = out.iloc[i]
        lg = str(r.get("league", "") or "")
        n_teams = n_by_league.get(lg) or None
        if n_teams is None and "OVERALL_DEF_RANK" in out.columns:
            try:
                # crude fallback: max overall rank in frame
                n_teams = int(pd.to_numeric(out["OVERALL_DEF_RANK"], errors="coerce").max())
            except (TypeError, ValueError):
                n_teams = None
        rk, tier, metric = select_prop_aware_rank_tier(
            r, prop=props.iloc[i] if len(props) else None, n_teams=n_teams
        )
        ranks.append(rk if rk == rk else pd.NA)
        tiers.append(tier)
        metrics.append(metric)

    out[out_rank_col] = ranks
    out[out_tier_col] = tiers
    out[metric_col] = metrics
    # Keep lowercase mirror in sync for step7/step8 consumers.
    if "def_tier" in out.columns or out_tier_col == "DEF_TIER":
        out["def_tier"] = out[out_tier_col]
    return out
