"""Stamp per-stat opponent D onto graded rows.

Logging only — never eligibility. ``goblin_70_eligible`` / ``ticket_gate_passes``
keep reading overall ``def_tier``. This field exists so a later 3PM / PRA / Assists
audit does not have to reconstruct the prop axis after the fact.

Uses the same basketball by-stat CSVs as step8. Does not rebuild those CSVs at
grade time (empty stamp if the file is missing). Does not overwrite a value
already on the row (board-time snapshot wins).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.defense_tiers import normalize_def_tier_label

WNBA_FAMILY = frozenset({"WNBA", "WNBA1H", "WNBA1Q"})
NBA_FAMILY = frozenset({"NBA", "NBA1H", "NBA1Q"})
BASKETBALL_STAT_DEF_SPORTS = WNBA_FAMILY | NBA_FAMILY

_EMPTY = {
    "stat_def_category": "",
    "stat_def_rank": None,
    "stat_def_tier": "",
}


def lookup_stat_def_for_row(sport: object, opp: object, prop: object) -> dict[str, Any]:
    """Return {stat_def_category, stat_def_rank, stat_def_tier} for opponent × prop."""
    sport_u = str(sport or "").strip().upper()
    if sport_u not in BASKETBALL_STAT_DEF_SPORTS:
        return dict(_EMPTY)
    try:
        from utils import basketball_prop_defense as bpd

        if sport_u in WNBA_FAMILY:
            from utils.wnba_prop_defense import canon_team, default_csv_path
        else:
            from utils.nba_prop_defense import canon_team, default_csv_path

        path = Path(default_csv_path())
        if not path.is_file() or path.stat().st_size < 50:
            return dict(_EMPTY)
        info = bpd.lookup_stat_defense(
            opp, prop, csv_path=str(path), canon_team_fn=canon_team
        )
    except Exception:
        return dict(_EMPTY)
    tier = normalize_def_tier_label(info.get("stat_def_tier")) or ""
    rank = info.get("stat_def_rank")
    try:
        rank_i = int(rank) if rank is not None and str(rank).strip() != "" else None
    except (TypeError, ValueError):
        rank_i = None
    return {
        "stat_def_category": str(info.get("stat_def_category") or ""),
        "stat_def_rank": rank_i,
        "stat_def_tier": tier,
    }


def merge_stat_def_fields(
    *,
    sport: object,
    opp: object,
    prop: object,
    existing_tier: object = "",
    existing_rank: object = "",
    existing_category: object = "",
) -> dict[str, Any]:
    """Prefer values already on the row; fill gaps from the by-stat CSV."""
    tier = str(existing_tier or "").strip()
    cat = str(existing_category or "").strip()
    rank: object = existing_rank
    if rank is not None and str(rank).strip() in {"", "nan", "None", "none"}:
        rank = None
    if tier and cat and rank is not None:
        return {
            "stat_def_category": cat,
            "stat_def_rank": rank,
            "stat_def_tier": normalize_def_tier_label(tier) or tier,
        }
    info = lookup_stat_def_for_row(sport, opp, prop)
    if not tier:
        tier = str(info.get("stat_def_tier") or "")
    else:
        tier = normalize_def_tier_label(tier) or tier
    if not cat:
        cat = str(info.get("stat_def_category") or "")
    if rank is None and info.get("stat_def_rank") is not None:
        rank = info.get("stat_def_rank")
    return {
        "stat_def_category": cat,
        "stat_def_rank": rank,
        "stat_def_tier": tier,
    }
