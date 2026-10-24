"""WNBA prop-specific opponent defense ranks (allowed PTS/REB/AST/…).

Ranks: 1 = stingiest (lowest opp-allowed) for that stat.
Tiers: canonical Elite → Weak (legacy HARD/EASY mapped via defense_tiers).
Built from box-score game logs in proporacle_ref.db when CSV is missing.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd

from utils import basketball_prop_defense as bpd
from utils.wnba_defensive_efficiency import team_defensive_efficiency

PROP_TO_CAT: dict[str, str] = dict(bpd.PROP_TO_CAT)

SOFT_PRIORITY_PROPS: frozenset[str] = frozenset(
    {
        "Rebounds",
        "Rebs+Asts",
        "Pts+Asts",
        "Pts+Rebs+Asts",
        "3-PT Made",
        "FG Made",
        "FG Attempted",
        "Free Throws Made",
        "Free Throws Attempted",
        "Two Pointers Made",
        "Two Pointers Attempted",
    }
)

SOFT_UNDER_PROPS: frozenset[str] = frozenset(
    {
        "Rebounds",
        "Pts+Rebs+Asts",
        "Pts+Rebs",
        "Pts+Asts",
        "Assists",
        "Rebs+Asts",
    }
)

TEAM_MAP: dict[str, str] = {
    "ATL": "ATL",
    "CHI": "CHI",
    "CON": "CON",
    "CONN": "CON",
    "DAL": "DAL",
    "GS": "GS",
    "GSV": "GS",
    "IND": "IND",
    "LA": "LA",
    "LAS": "LA",
    "LV": "LV",
    "LVA": "LV",
    "MIN": "MIN",
    "NY": "NY",
    "NYL": "NY",
    "PHX": "PHX",
    "PHO": "PHX",
    "POR": "POR",
    "SEA": "SEA",
    "TOR": "TOR",
    "WSH": "WSH",
    "WAS": "WSH",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_csv_path() -> Path:
    return _repo_root() / "Sports" / "WNBA" / "data" / "wnba_defense_by_stat.csv"


def default_db_path() -> Path:
    return bpd.default_db_path()


def canon_team(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if not s or "/" in s:
        return ""
    return TEAM_MAP.get(s, s)


def prop_category(prop: object) -> str:
    return bpd.prop_category(prop)


def coarse_bucket_from_tier(tier: object) -> str:
    return bpd.coarse_bucket_from_tier(tier)


def coarse_bucket_from_rank(rank: object, n_teams: int) -> str:
    return bpd.coarse_bucket_from_rank(rank, n_teams)


def _tier_label(rank: float, n_teams: int) -> str:
    return bpd.tier_label_from_rank(rank, n_teams)


def _overall_rank_hook(summary: pd.DataFrame, db_path: Path) -> pd.Series:
    try:
        eff = team_defensive_efficiency(db_path=db_path)
    except Exception:
        return summary["pts_rank"]
    if eff is None or eff.empty:
        return summary["pts_rank"]
    team_col = next((c for c in ("TEAM_ABBREVIATION", "team", "TEAM") if c in eff.columns), None)
    rank_col = next(
        (c for c in ("DEF_EFF_RANK", "DEF_EFF_RANK", "def_eff_rank", "rank") if c in eff.columns),
        None,
    )
    if not team_col or not rank_col:
        return summary["pts_rank"]
    m = eff[[team_col, rank_col]].rename(columns={team_col: "team", rank_col: "DEF_EFF_RANK"})
    m["team"] = m["team"].astype(str).str.strip().str.upper().map(canon_team)
    merged = summary[["team"]].merge(m, on="team", how="left")
    return (
        pd.to_numeric(merged["DEF_EFF_RANK"], errors="coerce")
        .fillna(pd.to_numeric(summary["pts_rank"], errors="coerce"))
        .astype("Int64")
    )


def rebuild_defense_by_stat(
    db_path: Optional[Path] = None,
    out_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Aggregate opp-allowed stats from wnba game logs; write CSV; return frame."""
    return bpd.rebuild_defense_by_stat(
        sport_table="wnba",
        out_path=Path(out_path or default_csv_path()),
        db_path=Path(db_path) if db_path else default_db_path(),
        team_map=TEAM_MAP,
        exclude_teams={"COOP", "SPO"},
        overall_rank_hook=_overall_rank_hook,
    )


def load_defense_table(csv_path: str = "") -> pd.DataFrame:
    path = str(csv_path or default_csv_path())
    df = bpd.load_defense_table(path)
    if df.empty:
        rebuild_defense_by_stat(out_path=Path(path))
        bpd.clear_defense_table_cache()
        df = bpd.load_defense_table(path)
    if df.empty:
        return df
    out = df.copy()
    out["team"] = out["team"].astype(str).str.strip().str.upper().map(canon_team)
    return out


def clear_defense_cache() -> None:
    bpd.clear_defense_table_cache()


def lookup_stat_defense(
    opp: object,
    prop: object,
    *,
    csv_path: str = "",
) -> dict:
    """
    Return {category, rank, tier, coarse} for opponent × prop.
    ``stat_def_tier`` is the canonical 5-bucket label (Elite→Weak).
    """
    path = str(csv_path or default_csv_path())
    if not Path(path).is_file() or Path(path).stat().st_size < 50:
        rebuild_defense_by_stat(out_path=Path(path))
        bpd.clear_defense_table_cache()
    return bpd.lookup_stat_defense(
        opp, prop, csv_path=path, canon_team_fn=canon_team
    )


def soft_priority_enabled() -> bool:
    return str(os.getenv("PROPORACLE_WNBA_STAT_DEF_SOFT", "1")).strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def soft_priority_delta(
    *,
    sport: object,
    prop: object,
    direction: object,
    stat_def_tier: object,
) -> float:
    """
    Soft score nudge for whitelist props.
    OVER vs Weak/Below Avg (EASY coarse) +0.06 / Elite/Above Avg (HARD) -0.04;
    UNDER (subset) HARD +0.04 / EASY -0.03.
    """
    if not soft_priority_enabled():
        return 0.0
    if str(sport or "").strip().upper() != "WNBA":
        return 0.0
    p = str(prop or "").strip()
    d = str(direction or "").strip().upper()
    coarse = coarse_bucket_from_tier(stat_def_tier)
    if d.startswith("O"):
        if p not in SOFT_PRIORITY_PROPS:
            return 0.0
        if coarse == "EASY":
            return 0.06
        if coarse == "HARD":
            return -0.04
        return 0.0
    if d.startswith("U"):
        if p not in SOFT_UNDER_PROPS:
            return 0.0
        if coarse == "HARD":
            return 0.04
        if coarse == "EASY":
            return -0.03
        return 0.0
    return 0.0


def attach_stat_defense_columns(df: pd.DataFrame, *, csv_path: str = "") -> pd.DataFrame:
    """Add stat_def_category / stat_def_rank / stat_def_tier columns (WNBA rows)."""
    path = str(csv_path or default_csv_path())
    if not Path(path).is_file() or Path(path).stat().st_size < 50:
        rebuild_defense_by_stat(out_path=Path(path))
        bpd.clear_defense_table_cache()
    return bpd.attach_stat_defense_columns(
        df,
        sport="WNBA",
        csv_path=path,
        canon_team_fn=canon_team,
        sport_values={"WNBA"},
    )
