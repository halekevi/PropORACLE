"""Attach NFL QB mobility + red-zone context onto prop frames.

Soft columns only — never ticket-gate until unique-game ledger validates.
Built by:
  - Sports/NFL/scripts/build_nfl_qb_mobility.py
  - Sports/NFL/scripts/build_nfl_red_zone_rates.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_espn_context import canon_nfl_abbr
from utils.nfl_player_names import norm_nfl_player_name

_REPO = Path(__file__).resolve().parents[1]


def _read(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path)


def attach_nfl_qb_mobility(
    df: pd.DataFrame,
    root: Path | None = None,
    *,
    player_col: str = "player",
) -> pd.DataFrame:
    """Left-join qb_mobility_tier / scramble_rate / sack_rate by player_norm."""
    if df is None or df.empty:
        return df
    path = (root or _REPO) / "Sports" / "NFL" / "data" / "nfl_qb_mobility.csv"
    mob = _read(path)
    if mob.empty or "player_norm" not in mob.columns:
        return df
    keep = [
        c
        for c in (
            "player_norm",
            "qb_mobility_tier",
            "scramble_rate",
            "sack_rate",
            "rush_att_pg",
            "dropbacks",
            "sacks_taken",
        )
        if c in mob.columns
    ]
    # One row per player_norm (prefer highest dropbacks if multi-team).
    m = mob[keep].copy()
    if "dropbacks" in m.columns:
        m = m.sort_values("dropbacks", ascending=False)
    m = m.drop_duplicates("player_norm", keep="first")
    m = m.rename(
        columns={
            "sacks_taken": "qb_sacks_taken_pbp",
            "sack_rate": "qb_sack_rate_pbp",
            "scramble_rate": "qb_scramble_rate",
            "rush_att_pg": "qb_designed_rush_att_pg",
            "dropbacks": "qb_dropbacks",
        }
    )
    out = df.copy()
    if "player_norm" not in out.columns:
        src = player_col if player_col in out.columns else "player"
        out["player_norm"] = out[src].map(norm_nfl_player_name) if src in out.columns else ""
    out = out.merge(m, on="player_norm", how="left")
    return out


def attach_nfl_red_zone(
    df: pd.DataFrame,
    root: Path | None = None,
    *,
    team_col: str = "team",
    player_col: str = "player",
) -> pd.DataFrame:
    """Left-join team RZ rates + player RZ rush/target shares."""
    if df is None or df.empty:
        return df
    base = (root or _REPO) / "Sports" / "NFL" / "data"
    team = _read(base / "nfl_red_zone_team.csv")
    player = _read(base / "nfl_red_zone_player.csv")
    out = df.copy()

    if not team.empty and "team" in team.columns and team_col in out.columns:
        tcols = [
            c
            for c in (
                "team",
                "rz_rush_rate",
                "rz_pass_rate",
                "rz_td_rate",
                "rz_rush_td_rate",
                "rz_rush_tier",
                "rz_td_tier",
            )
            if c in team.columns
        ]
        t = team[tcols].copy()
        t["team"] = t["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
        t = t.rename(
            columns={
                "rz_rush_rate": "own_rz_rush_rate",
                "rz_pass_rate": "own_rz_pass_rate",
                "rz_td_rate": "own_rz_td_rate",
                "rz_rush_td_rate": "own_rz_rush_td_rate",
                "rz_rush_tier": "own_rz_rush_tier",
                "rz_td_tier": "own_rz_td_tier",
            }
        )
        out["_jk"] = out[team_col].map(lambda x: canon_nfl_abbr(x) or "")
        t = t.rename(columns={"team": "_jk"})
        out = out.merge(t, on="_jk", how="left").drop(columns=["_jk"])

    if not player.empty and "player_norm" in player.columns:
        pcols = [
            c
            for c in (
                "player_norm",
                "rz_rush_share",
                "rz_target_share",
                "rz_tds",
                "high_rz_rush",
                "high_rz_target",
            )
            if c in player.columns
        ]
        p = player[pcols].copy()
        p = p.sort_values("rz_tds" if "rz_tds" in p.columns else pcols[0], ascending=False)
        p = p.drop_duplicates("player_norm", keep="first")
        p = p.rename(
            columns={
                "rz_rush_share": "player_rz_rush_share",
                "rz_target_share": "player_rz_target_share",
                "rz_tds": "player_rz_tds",
                "high_rz_rush": "player_high_rz_rush",
                "high_rz_target": "player_high_rz_target",
            }
        )
        if "player_norm" not in out.columns:
            src = player_col if player_col in out.columns else "player"
            out["player_norm"] = out[src].map(norm_nfl_player_name) if src in out.columns else ""
        out = out.merge(p, on="player_norm", how="left")

    return out


def attach_nfl_context_layers(
    df: pd.DataFrame,
    root: Path | None = None,
    *,
    team_col: str = "team",
    player_col: str = "player",
) -> pd.DataFrame:
    """Attach QB mobility + red-zone soft context."""
    out = attach_nfl_qb_mobility(df, root, player_col=player_col)
    out = attach_nfl_red_zone(out, root, team_col=team_col, player_col=player_col)
    return out
