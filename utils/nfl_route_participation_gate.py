"""NFL route / target participation soft attach + optional hard gates.

Built by ``Sports/NFL/scripts/build_nfl_route_participation.py``.

Soft-first: ``attach_participation()`` always joins badge columns.
Hard suppress rules stay behind ``PARTICIPATION_HARD_GATES_ENABLED`` until a
Week 2+ unique-game ledger validates them.

Hard rules (when enabled):
  - snap_pct < 40% -> suppress all props for that player
  - wopr < 0.50 on receiving_yards OVER -> block
  - target_share < 0.20 on receptions OVER -> block
  - Low WOPR + Elite secondary UNDER -> stacked signal (strengthens, never blocks)
  - Unknown participation -> pass through (never suppress on missing data)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from utils.nfl_player_names import norm_nfl_player_name

_REPO = Path(__file__).resolve().parents[1]

# Flip only after Week 2+ ledger confirms volume floors move hit rates.
PARTICIPATION_HARD_GATES_ENABLED = False

SNAP_FLOOR_PCT = 40.0
WOPR_OVER_FLOOR = 0.50
TARGET_SHARE_RECEPTIONS_OVER_FLOOR = 0.20

_REC_PROPS = frozenset(
    {
        "receiving_yards",
        "receptions",
        "longest_reception",
        "receiving_tds",
        "rush_rec_yds",
        "rec_yds",
    }
)


def _tok(v: object) -> str:
    return str(v or "").strip().lower().replace(" ", "_").replace("-", "_")


def _prop(r: dict[str, Any]) -> str:
    for k in ("prop", "Prop", "prop_type", "prop_type_normalized", "stat_type"):
        if k in r and r.get(k) not in (None, ""):
            return _tok(r.get(k))
    return ""


def _side(r: dict[str, Any]) -> str:
    for k in ("side", "Direction", "bet_direction", "recommended_side", "dir"):
        if k in r and r.get(k) not in (None, ""):
            return str(r.get(k) or "").strip().upper()
    return ""


def _num(r: dict[str, Any], *keys: str) -> float | None:
    for k in keys:
        if k not in r or r.get(k) in (None, ""):
            continue
        try:
            v = float(r.get(k))
        except (TypeError, ValueError):
            continue
        if v == v:
            return v
    return None


def load_route_participation(root: Path | None = None) -> pd.DataFrame:
    path = (root or _REPO) / "Sports" / "NFL" / "data" / "nfl_route_participation.csv"
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "player_norm" not in df.columns and "player" in df.columns:
        df = df.copy()
        df["player_norm"] = df["player"].map(norm_nfl_player_name)
    return df


def attach_participation(
    df: pd.DataFrame,
    root: Path | None = None,
    *,
    player_col: str | None = None,
) -> pd.DataFrame:
    """Left-join L3 participation onto prop rows. Safe no-op if CSV missing."""
    if df is None or df.empty:
        return df
    part = load_route_participation(root)
    if part.empty:
        return df

    out = df.copy()
    pcol = player_col
    if not pcol:
        pcol = (
            "player_name"
            if "player_name" in out.columns
            else ("player" if "player" in out.columns else "")
        )
    if not pcol:
        return out

    keep = [
        c
        for c in (
            "player_norm",
            "gsis_id",
            "snap_pct_L3",
            "snap_pct_season",
            "wopr_L3",
            "wopr_season",
            "target_share_L3",
            "target_share_season",
            "air_yards_share_L3",
            "route_pct_L3",
            "route_pct_season",
            "route_pct_source",
        )
        if c in part.columns
    ]
    r = part[keep].drop_duplicates(subset=["player_norm"], keep="last").copy()
    renames = {
        "snap_pct_L3": "part_snap_pct_L3",
        "snap_pct_season": "part_snap_pct_season",
        "gsis_id": "part_gsis_id",
    }
    r = r.rename(columns={k: v for k, v in renames.items() if k in r.columns})

    out["_pnorm"] = out[pcol].map(norm_nfl_player_name)
    out = out.merge(r, left_on="_pnorm", right_on="player_norm", how="left")
    out = out.drop(columns=["_pnorm", "player_norm"], errors="ignore")

    if "part_snap_pct_L3" in out.columns:
        if "snap_pct_L3" not in out.columns:
            out["snap_pct_L3"] = out["part_snap_pct_L3"]
        else:
            out["snap_pct_L3"] = out["snap_pct_L3"].fillna(out["part_snap_pct_L3"])
    if "part_snap_pct_season" in out.columns:
        if "snap_pct_season" not in out.columns:
            out["snap_pct_season"] = out["part_snap_pct_season"]
        else:
            out["snap_pct_season"] = out["snap_pct_season"].fillna(out["part_snap_pct_season"])

    return out


def participation_soft_signals(r: dict[str, Any]) -> list[str]:
    """Badge strings for display — never blocks."""
    badges: list[str] = []
    snap = _num(r, "snap_pct_L3", "part_snap_pct_L3", "snap_pct_season")
    wopr = _num(r, "wopr_L3", "wopr_season")
    tgt = _num(r, "target_share_L3", "target_share_season")
    if snap is not None:
        if snap < SNAP_FLOOR_PCT:
            badges.append(f"SnapLow {snap:.0f}%")
        elif snap >= 80:
            badges.append(f"SnapHigh {snap:.0f}%")
    if wopr is not None:
        if wopr < WOPR_OVER_FLOOR:
            badges.append(f"WOPRLow {wopr:.2f}")
        elif wopr >= 0.70:
            badges.append(f"WOPRHigh {wopr:.2f}")
    if tgt is not None and tgt < TARGET_SHARE_RECEPTIONS_OVER_FLOOR:
        badges.append(f"TgtLow {tgt:.0%}")

    sec = str(
        r.get("opp_secondary_tier") or r.get("Opp Secondary Tier") or r.get("def_tier") or ""
    ).strip()
    if (
        wopr is not None
        and wopr < WOPR_OVER_FLOOR
        and sec.lower() in ("elite", "above avg", "above")
        and _side(r) == "UNDER"
        and _prop(r) in _REC_PROPS
    ):
        badges.append("LowWOPR+EliteSec")
    return badges


def participation_gate_check(r: dict[str, Any]) -> tuple[bool, str]:
    """Return (allowed, reason). Missing data always allows."""
    if not PARTICIPATION_HARD_GATES_ENABLED:
        return True, "soft_only"

    snap = _num(r, "snap_pct_L3", "part_snap_pct_L3", "snap_pct_season", "Snap L3")
    wopr = _num(r, "wopr_L3", "wopr_season")
    tgt = _num(r, "target_share_L3", "target_share_season")

    if snap is None and wopr is None and tgt is None:
        return True, "unknown_pass"

    if snap is not None and snap < SNAP_FLOOR_PCT:
        return False, f"snap_pct<{SNAP_FLOOR_PCT:g}"

    prop = _prop(r)
    side = _side(r)
    if side == "OVER":
        if prop in ("receiving_yards", "rec_yds", "rush_rec_yds") and wopr is not None:
            if wopr < WOPR_OVER_FLOOR:
                return False, f"wopr<{WOPR_OVER_FLOOR:g}"
        if prop == "receptions" and tgt is not None:
            if tgt < TARGET_SHARE_RECEPTIONS_OVER_FLOOR:
                return False, f"target_share<{TARGET_SHARE_RECEPTIONS_OVER_FLOOR:g}"

    return True, "pass"
