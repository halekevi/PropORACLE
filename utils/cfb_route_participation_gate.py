"""CFB target / rush-share soft attach. No WOPR, no snap %.

Built by ``Sports/CFB/scripts/build_cfb_route_participation.py`` from
``cfb_player_volume.csv``. Usage-share floors were inverted on tape — do not
hard-gate. ``PARTICIPATION_HARD_GATES_ENABLED`` stays False until a
``graded_props_*.json`` unique-game ledger shows shares are not inverted.
The weekly O/D markdown is not that ledger.

Unknown participation always passes through.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]

# Soft attach only. Do not flip to True from a weekly O/D rebuild, a populated
# cfb_route_participation.csv, or by copying NFL's flag.
#
# Safe to enable only after all of:
#   1. Unique-game HIT/MISS ledger on graded_props_*.json (same tape as the
#      usage-share audit — not the O/D markdown percentile tiers).
#   2. rec_tgt_share / rush_att_share (and any new route/snap field) are
#      monotonic in the expected direction on that tape. Usage floors at
#      25–50% were inverted on Rush Yards / Rec Yards / Receptions; a share
#      column that looks like volume is not enough.
#   3. CFB-specific suppress rules are written from that ledger. Do not paste
#      NFL snap% / WOPR floors — this file has no WOPR and no snap %.
#
# Same failure shape as CBB injury join / is_wcbb_slate: a dormant True path
# that looks unused until the day it fires. Leave False until (1)–(3) exist.
PARTICIPATION_HARD_GATES_ENABLED = False


def _player_norm(v: object) -> str:
    return " ".join(str(v or "").strip().lower().split())


def load_route_participation(root: Path | None = None) -> pd.DataFrame:
    path = (root or _REPO) / "Sports" / "CFB" / "data" / "cfb_route_participation.csv"
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "player_norm" not in df.columns and "player" in df.columns:
        df = df.copy()
        df["player_norm"] = df["player"].map(_player_norm)
    return df


def attach_participation(
    df: pd.DataFrame,
    root: Path | None = None,
    *,
    player_col: str | None = None,
) -> pd.DataFrame:
    """Left-join volume shares onto prop rows. Safe no-op if CSV missing."""
    if df is None or df.empty:
        return df
    part = load_route_participation(root)
    if part.empty:
        return df

    out = df.copy()
    pcol = player_col
    if not pcol:
        pcol = (
            "Player"
            if "Player" in out.columns
            else ("player" if "player" in out.columns else "")
        )
    if not pcol:
        return out

    keep = [
        c
        for c in (
            "player_norm",
            "team",
            "rec_tgt_share",
            "rush_att_share",
            "pass_att_share",
            "volume_games",
        )
        if c in part.columns
    ]
    if "player_norm" not in keep:
        return out
    r = part[keep].drop_duplicates(subset=["player_norm"], keep="last").copy()
    r = r.rename(
        columns={
            "rec_tgt_share": "part_rec_tgt_share",
            "rush_att_share": "part_rush_att_share",
            "pass_att_share": "part_pass_att_share",
            "volume_games": "part_volume_games",
        }
    )
    out["_pnorm"] = out[pcol].map(_player_norm)
    out = out.merge(r, left_on="_pnorm", right_on="player_norm", how="left")
    return out.drop(columns=["_pnorm", "player_norm"], errors="ignore")


def participation_soft_signals(r: dict[str, Any]) -> list[str]:
    """Badge strings for display — never blocks. No WOPR."""
    badges: list[str] = []
    tgt = r.get("part_rec_tgt_share")
    rush = r.get("part_rush_att_share")
    try:
        if tgt is not None and tgt == tgt:
            badges.append(f"TgtShare {float(tgt):.0%}")
    except (TypeError, ValueError):
        pass
    try:
        if rush is not None and rush == rush:
            badges.append(f"RushShare {float(rush):.0%}")
    except (TypeError, ValueError):
        pass
    return badges


def participation_gate_check(r: dict[str, Any]) -> tuple[bool, str]:
    """Return (allowed, reason). Missing data always allows.

    The True branch is intentionally still a pass-through. Enabling the flag
    without a CFB ledger must not start suppressing. Do not copy NFL rules here.
    """
    if not PARTICIPATION_HARD_GATES_ENABLED:
        return True, "soft_only"
    return True, "flag_on_but_unvalidated_passthrough"
