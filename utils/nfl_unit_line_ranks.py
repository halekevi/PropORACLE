"""Load / attach NFL OL·DL·secondary·box unit ranks onto prop frames.

Built by ``Sports/NFL/scripts/build_nfl_unit_line_ranks.py``.
Does not change ``def_tier`` — soft context columns only.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_espn_context import canon_nfl_abbr

_REPO = Path(__file__).resolve().parents[1]


def _read(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "team" in df.columns:
        df = df.copy()
        df["team"] = df["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    return df


def load_unit_line_tables(root: Path | None = None) -> dict[str, pd.DataFrame]:
    base = (root or _REPO) / "Sports" / "NFL" / "data"
    return {
        "ol": _read(base / "nfl_ol_ranks.csv"),
        "dl": _read(base / "nfl_dl_ranks.csv"),
        "secondary": _read(base / "nfl_secondary_ranks.csv"),
        "box": _read(base / "nfl_box_ranks.csv"),
    }


def _merge_on(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_key: str,
    renames: dict[str, str],
) -> pd.DataFrame:
    if right.empty or left_key not in left.columns:
        return left
    cols = ["team", *[c for c in renames if c in right.columns]]
    r = right[cols].rename(columns=renames).copy()
    r["_jk"] = r["team"]
    r = r.drop(columns=["team"])
    out = left.copy()
    out["_jk"] = out[left_key].map(lambda x: canon_nfl_abbr(x) or "")
    out = out.merge(r, on="_jk", how="left")
    return out.drop(columns=["_jk"])


def attach_nfl_unit_line_ranks(
    df: pd.DataFrame,
    root: Path | None = None,
    *,
    team_col: str = "team",
    opp_col: str = "opp_team",
) -> pd.DataFrame:
    """Left-join own OL + opp DL/secondary/box. Safe no-op if CSVs missing."""
    if df is None or df.empty:
        return df
    tables = load_unit_line_tables(root)
    out = df

    out = _merge_on(
        out,
        tables["ol"],
        left_key=team_col,
        renames={
            "ol_rank": "own_ol_rank",
            "pass_block_rank": "own_pass_block_rank",
            "run_block_rank": "own_run_block_rank",
            "tier": "own_ol_tier",
            "sack_rate_allowed": "own_sack_rate_allowed",
            "pressure_rate_allowed": "own_pressure_rate_allowed",
            "yards_before_contact": "own_yards_before_contact",
        },
    )
    out = _merge_on(
        out,
        tables["dl"],
        left_key=opp_col,
        renames={
            "dl_rank": "opp_dl_rank",
            "pass_rush_rank": "opp_pass_rush_rank",
            "run_stop_rank": "opp_dl_run_stop_rank",
            "tier": "opp_dl_tier",
            "sacks_pg": "opp_sacks_pg",
            "yards_before_contact_allowed": "opp_dl_ybc_allowed",
        },
    )
    out = _merge_on(
        out,
        tables["secondary"],
        left_key=opp_col,
        renames={
            "secondary_rank": "opp_secondary_rank",
            "coverage_rank": "opp_coverage_rank",
            "yards_per_target": "opp_yards_per_target",
            "completion_pct_allowed": "opp_completion_pct_allowed",
            "tier": "opp_secondary_tier",
            "coverage_scheme": "opp_coverage_scheme",
        },
    )
    out = _merge_on(
        out,
        tables["box"],
        left_key=opp_col,
        renames={
            "box_rank": "opp_box_rank",
            "blitz_rate": "opp_blitz_rate",
            "tier": "opp_box_tier",
            "yards_after_contact_allowed": "opp_yac_allowed",
        },
    )
    return out
