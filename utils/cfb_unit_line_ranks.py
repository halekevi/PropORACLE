"""Load / attach CFB OL·DL·secondary·box·pace ranks onto prop frames.

Ranks are **views** of existing tables:

- ``ol_index`` / ``dl_index`` / ``secondary_index`` from
  ``utils.cfb_line_secondary.load_line_secondary_table`` (do not recompute z)
- ESPN YPG / sacks / plays from ``cfb_team_unit_rankings.csv``
- Box CSV is an alias of ``dl_index`` (no second front-seven z)

Does not change ``def_tier``. Soft context only.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from utils.cfb_line_secondary import load_line_secondary_table
from utils.cfb_playoff_metadata import cfb_abbr_lookup_keys, norm_cfb_team_abbr

_REPO = Path(__file__).resolve().parents[1]

# Brief bands (not equal quintiles, not NFL 1–8). Rank 1 = best of n FBS.
_ELITE_PCT = 0.15
_ABOVE_PCT = 0.30
_AVG_PCT = 0.65
_BELOW_PCT = 0.85


def clean_cfb_abbr(x: object) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return ""
    return norm_cfb_team_abbr(s) or s.upper()


def percentile_tier(rank: object, n: int) -> str:
    """Elite top 15% · Above 16–30% · Avg 31–65% · Below 66–85% · Weak bottom 15%."""
    if rank is None or (isinstance(rank, float) and pd.isna(rank)):
        return ""
    try:
        r = int(rank)
    except (TypeError, ValueError):
        return ""
    n = max(int(n), 1)
    r = min(max(r, 1), n)
    elite_max = max(1, math.ceil(n * _ELITE_PCT))
    above_max = max(elite_max, math.ceil(n * _ABOVE_PCT))
    avg_max = max(above_max, math.ceil(n * _AVG_PCT))
    below_max = max(avg_max, math.ceil(n * _BELOW_PCT))
    if r <= elite_max:
        return "Elite"
    if r <= above_max:
        return "Above"
    if r <= avg_max:
        return "Avg"
    if r <= below_max:
        return "Below"
    return "Weak"


def cfb_week0_saturday(year: int) -> date:
    """First Saturday on or after Aug 23 (typical CFB Week 0)."""
    d = date(int(year), 8, 23)
    return d + timedelta(days=(5 - d.weekday()) % 7)


def cfb_week_from_date(d: date) -> int:
    """0-based CFB week: Week 0 Saturday + 7-day bins."""
    year = d.year if d.month >= 8 else d.year - 1
    w0 = cfb_week0_saturday(year)
    delta = (d - w0).days
    if delta < 0:
        return 0
    return int(delta // 7)


# Dates that can print on the O/D board (step8 pairings) with no graded_props
# file and no recovery path. Board pairings ≠ hit-rate tape.
PERMANENT_GRADE_HOLES = frozenset({date(2026, 9, 19)})


def od_board_authority_note(slate: date | None) -> str:
    """Header line: live board vs graded tape, including the 9/19 permanent hole."""
    general = (
        "This file is the live weekly ranking board (step8 pairings + ESPN YPG). "
        "HIT/MISS and margin×D live on graded_props_*.json — join any OL/DL/secondary "
        "composite to that tape to audit; do not read percentile tiers as hit rates."
    )
    if slate in PERMANENT_GRADE_HOLES:
        iso = slate.isoformat()
        return (
            f"{general} This board's step8 date is {iso}. Pairings, percentile tiers, "
            "and matchup leans print here; the graded tape has nothing for that "
            "Saturday and never will (permanent hole). Seeing 9/19 on this board and "
            "nowhere in the hit-rate ledger is expected, not a missing-grade bug."
        )
    return (
        f"{general} 2026-09-19 remains a permanent grade hole: a week-3 archive of "
        "this board can list that Saturday's pairings while graded_props has no 9/19 "
        "file. That mismatch is expected, not a missing-grade bug."
    )


def _read(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "team" in df.columns:
        df = df.copy()
        df["team"] = df["team"].map(clean_cfb_abbr)
    return df


def load_unit_line_tables(root: Path | None = None) -> dict[str, pd.DataFrame]:
    base = (root or _REPO) / "Sports" / "CFB" / "data"
    return {
        "ol": _read(base / "cfb_ol_ranks.csv"),
        "dl": _read(base / "cfb_dl_ranks.csv"),
        "secondary": _read(base / "cfb_secondary_ranks.csv"),
        "box": _read(base / "cfb_box_ranks.csv"),
        "pace": _read(base / "cfb_pace_ranks.csv"),
    }


def load_espn_unit_rankings(root: Path | None = None) -> pd.DataFrame:
    path = (root or _REPO) / "Sports" / "CFB" / "data" / "reference" / "cfb_team_unit_rankings.csv"
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "team_abbr" in df.columns:
        df = df.copy()
        df["team"] = df["team_abbr"].map(clean_cfb_abbr)
    return df


def rank_higher_better(series: pd.Series) -> pd.Series:
    """Rank 1 = highest value (stronger OL/DL/sec z, more sacks, more YPG)."""
    return pd.to_numeric(series, errors="coerce").rank(method="min", ascending=False).astype("Int64")


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
    lookup: dict[str, pd.Series] = {}
    for rec in r.to_dict(orient="records"):
        team = str(rec.get("team") or "").strip().upper()
        row = {k: rec[k] for k in rec if k != "team"}
        for key in cfb_abbr_lookup_keys(team) | {team}:
            lookup[str(key).upper()] = row
    out = left.copy()
    extra = {v: [] for v in renames.values() if v}
    for raw in out[left_key].tolist():
        abbr = clean_cfb_abbr(raw) or str(raw or "").strip().upper()
        hit = lookup.get(abbr) or lookup.get(str(raw or "").strip().upper())
        for dest in extra:
            extra[dest].append(None if not hit else hit.get(dest))
    for dest, vals in extra.items():
        out[dest] = vals
    return out


def attach_cfb_unit_line_ranks(
    df: pd.DataFrame,
    root: Path | None = None,
    *,
    team_col: str = "Team",
    opp_col: str = "Opp",
) -> pd.DataFrame:
    """Left-join own OL/pace + opp DL/secondary/box/pace. No-op if CSVs missing.

    Never writes ``def_tier``.
    """
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
            "ol_index": "own_ol_index",
            "tier": "own_ol_tier",
            "sack_rate_allowed_rank": "own_sack_rate_allowed_rank",
        },
    )
    out = _merge_on(
        out,
        tables["pace"],
        left_key=team_col,
        renames={
            "pace_rank": "own_pace_rank",
            "plays_per_game": "own_plays_per_game",
            "tier": "own_pace_tier",
            "clock_tier": "own_clock_tier",
        },
    )
    out = _merge_on(
        out,
        tables["dl"],
        left_key=opp_col,
        renames={
            "dl_rank": "opp_dl_rank",
            "dl_index": "opp_dl_index",
            "tier": "opp_dl_tier",
            "espn_sacks_pg": "opp_espn_sacks_pg",
            "espn_sacks_rank": "opp_espn_sacks_rank",
        },
    )
    out = _merge_on(
        out,
        tables["secondary"],
        left_key=opp_col,
        renames={
            "secondary_rank": "opp_secondary_rank",
            "secondary_index": "opp_secondary_index",
            "tier": "opp_secondary_tier",
        },
    )
    out = _merge_on(
        out,
        tables["box"],
        left_key=opp_col,
        renames={
            "box_rank": "opp_box_rank",
            "dl_index": "opp_box_index",
            "tier": "opp_box_tier",
        },
    )
    out = _merge_on(
        out,
        tables["pace"],
        left_key=opp_col,
        renames={
            "pace_rank": "opp_pace_rank",
            "plays_per_game": "opp_plays_per_game",
            "tier": "opp_pace_tier",
        },
    )
    return out


def line_secondary_or_empty(root: Path | None = None) -> pd.DataFrame:
    """Existing CFBD z table only — never a second composite."""
    path = None
    if root is not None:
        path = Path(root) / "Sports" / "CFB" / "data" / "reference" / "cfb_line_secondary.csv"
    return load_line_secondary_table(path)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
