#!/usr/bin/env python3
"""Build NFL offensive pace/tempo ranks from free nflverse pbp.

Metrics (offense = ``posteam``):
  - plays_per_game
  - plays_per_drive
  - sec_per_play (drive TOP / plays)
  - no_huddle_rate

``pace_rank`` 1 = fastest (high volume + low sec/play). Soft context only.

Writes::

  Sports/NFL/data/nfl_pace_ranks.csv

Refresh::

  py -3.14 Sports/NFL/scripts/build_nfl_pace_ranks.py --season 2026
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_SCRIPT = Path(__file__).resolve().parent
_NFL = _SCRIPT.parent
_REPO = _SCRIPT.parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nfl_espn_context import canon_nfl_abbr  # noqa: E402

UA = {"User-Agent": "PropOracle/1.0 (+https://github.com/halekevi/PropORACLE)"}
NFLVERSE = "https://github.com/nflverse/nflverse-data/releases/download"
OUT = _NFL / "data" / "nfl_pace_ranks.csv"
OUT_META = _NFL / "data" / "nfl_pace_ranks_meta.json"

TEAMS_32 = (
    "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE",
    "DAL", "DEN", "DET", "GB", "HOU", "IND", "JAX", "KC",
    "LAC", "LAR", "LV", "MIA", "MIN", "NE", "NO", "NYG",
    "NYJ", "PHI", "PIT", "SEA", "SF", "TB", "TEN", "WSH",
)

PLAY_TYPES = frozenset({"pass", "run", "qb_kneel", "qb_spike"})


def infer_season(today: datetime | None = None) -> int:
    d = today or datetime.now(timezone.utc)
    return d.year if d.month >= 9 else d.year - 1


def _get_bytes(url: str, timeout: float = 180.0) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _top_to_seconds(raw: object) -> float:
    s = str(raw or "").strip()
    if ":" not in s:
        return float("nan")
    a, b = s.split(":", 1)
    try:
        return int(a) * 60 + int(float(b))
    except (TypeError, ValueError):
        return float("nan")


def _tier(rank: object, n: int = 32) -> str:
    try:
        r = int(rank)
    except (TypeError, ValueError):
        return "Avg"
    if r <= max(1, n // 5):
        return "Fast"
    if r <= max(2, (2 * n) // 5):
        return "Above Avg"
    if r <= max(3, (3 * n) // 5):
        return "Avg"
    if r <= max(4, (4 * n) // 5):
        return "Below Avg"
    return "Slow"


def load_pbp(season: int) -> pd.DataFrame:
    url = f"{NFLVERSE}/pbp/play_by_play_{season}.csv.gz"
    usecols = [
        "game_id",
        "play_id",
        "posteam",
        "play_type",
        "no_huddle",
        "fixed_drive",
        "drive_time_of_possession",
        "season_type",
    ]
    raw = gzip.decompress(_get_bytes(url))
    df = pd.read_csv(io.BytesIO(raw), usecols=usecols)
    if "season_type" in df.columns:
        df = df[df["season_type"].astype(str).str.upper().isin(("REG", "REGULAR"))].copy()
    df["posteam"] = df["posteam"].map(lambda x: canon_nfl_abbr(x) or "")
    df = df[df["posteam"].isin(TEAMS_32)].copy()
    df = df[df["play_type"].astype(str).isin(PLAY_TYPES)].copy()
    return df


def build_pace(df: pd.DataFrame, season: int) -> pd.DataFrame:
    d = df.dropna(subset=["posteam", "fixed_drive", "game_id"]).copy()
    d["no_huddle"] = d["no_huddle"].fillna(False).astype(bool)

    drives = (
        d.groupby(["game_id", "posteam", "fixed_drive"], as_index=False)
        .agg(
            plays=("play_id", "count"),
            top=("drive_time_of_possession", "first"),
            no_huddle=("no_huddle", "mean"),
        )
    )
    drives["top_sec"] = drives["top"].map(_top_to_seconds)
    drives["sec_per_play"] = np.where(
        drives["plays"] > 0, drives["top_sec"] / drives["plays"], np.nan
    )

    by_team = drives.groupby("posteam", as_index=False).agg(
        plays_per_drive=("plays", "mean"),
        sec_per_play=("sec_per_play", "mean"),
        drives=("fixed_drive", "count"),
        no_huddle_rate=("no_huddle", "mean"),
    )
    ppg = (
        d.groupby(["game_id", "posteam"])
        .size()
        .groupby("posteam")
        .mean()
        .rename("plays_per_game")
    )
    games = (
        d.groupby("posteam")["game_id"].nunique().rename("games")
    )
    by_team = by_team.merge(ppg, left_on="posteam", right_index=True, how="left")
    by_team = by_team.merge(games, left_on="posteam", right_index=True, how="left")
    by_team = by_team.rename(columns={"posteam": "team"})

    # Ensure all 32 teams present
    base = pd.DataFrame({"team": list(TEAMS_32)})
    out = base.merge(by_team, on="team", how="left")
    out["season"] = season

    # Fast = high PPG + low sec/play
    ppg_rank = out["plays_per_game"].rank(method="min", ascending=False)
    spp_rank = out["sec_per_play"].rank(method="min", ascending=True)
    out["pace_rank"] = (
        (ppg_rank.astype(float).fillna(16) + spp_rank.astype(float).fillna(16)) / 2.0
    ).rank(method="min").astype("Int64")
    out["plays_per_game_rank"] = ppg_rank.astype("Int64")
    out["sec_per_play_rank"] = spp_rank.astype("Int64")
    out["tier"] = out["pace_rank"].map(_tier)

    cols = [
        "team",
        "season",
        "games",
        "drives",
        "pace_rank",
        "plays_per_game",
        "plays_per_drive",
        "sec_per_play",
        "no_huddle_rate",
        "plays_per_game_rank",
        "sec_per_play_rank",
        "tier",
    ]
    return out[cols]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=0)
    args = ap.parse_args()
    season = int(args.season) or infer_season()

    print(f"[pace] season={season}")
    df = load_pbp(season)
    print(f"  pbp offensive plays={len(df)}")
    out = build_pace(df, season)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    meta: dict[str, Any] = {
        "season": season,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "source": f"play_by_play_{season}.csv.gz",
        "path": str(OUT.relative_to(_REPO)),
        "notes": [
            "pace_rank 1=fastest (high plays/game + low sec/play).",
            "Soft context only — do not hard-gate until Week 2+ ledger.",
            "Supports rec/rush UNDER when opp or game script is Slow; OVERs when Fast.",
        ],
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"  wrote {OUT}")
    print(f"  wrote {OUT_META}")
    print("\nFastest 8:")
    print(
        out.nsmallest(8, "pace_rank")[
            ["team", "pace_rank", "plays_per_game", "sec_per_play", "no_huddle_rate", "tier"]
        ].to_string(index=False)
    )
    print("\nSlowest 5:")
    print(
        out.nlargest(5, "pace_rank")[
            ["team", "pace_rank", "plays_per_game", "sec_per_play", "tier"]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
