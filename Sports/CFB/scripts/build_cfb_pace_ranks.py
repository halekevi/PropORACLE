#!/usr/bin/env python3
"""Build CFB pace ranks from ESPN unit rankings (plays/G + clock tier).

No PBP fetch. ``pace_rank`` 1 = fastest (most ``off_plays_pg``).

Writes::

  Sports/CFB/data/cfb_pace_ranks.csv

Refresh::

  py -3.14 Sports/CFB/scripts/build_cfb_pace_ranks.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_SCRIPT = Path(__file__).resolve().parent
_CFB = _SCRIPT.parent
_REPO = _SCRIPT.parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.cfb_unit_line_ranks import clean_cfb_abbr, now_iso, percentile_tier, rank_higher_better  # noqa: E402
from utils.cfbd_client import infer_default_year  # noqa: E402

RANK_CSV = _CFB / "data" / "reference" / "cfb_team_unit_rankings.csv"
OUT = _CFB / "data" / "cfb_pace_ranks.csv"
OUT_META = _CFB / "data" / "cfb_pace_ranks_meta.json"


def build_pace(espn: pd.DataFrame, *, season: int) -> pd.DataFrame:
    df = espn.copy()
    df["team"] = df["team_abbr"].map(clean_cfb_abbr)
    df["conference"] = df.get("conference_name", pd.Series("", index=df.index)).fillna("").astype(str)
    n = max(int(len(df)), 1)
    df["plays_per_game"] = pd.to_numeric(df.get("off_plays_pg"), errors="coerce")
    df["pace_rank"] = rank_higher_better(df["plays_per_game"])
    df["tier"] = df["pace_rank"].map(lambda r: percentile_tier(r, n))
    df["clock_tier"] = df.get("off_clock_tier", pd.Series("", index=df.index))
    df["season"] = season
    cols = [
        "team",
        "conference",
        "season",
        "games",
        "pace_rank",
        "plays_per_game",
        "clock_tier",
        "tier",
    ]
    return df[[c for c in cols if c in df.columns]]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=0)
    args = ap.parse_args()
    season = int(args.season) or infer_default_year()
    if not RANK_CSV.is_file():
        print(f"[CFB pace] missing {RANK_CSV}")
        return 1
    espn = pd.read_csv(RANK_CSV)
    out = build_pace(espn, season=season)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    OUT_META.write_text(
        json.dumps(
            {
                "season": season,
                "built_at": now_iso(),
                "source": "cfb_team_unit_rankings.csv off_plays_pg + off_clock_tier",
                "n": int(len(out)),
                "note": "No CFBD PBP. pace_rank 1=fastest (most plays/G).",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[CFB pace] wrote {OUT} n={len(out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
