#!/usr/bin/env python3
"""Reshape CFB player volume into an NFL-shaped participation CSV.

Source: ``Sports/CFB/data/reference/cfb_player_volume.csv``.
No WOPR, no snap %. Shares only.

Writes::

  Sports/CFB/data/cfb_route_participation.csv
  Sports/CFB/data/cfb_route_participation_meta.json

Refresh::

  py -3.14 Sports/CFB/scripts/build_cfb_route_participation.py
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

from utils.cfb_unit_line_ranks import clean_cfb_abbr, now_iso  # noqa: E402
from utils.cfbd_client import infer_default_year  # noqa: E402

VOL = _CFB / "data" / "reference" / "cfb_player_volume.csv"
RANK = _CFB / "data" / "reference" / "cfb_team_unit_rankings.csv"
OUT = _CFB / "data" / "cfb_route_participation.csv"
OUT_META = _CFB / "data" / "cfb_route_participation_meta.json"


def build_participation(vol: pd.DataFrame, espn: pd.DataFrame, *, season: int) -> pd.DataFrame:
    id_map: dict[str, str] = {}
    if not espn.empty and "team_id" in espn.columns:
        for _, r in espn.iterrows():
            try:
                tid = str(int(float(r.get("team_id"))))
            except (TypeError, ValueError):
                continue
            abbr = clean_cfb_abbr(r.get("team_abbr"))
            if tid and abbr:
                id_map[tid] = abbr
    df = vol.copy()
    names = df.get("player_norm", pd.Series("", index=df.index)).astype(str).str.strip().str.lower()
    df = df[~names.isin(("", "team", "nan", "none"))].copy()
    df["player_norm"] = df.get("player_norm", pd.Series("", index=df.index)).astype(str)
    df["team"] = df.get("team_abbr", pd.Series("", index=df.index)).map(clean_cfb_abbr)
    tids = pd.to_numeric(df.get("team_id"), errors="coerce")
    df["team"] = [
        t if t else id_map.get("" if pd.isna(tid) else str(int(tid)), "")
        for t, tid in zip(df["team"].tolist(), tids.tolist())
    ]
    df = df[df["team"].astype(str).str.len() > 0].copy()
    df["season"] = pd.to_numeric(df.get("season"), errors="coerce").fillna(season).astype("Int64")
    keep = [
        c
        for c in (
            "player_norm",
            "espn_athlete_id",
            "team",
            "season",
            "volume_games",
            "rec_tgt_share",
            "rush_att_share",
            "pass_att_share",
            "rec_tgt_pg",
            "rush_att_pg",
            "pass_att_pg",
            "rec_pg",
        )
        if c in df.columns
    ]
    out = df[keep].copy()
    out["player"] = out["player_norm"]
    # Explicit: this file has no WOPR / snap columns.
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=0)
    args = ap.parse_args()
    season = int(args.season) or infer_default_year()
    if not VOL.is_file():
        print(f"[CFB part] missing {VOL}")
        return 1
    vol = pd.read_csv(VOL)
    espn = pd.read_csv(RANK) if RANK.is_file() else pd.DataFrame()
    out = build_participation(vol, espn, season=season)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    OUT_META.write_text(
        json.dumps(
            {
                "season": season,
                "built_at": now_iso(),
                "source": "cfb_player_volume.csv",
                "n": int(len(out)),
                "wopr": False,
                "snap_pct": False,
                "note": "Shares only. Usage floors inverted on tape — display, do not gate.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[CFB part] wrote {OUT} n={len(out)} (no WOPR)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
