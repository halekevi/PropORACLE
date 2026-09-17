#!/usr/bin/env python3
"""Build NFL route / target participation from free nflverse releases.

Sources (no auth):
  - snap_counts_{season}.csv.gz          -> offense snap %
  - stats_player_week_{season}.csv.gz    -> target_share, air_yards_share, WOPR
  - pbp_participation_{season}.csv       -> true route % when published (2025 ok;
                                           2026 often missing early season)

WOPR is nflverse's weighted opportunity rating
(``1.5 * target_share + 0.7 * air_yards_share``). stats_player_week already
ships that column; we keep it rather than re-derive from raw pbp.

Writes::

  Sports/NFL/data/nfl_route_participation.csv
  Sports/NFL/data/nfl_route_participation_games.csv
  Sports/NFL/data/nfl_route_participation_meta.json

Refresh (Tuesday after grades)::

  py -3.14 Sports/NFL/scripts/build_nfl_route_participation.py --season 2026 --week 2
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
from utils.nfl_player_names import norm_nfl_player_name  # noqa: E402

UA = {"User-Agent": "PropOracle/1.0 (+https://github.com/halekevi/PropORACLE)"}
NFLVERSE = "https://github.com/nflverse/nflverse-data/releases/download"

OUT = _NFL / "data" / "nfl_route_participation.csv"
OUT_GAMES = _NFL / "data" / "nfl_route_participation_games.csv"
OUT_META = _NFL / "data" / "nfl_route_participation_meta.json"

SKILL_POS = frozenset({"WR", "TE", "RB", "FB"})


def infer_season(today: datetime | None = None) -> int:
    d = today or datetime.now(timezone.utc)
    return d.year if d.month >= 9 else d.year - 1


def _get_bytes(url: str, timeout: float = 180.0) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _to_pct(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return s
    sample = s.dropna()
    if (sample <= 1.5).mean() >= 0.8:
        return s * 100.0
    return s


def load_snap_counts(season: int) -> pd.DataFrame:
    url = f"{NFLVERSE}/snap_counts/snap_counts_{season}.csv.gz"
    raw = gzip.decompress(_get_bytes(url))
    df = pd.read_csv(io.BytesIO(raw))
    if "game_type" in df.columns:
        reg = df[df["game_type"].astype(str).str.upper().eq("REG")]
        if not reg.empty:
            df = reg
    df["team"] = df["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    df["week"] = pd.to_numeric(df.get("week"), errors="coerce")
    df["snap_pct"] = _to_pct(df["offense_pct"])
    df["player_norm"] = df["player"].map(norm_nfl_player_name)
    df["position"] = df.get("position", pd.Series("", index=df.index)).astype(str).str.upper()
    return df


def load_stats_week(season: int) -> pd.DataFrame:
    url = f"{NFLVERSE}/stats_player/stats_player_week_{season}.csv.gz"
    raw = gzip.decompress(_get_bytes(url))
    usecols = [
        "player_id",
        "player_name",
        "player_display_name",
        "position",
        "week",
        "team",
        "targets",
        "receptions",
        "receiving_yards",
        "receiving_air_yards",
        "target_share",
        "air_yards_share",
        "wopr",
        "season_type",
    ]
    header = pd.read_csv(io.BytesIO(raw), nrows=0)
    cols = [c for c in usecols if c in header.columns]
    df = pd.read_csv(io.BytesIO(raw), usecols=cols)
    if "season_type" in df.columns:
        reg = df[df["season_type"].astype(str).str.upper().isin(("REG", "REGULAR"))]
        if not reg.empty:
            df = reg
    df["team"] = df["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    df["week"] = pd.to_numeric(df.get("week"), errors="coerce")
    df["player_display_name"] = df.get(
        "player_display_name", df.get("player_name", pd.Series("", index=df.index))
    )
    df["player_norm"] = df["player_display_name"].map(norm_nfl_player_name)
    if "player_name" in df.columns:
        df["player_short_norm"] = df["player_name"].map(norm_nfl_player_name)
    df["position"] = df.get("position", pd.Series("", index=df.index)).astype(str).str.upper()
    for c in ("target_share", "air_yards_share", "wopr"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def probe_participation(season: int) -> dict[str, Any]:
    """Check whether pbp_participation exists (true route/man-zone)."""
    url = f"{NFLVERSE}/pbp_participation/pbp_participation_{season}.csv"
    try:
        req = urllib.request.Request(url, method="HEAD", headers=UA)
        with urllib.request.urlopen(req, timeout=45) as resp:
            ok = 200 <= int(resp.status) < 300
        return {"available": ok, "url": url}
    except Exception as exc:
        return {"available": False, "url": url, "error": str(exc)}


def build_game_rows(
    snaps: pd.DataFrame,
    stats: pd.DataFrame,
    *,
    season: int,
    through_week: int | None,
) -> pd.DataFrame:
    s = snaps.copy()
    t = stats.copy()
    if through_week is not None:
        s = s[s["week"] <= through_week]
        t = t[t["week"] <= through_week]

    skill = s[s["position"].isin(SKILL_POS)].copy()
    keep_snap = [
        c
        for c in (
            "game_id",
            "season",
            "week",
            "player",
            "player_norm",
            "pfr_player_id",
            "position",
            "team",
            "opponent",
            "offense_snaps",
            "snap_pct",
        )
        if c in skill.columns
    ]
    skill = skill[keep_snap]

    keep_stats = [
        c
        for c in (
            "player_id",
            "player_display_name",
            "player_name",
            "player_norm",
            "player_short_norm",
            "position",
            "week",
            "team",
            "targets",
            "receptions",
            "receiving_yards",
            "receiving_air_yards",
            "target_share",
            "air_yards_share",
            "wopr",
        )
        if c in t.columns
    ]
    t = t[keep_stats]

    merged = skill.merge(
        t,
        on=["player_norm", "team", "week"],
        how="left",
        suffixes=("", "_stat"),
    )
    if "player_display_name" in merged.columns:
        merged["player"] = merged["player"].fillna(merged["player_display_name"])
    merged["gsis_id"] = merged["player_id"] if "player_id" in merged.columns else pd.NA
    merged["season"] = season
    # Route % proxy until GSIS participation explode lands for the season.
    merged["route_pct"] = merged["snap_pct"]
    merged["route_pct_source"] = "snap_proxy"
    merged["wopr"] = pd.to_numeric(merged.get("wopr"), errors="coerce")
    merged["target_share"] = pd.to_numeric(merged.get("target_share"), errors="coerce")
    merged["air_yards_share"] = pd.to_numeric(merged.get("air_yards_share"), errors="coerce")
    need = merged["wopr"].isna() & merged["target_share"].notna() & merged["air_yards_share"].notna()
    merged.loc[need, "wopr"] = (
        1.5 * merged.loc[need, "target_share"] + 0.7 * merged.loc[need, "air_yards_share"]
    )
    return merged


def aggregate_players(games: pd.DataFrame) -> pd.DataFrame:
    if games.empty:
        return pd.DataFrame()
    work = games.sort_values(["player_norm", "week"]).copy()
    rows: list[dict[str, Any]] = []
    for key, grp in work.groupby("player_norm", sort=False):
        if not key:
            continue
        last = grp.iloc[-1]
        snap = pd.to_numeric(grp["snap_pct"], errors="coerce")
        wopr = pd.to_numeric(grp["wopr"], errors="coerce")
        tgt = pd.to_numeric(grp["target_share"], errors="coerce")
        air = pd.to_numeric(grp["air_yards_share"], errors="coerce")
        route = pd.to_numeric(grp["route_pct"], errors="coerce")
        last3 = grp.tail(3)
        rows.append(
            {
                "player": str(last.get("player") or "").strip(),
                "player_norm": key,
                "gsis_id": str(last.get("gsis_id") or "").strip(),
                "pfr_player_id": str(last.get("pfr_player_id") or "").strip(),
                "team": str(last.get("team") or "").strip(),
                "position": str(last.get("position") or "").strip(),
                "season": int(last.get("season") or 0),
                "as_of_week": int(pd.to_numeric(last.get("week"), errors="coerce") or 0),
                "games": int(len(grp)),
                "snap_pct_L3": round(float(snap.tail(3).mean()), 2) if snap.notna().any() else np.nan,
                "snap_pct_season": round(float(snap.mean()), 2) if snap.notna().any() else np.nan,
                "wopr_L3": round(float(wopr.tail(3).mean()), 4) if wopr.notna().any() else np.nan,
                "wopr_season": round(float(wopr.mean()), 4) if wopr.notna().any() else np.nan,
                "target_share_L3": round(float(tgt.tail(3).mean()), 4) if tgt.notna().any() else np.nan,
                "target_share_season": round(float(tgt.mean()), 4) if tgt.notna().any() else np.nan,
                "air_yards_share_L3": round(float(air.tail(3).mean()), 4) if air.notna().any() else np.nan,
                "route_pct_L3": round(float(route.tail(3).mean()), 2) if route.notna().any() else np.nan,
                "route_pct_season": round(float(route.mean()), 2) if route.notna().any() else np.nan,
                "route_pct_source": str(last.get("route_pct_source") or "snap_proxy"),
                "targets_L3": int(pd.to_numeric(last3.get("targets"), errors="coerce").fillna(0).sum()),
                "receptions_L3": int(
                    pd.to_numeric(last3.get("receptions"), errors="coerce").fillna(0).sum()
                ),
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values(["team", "wopr_L3"], ascending=[True, False]).reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=0)
    ap.add_argument(
        "--week",
        type=int,
        default=0,
        help="Include games through this week (0 = all available)",
    )
    args = ap.parse_args()
    season = int(args.season) or infer_season()
    through = int(args.week) or None

    print(f"[route] season={season} through_week={through or 'all'}")
    snaps = load_snap_counts(season)
    print(f"  snap_counts rows={len(snaps)}")
    stats = load_stats_week(season)
    print(f"  stats_player_week rows={len(stats)}")
    part_meta = probe_participation(season)
    print(f"  pbp_participation available={part_meta.get('available')}")

    games = build_game_rows(snaps, stats, season=season, through_week=through)
    print(f"  game skill rows={len(games)}")
    players = aggregate_players(games)
    print(f"  players={len(players)}")

    if not games.empty:
        wk = int(through or games["week"].min())
        w1 = games[games["week"] == wk]
        print(f"\nName format sample (week {wk}):")
        cols = [
            c
            for c in (
                "player",
                "player_display_name",
                "player_name",
                "player_norm",
                "snap_pct",
                "wopr",
                "target_share",
            )
            if c in w1.columns
        ]
        print(w1.head(12)[cols].to_string(index=False))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    game_cols = [
        c
        for c in (
            "season",
            "week",
            "game_id",
            "player",
            "player_norm",
            "gsis_id",
            "pfr_player_id",
            "team",
            "opponent",
            "position",
            "offense_snaps",
            "snap_pct",
            "route_pct",
            "route_pct_source",
            "targets",
            "receptions",
            "receiving_yards",
            "target_share",
            "air_yards_share",
            "wopr",
            "player_display_name",
            "player_name",
        )
        if c in games.columns
    ]
    games[game_cols].to_csv(OUT_GAMES, index=False)
    players.to_csv(OUT, index=False)

    meta: dict[str, Any] = {
        "season": season,
        "through_week": through,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "players": int(len(players)),
        "game_rows": int(len(games)),
        "path": str(OUT.relative_to(_REPO)),
        "games_path": str(OUT_GAMES.relative_to(_REPO)),
        "pbp_participation": part_meta,
        "sources": [
            f"snap_counts_{season}.csv.gz",
            f"stats_player_week_{season}.csv.gz",
        ],
        "notes": [
            "WOPR from nflverse stats_player_week (1.5*tgt_share + 0.7*air_share).",
            "route_pct currently snap_pct proxy for WR/TE/RB until GSIS participation explode.",
            "Soft context first — hard gates behind PARTICIPATION_HARD_GATES_ENABLED.",
            "Name join uses utils.nfl_player_names.norm_nfl_player_name (DJ↔D.J., drop III).",
        ],
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"\n  wrote {OUT}")
    print(f"  wrote {OUT_GAMES}")
    print(f"  wrote {OUT_META}")

    if not players.empty:
        print("\nTop WOPR L3 (skill):")
        print(
            players.nlargest(10, "wopr_L3")[
                ["player", "team", "position", "snap_pct_L3", "wopr_L3", "target_share_L3"]
            ].to_string(index=False)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
