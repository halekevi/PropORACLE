#!/usr/bin/env python3
"""Build NFL red-zone offense rates from free nflverse pbp.

Team + skill-player RZ shares (yardline_100 <= 20). Soft context for rush/rec
TD watches — not a ticket gate until unique-game ledger validates.

Writes::

  Sports/NFL/data/nfl_red_zone_team.csv
  Sports/NFL/data/nfl_red_zone_player.csv
  Sports/NFL/data/nfl_red_zone_meta.json

Refresh::

  py -3.14 Sports/NFL/scripts/build_nfl_red_zone_rates.py --seasons 2025,2026
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
OUT_TEAM = _NFL / "data" / "nfl_red_zone_team.csv"
OUT_PLAYER = _NFL / "data" / "nfl_red_zone_player.csv"
OUT_META = _NFL / "data" / "nfl_red_zone_meta.json"

TEAMS_32 = (
    "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE",
    "DAL", "DEN", "DET", "GB", "HOU", "IND", "JAX", "KC",
    "LAC", "LAR", "LV", "MIA", "MIN", "NE", "NO", "NYG",
    "NYJ", "PHI", "PIT", "SEA", "SF", "TB", "TEN", "WSH",
)

USECOLS = [
    "game_id",
    "play_id",
    "posteam",
    "play_type",
    "season_type",
    "yardline_100",
    "rush_attempt",
    "pass_attempt",
    "touchdown",
    "rusher_player_name",
    "receiver_player_name",
    "passer_player_name",
    "td_team",
]


def infer_season(today: datetime | None = None) -> int:
    d = today or datetime.now(timezone.utc)
    return d.year if d.month >= 9 else d.year - 1


def _get_bytes(url: str, timeout: float = 240.0) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _tier_share(rank: object, n: int) -> str:
    try:
        r = int(rank)
    except (TypeError, ValueError):
        return "Avg"
    if n <= 0:
        return "Avg"
    if r <= max(1, n // 5):
        return "Elite"
    if r <= max(2, (2 * n) // 5):
        return "Above"
    if r <= max(3, (3 * n) // 5):
        return "Avg"
    if r <= max(4, (4 * n) // 5):
        return "Below"
    return "Weak"


def load_name_map(season: int) -> pd.DataFrame:
    """Map nflverse short names → display names (all skill positions)."""
    url = f"{NFLVERSE}/stats_player/stats_player_week_{season}.csv.gz"
    try:
        raw = gzip.decompress(_get_bytes(url))
    except Exception:
        return pd.DataFrame()
    header = pd.read_csv(io.BytesIO(raw), nrows=0)
    cols = [c for c in ("player_name", "player_display_name") if c in header.columns]
    if len(cols) < 2:
        return pd.DataFrame()
    df = pd.read_csv(io.BytesIO(raw), usecols=cols)
    df = df.dropna(subset=cols).drop_duplicates("player_name", keep="last")
    df["short_norm"] = df["player_name"].map(norm_nfl_player_name)
    df["player_norm"] = df["player_display_name"].map(norm_nfl_player_name)
    return df[["short_norm", "player_norm", "player_display_name"]].rename(
        columns={"player_display_name": "player_full"}
    )


def load_pbp(season: int) -> pd.DataFrame:
    url = f"{NFLVERSE}/pbp/play_by_play_{season}.csv.gz"
    raw = gzip.decompress(_get_bytes(url))
    df = pd.read_csv(io.BytesIO(raw), low_memory=False)
    have = [c for c in USECOLS if c in df.columns]
    df = df[have].copy()
    if "season_type" in df.columns:
        df = df[df["season_type"].astype(str).str.upper().isin(("REG", "REGULAR"))].copy()
    df["posteam"] = df["posteam"].map(lambda x: canon_nfl_abbr(x) or "")
    df = df[df["posteam"].isin(TEAMS_32)].copy()
    df["yardline_100"] = pd.to_numeric(df.get("yardline_100"), errors="coerce")
    df["season"] = season
    return df


def build_team(rz: pd.DataFrame) -> pd.DataFrame:
    d = rz.copy()
    d["is_rush"] = pd.to_numeric(d.get("rush_attempt"), errors="coerce").fillna(0) > 0
    d["is_pass"] = pd.to_numeric(d.get("pass_attempt"), errors="coerce").fillna(0) > 0
    # Fallback when attempt flags missing
    if d["is_rush"].sum() == 0:
        d["is_rush"] = d["play_type"].astype(str).eq("run")
    if d["is_pass"].sum() == 0:
        d["is_pass"] = d["play_type"].astype(str).eq("pass")
    d["is_td"] = pd.to_numeric(d.get("touchdown"), errors="coerce").fillna(0) > 0
    d["is_rush_td"] = d["is_rush"] & d["is_td"]
    d["is_pass_td"] = d["is_pass"] & d["is_td"]

    g = (
        d.groupby("posteam", dropna=False)
        .agg(
            rz_plays=("play_id", "count"),
            rz_rush=("is_rush", "sum"),
            rz_pass=("is_pass", "sum"),
            rz_td=("is_td", "sum"),
            rz_rush_td=("is_rush_td", "sum"),
            rz_pass_td=("is_pass_td", "sum"),
            games=("game_id", "nunique"),
        )
        .reset_index()
        .rename(columns={"posteam": "team"})
    )
    plays = g["rz_plays"].clip(lower=1)
    g["rz_rush_rate"] = g["rz_rush"] / plays
    g["rz_pass_rate"] = g["rz_pass"] / plays
    g["rz_td_rate"] = g["rz_td"] / plays
    g["rz_rush_td_rate"] = g["rz_rush_td"] / plays
    n = len(g)
    g["rz_rush_rate_rank"] = g["rz_rush_rate"].rank(ascending=False, method="min")
    g["rz_td_rate_rank"] = g["rz_td_rate"].rank(ascending=False, method="min")
    g["rz_rush_tier"] = g["rz_rush_rate_rank"].map(lambda r: _tier_share(r, n))
    g["rz_td_tier"] = g["rz_td_rate_rank"].map(lambda r: _tier_share(r, n))
    return g.sort_values("rz_td_rate_rank").reset_index(drop=True)


def build_player(rz: pd.DataFrame, name_map: pd.DataFrame) -> pd.DataFrame:
    d = rz.copy()
    d["is_rush"] = pd.to_numeric(d.get("rush_attempt"), errors="coerce").fillna(0) > 0
    d["is_pass"] = pd.to_numeric(d.get("pass_attempt"), errors="coerce").fillna(0) > 0
    if d["is_rush"].sum() == 0:
        d["is_rush"] = d["play_type"].astype(str).eq("run")
    if d["is_pass"].sum() == 0:
        d["is_pass"] = d["play_type"].astype(str).eq("pass")
    d["is_td"] = pd.to_numeric(d.get("touchdown"), errors="coerce").fillna(0) > 0

    short_to_norm = (
        dict(zip(name_map["short_norm"], name_map["player_norm"])) if not name_map.empty else {}
    )
    short_to_full = (
        dict(zip(name_map["short_norm"], name_map["player_full"])) if not name_map.empty else {}
    )

    def _norm(s: object) -> str:
        sn = norm_nfl_player_name(s)
        return short_to_norm.get(sn, sn)

    def _full(s: object) -> str:
        sn = norm_nfl_player_name(s)
        return short_to_full.get(sn, str(s or ""))

    team_rush = d.loc[d["is_rush"]].groupby("posteam").size().rename("team_rz_rush")
    team_tgt = d.loc[d["is_pass"]].groupby("posteam").size().rename("team_rz_pass")

    rush = d.loc[d["is_rush"] & d["rusher_player_name"].notna()].copy()
    rush["player_norm"] = rush["rusher_player_name"].map(_norm)
    rush["player"] = rush["rusher_player_name"].map(_full)
    ragg = (
        rush.groupby(["player_norm", "player", "posteam"], dropna=False)
        .agg(rz_touches=("play_id", "count"), rz_tds=("is_td", "sum"), games=("game_id", "nunique"))
        .reset_index()
    )
    ragg = ragg.merge(team_rush, left_on="posteam", right_index=True, how="left")
    ragg["rz_rush_share"] = ragg["rz_touches"] / ragg["team_rz_rush"].clip(lower=1)
    ragg["rz_target_share"] = 0.0

    tgt = d.loc[d["is_pass"] & d["receiver_player_name"].notna()].copy()
    tgt["player_norm"] = tgt["receiver_player_name"].map(_norm)
    tgt["player"] = tgt["receiver_player_name"].map(_full)
    tagg = (
        tgt.groupby(["player_norm", "player", "posteam"], dropna=False)
        .agg(rz_touches=("play_id", "count"), rz_tds=("is_td", "sum"), games=("game_id", "nunique"))
        .reset_index()
    )
    tagg = tagg.merge(team_tgt, left_on="posteam", right_index=True, how="left")
    tagg["rz_target_share"] = tagg["rz_touches"] / tagg["team_rz_pass"].clip(lower=1)
    tagg["rz_rush_share"] = 0.0

    out = pd.concat([ragg, tagg], ignore_index=True)
    out = out.rename(columns={"posteam": "team"})
    out["team"] = out["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    collapsed = (
        out.groupby(["player_norm", "player", "team"], dropna=False)
        .agg(
            rz_rush_share=("rz_rush_share", "max"),
            rz_target_share=("rz_target_share", "max"),
            rz_touches=("rz_touches", "sum"),
            rz_tds=("rz_tds", "sum"),
            games=("games", "max"),
        )
        .reset_index()
    )
    collapsed["high_rz_rush"] = collapsed["rz_rush_share"] >= 0.40
    collapsed["high_rz_target"] = collapsed["rz_target_share"] >= 0.20
    return collapsed.sort_values(["rz_tds", "rz_touches"], ascending=False).reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Build NFL red-zone rates")
    ap.add_argument("--seasons", default="", help="Comma seasons (default prior+current)")
    args = ap.parse_args()
    cur = infer_season()
    if str(args.seasons).strip():
        seasons = [int(x) for x in str(args.seasons).split(",") if x.strip()]
    else:
        seasons = [cur - 1, cur]

    frames: list[pd.DataFrame] = []
    name_maps: list[pd.DataFrame] = []
    sources: dict[str, str] = {}
    for s in seasons:
        try:
            df = load_pbp(s)
            print(f"  season={s} pbp_rows={len(df)}")
            frames.append(df)
            sources[str(s)] = f"play_by_play_{s}"
        except Exception as exc:
            print(f"  season={s} FAIL: {exc}")
            sources[str(s)] = f"FAIL: {exc}"
        try:
            nm = load_name_map(s)
            print(f"  season={s} name_map={len(nm)}")
            name_maps.append(nm)
        except Exception as exc:
            print(f"  season={s} name_map FAIL: {exc}")

    if not frames:
        raise SystemExit("no pbp loaded")
    all_pbp = pd.concat(frames, ignore_index=True)
    name_map = (
        pd.concat([m for m in name_maps if not m.empty], ignore_index=True).drop_duplicates(
            "short_norm", keep="last"
        )
        if name_maps
        else pd.DataFrame()
    )
    rz = all_pbp[all_pbp["yardline_100"].notna() & (all_pbp["yardline_100"] <= 20)].copy()
    print(f"  red-zone plays={len(rz)}")
    team = build_team(rz)
    player = build_player(rz, name_map)

    OUT_TEAM.parent.mkdir(parents=True, exist_ok=True)
    team.to_csv(OUT_TEAM, index=False, encoding="utf-8-sig")
    player.to_csv(OUT_PLAYER, index=False, encoding="utf-8-sig")
    meta = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "seasons": seasons,
        "rz_plays": int(len(rz)),
        "n_teams": int(len(team)),
        "n_players": int(len(player)),
        "sources": sources,
        "note": "Soft context only. yardline_100 <= 20.",
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_TEAM} teams={len(team)}")
    print(f"Wrote {OUT_PLAYER} players={len(player)}")


if __name__ == "__main__":
    main()
