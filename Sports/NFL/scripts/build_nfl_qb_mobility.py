#!/usr/bin/env python3
"""Build NFL QB mobility tiers from free nflverse pbp.

Classifies passers as Pocket / Mixed / Mobile from scramble rate and
designed-rush volume. Soft context only — unlocks sacks_taken UNDER splits;
do not wire into ticket gates until unique-game ledger validates.

Writes::

  Sports/NFL/data/nfl_qb_mobility.csv
  Sports/NFL/data/nfl_qb_mobility_meta.json

Refresh::

  py -3.14 Sports/NFL/scripts/build_nfl_qb_mobility.py --seasons 2025,2026
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
OUT = _NFL / "data" / "nfl_qb_mobility.csv"
OUT_META = _NFL / "data" / "nfl_qb_mobility_meta.json"

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
    "passer_player_id",
    "passer_player_name",
    "rusher_player_id",
    "rusher_player_name",
    "qb_scramble",
    "qb_kneel",
    "sack",
    "qb_dropback",
    "rushing_yards",
]


def infer_season(today: datetime | None = None) -> int:
    d = today or datetime.now(timezone.utc)
    return d.year if d.month >= 9 else d.year - 1


def _get_bytes(url: str, timeout: float = 240.0) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def load_qb_name_map(season: int) -> pd.DataFrame:
    """Map nflverse short names / ids → display names (QBs)."""
    url = f"{NFLVERSE}/stats_player/stats_player_week_{season}.csv.gz"
    try:
        raw = gzip.decompress(_get_bytes(url))
    except Exception:
        return pd.DataFrame()
    header = pd.read_csv(io.BytesIO(raw), nrows=0)
    cols = [
        c
        for c in ("player_name", "player_display_name", "position", "player_id")
        if c in header.columns
    ]
    if "player_name" not in cols or "player_display_name" not in cols:
        return pd.DataFrame()
    df = pd.read_csv(io.BytesIO(raw), usecols=cols)
    if "position" in df.columns:
        df = df[df["position"].astype(str).str.upper().eq("QB")]
    df = df.dropna(subset=["player_name", "player_display_name"]).drop_duplicates(
        "player_name", keep="last"
    )
    df["short_norm"] = df["player_name"].map(norm_nfl_player_name)
    df["player_norm"] = df["player_display_name"].map(norm_nfl_player_name)
    if "player_id" in df.columns:
        df = df.drop_duplicates("player_id", keep="last")
    return df.rename(columns={"player_display_name": "player_full"})


def load_pbp(season: int) -> pd.DataFrame:
    url = f"{NFLVERSE}/pbp/play_by_play_{season}.csv.gz"
    raw = gzip.decompress(_get_bytes(url))
    # Some early-season files miss optional cols — read all then select.
    df = pd.read_csv(io.BytesIO(raw), low_memory=False)
    have = [c for c in USECOLS if c in df.columns]
    df = df[have].copy()
    if "season_type" in df.columns:
        df = df[df["season_type"].astype(str).str.upper().isin(("REG", "REGULAR"))].copy()
    df["posteam"] = df["posteam"].map(lambda x: canon_nfl_abbr(x) or "")
    df = df[df["posteam"].isin(TEAMS_32)].copy()
    df["season"] = season
    return df


def _tier_from_scramble(rate: float, rush_att_pg: float) -> str:
    """Pocket / Mixed / Mobile from scramble rate + designed rush volume."""
    if rate != rate:  # NaN
        return ""
    # Mobile: scramblers or designed-rush QBs (Lamar-class).
    if rate >= 0.085 or rush_att_pg >= 6.0:
        return "Mobile"
    if rate <= 0.035 and rush_att_pg < 3.0:
        return "Pocket"
    return "Mixed"


def build_mobility(
    frames: list[pd.DataFrame],
    name_maps: list[pd.DataFrame],
    *,
    min_dropbacks: int = 40,
) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    for c in ("qb_scramble", "sack", "qb_dropback", "rushing_yards", "qb_kneel"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    maps = [m for m in name_maps if m is not None and not m.empty]
    if maps:
        name_map = pd.concat(maps, ignore_index=True)
        if "player_id" in name_map.columns:
            name_map = name_map.drop_duplicates("player_id", keep="last")
        else:
            name_map = name_map.drop_duplicates("short_norm", keep="last")
    else:
        name_map = pd.DataFrame()
    # Prefer id map; fall back to short_norm for rows missing id.
    id_to_norm: dict[str, str] = {}
    id_to_full: dict[str, str] = {}
    short_to_norm: dict[str, str] = {}
    short_to_full: dict[str, str] = {}
    if not name_map.empty:
        if "player_id" in name_map.columns:
            for _, r in name_map.dropna(subset=["player_id"]).iterrows():
                id_to_norm[str(r["player_id"])] = r["player_norm"]
                id_to_full[str(r["player_id"])] = r["player_full"]
        for _, r in name_map.iterrows():
            short_to_norm[r["short_norm"]] = r["player_norm"]
            short_to_full[r["short_norm"]] = r["player_full"]
    qb_ids = set(id_to_norm)

    def _norm_from(id_col: str, name_col: str, row: pd.Series) -> str:
        pid = str(row.get(id_col) or "")
        if pid and pid in id_to_norm:
            return id_to_norm[pid]
        sn = norm_nfl_player_name(row.get(name_col))
        return short_to_norm.get(sn, sn)

    def _full_from(id_col: str, name_col: str, row: pd.Series) -> str:
        pid = str(row.get(id_col) or "")
        if pid and pid in id_to_full:
            return id_to_full[pid]
        sn = norm_nfl_player_name(row.get(name_col))
        return short_to_full.get(sn, str(row.get(name_col) or ""))

    # Dropbacks + sacks taken attributed to *passer*.
    has_passer = df["passer_player_name"].notna() if "passer_player_name" in df.columns else pd.Series(False, index=df.index)
    if "qb_dropback" in df.columns:
        drop_mask = has_passer & (df["qb_dropback"].fillna(0) > 0)
    else:
        drop_mask = has_passer & df["play_type"].astype(str).eq("pass")
    drops = df.loc[drop_mask].copy()
    drops["player_norm"] = [
        _norm_from("passer_player_id", "passer_player_name", r) for _, r in drops.iterrows()
    ]
    drops["player"] = [
        _full_from("passer_player_id", "passer_player_name", r) for _, r in drops.iterrows()
    ]
    drops["is_sack"] = (drops.get("sack", 0).fillna(0) > 0).astype(int)
    qb = (
        drops.groupby(["player_norm", "player", "posteam"], dropna=False)
        .agg(
            dropbacks=("play_id", "count"),
            sacks_taken=("is_sack", "sum"),
            games=("game_id", "nunique"),
            seasons=("season", "nunique"),
        )
        .reset_index()
        .rename(columns={"posteam": "team"})
    )

    # Scrambles: rusher with qb_scramble=1, restrict to known QB ids when available.
    scr = df[(df.get("qb_scramble", 0).fillna(0) > 0) & df["rusher_player_name"].notna()].copy()
    if not scr.empty:
        if qb_ids and "rusher_player_id" in scr.columns:
            scr = scr[scr["rusher_player_id"].astype(str).isin(qb_ids)]
        scr["player_norm"] = [
            _norm_from("rusher_player_id", "rusher_player_name", r) for _, r in scr.iterrows()
        ]
        sagg = (
            scr.groupby("player_norm")
            .agg(scrambles=("play_id", "count"), scramble_yds=("rushing_yards", "sum"))
            .reset_index()
        )
        qb = qb.merge(sagg, on="player_norm", how="left")
    else:
        qb["scrambles"] = 0
        qb["scramble_yds"] = 0.0

    # Designed QB rushes: run, not scramble, not kneel, rusher is known QB id.
    rush = df[df["play_type"].astype(str).eq("run")].copy()
    if not rush.empty and "rusher_player_name" in rush.columns:
        is_scr = rush.get("qb_scramble", 0).fillna(0) > 0
        is_kneel = rush.get("qb_kneel", 0).fillna(0) > 0
        designed = rush[(~is_scr) & (~is_kneel)].copy()
        if qb_ids and "rusher_player_id" in designed.columns:
            designed = designed[designed["rusher_player_id"].astype(str).isin(qb_ids)]
        else:
            designed["player_norm"] = [
                _norm_from("rusher_player_id", "rusher_player_name", r)
                for _, r in designed.iterrows()
            ]
            designed = designed[designed["player_norm"].isin(set(qb["player_norm"]))]
        if "player_norm" not in designed.columns:
            designed["player_norm"] = [
                _norm_from("rusher_player_id", "rusher_player_name", r)
                for _, r in designed.iterrows()
            ]
        ragg = (
            designed.groupby("player_norm")
            .agg(
                designed_rush_att=("play_id", "count"),
                designed_rush_yds=("rushing_yards", "sum"),
            )
            .reset_index()
        )
        qb = qb.merge(ragg, on="player_norm", how="left")
    else:
        qb["designed_rush_att"] = 0
        qb["designed_rush_yds"] = 0.0

    qb["scrambles"] = pd.to_numeric(qb.get("scrambles"), errors="coerce").fillna(0)
    qb["designed_rush_att"] = pd.to_numeric(qb.get("designed_rush_att"), errors="coerce").fillna(0)
    qb["designed_rush_yds"] = pd.to_numeric(qb.get("designed_rush_yds"), errors="coerce").fillna(0)
    qb["scramble_rate"] = qb["scrambles"] / qb["dropbacks"].clip(lower=1)
    qb["sack_rate"] = qb["sacks_taken"] / qb["dropbacks"].clip(lower=1)
    qb["rush_att_pg"] = qb["designed_rush_att"] / qb["games"].clip(lower=1)
    qb = qb[qb["dropbacks"] >= int(min_dropbacks)].copy()
    # Prefer primary team (most dropbacks) per player_norm.
    qb = qb.sort_values("dropbacks", ascending=False).drop_duplicates("player_norm", keep="first")
    qb["qb_mobility_tier"] = [
        _tier_from_scramble(float(r), float(a))
        for r, a in zip(qb["scramble_rate"], qb["rush_att_pg"])
    ]
    qb["team"] = qb["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    qb = qb.sort_values(["qb_mobility_tier", "scramble_rate"], ascending=[True, False])
    return qb.reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Build NFL QB mobility tiers")
    ap.add_argument(
        "--seasons",
        default="",
        help="Comma seasons (default: prior+current RS)",
    )
    ap.add_argument("--min-dropbacks", type=int, default=40)
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
            nm = load_qb_name_map(s)
            print(f"  season={s} qb_name_map={len(nm)}")
            name_maps.append(nm)
        except Exception as exc:
            print(f"  season={s} name_map FAIL: {exc}")

    out = build_mobility(frames, name_maps, min_dropbacks=args.min_dropbacks)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    meta = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "seasons": seasons,
        "min_dropbacks": args.min_dropbacks,
        "n_qb": int(len(out)),
        "tier_counts": out["qb_mobility_tier"].value_counts().to_dict() if not out.empty else {},
        "sources": sources,
        "note": "Soft context only. Do not ticket-gate until unique-game soft-attach validates.",
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Wrote {OUT} rows={len(out)}")
    if not out.empty:
        print(out[["player", "team", "qb_mobility_tier", "scramble_rate", "rush_att_pg", "dropbacks"]].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
