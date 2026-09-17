#!/usr/bin/env python3
"""
Build ``Sports/NFL/data/nfl_snap_pct_cache.json`` from public nflverse snap counts.

Source (no auth):
  https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_{season}.csv.gz

Refresh::

  py -3.14 Sports/NFL/scripts/build_nfl_snap_pct_cache.py
  py -3.14 Sports/NFL/scripts/build_nfl_snap_pct_cache.py --season 2025

Then re-run step4c (NFL_PIPELINE_ACTIVE=1) so minutes_tier soft uses the cache.

Cache shape (consumed by ``step4c_attach_role_context_nfl.py``)::

  {
    "meta": {"season": 2025, "source": "...", "players": N},
    "players": {
      "justin jefferson": {
        "player": "Justin Jefferson",
        "pfr_player_id": "JeffJu00",
        "snap_pct_L3": 92.5,
        "snap_pct_season": 88.1,
        "role_stability_score": 0.91,
        "offense_pct": 88.1
      },
      ...
    }
  }

offense_pct values in nflverse are 0–1; we store 0–100 snap percentages.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

_SCRIPT_DIR = Path(__file__).resolve().parent
_NFL_ROOT = _SCRIPT_DIR.parent
_REPO_ROOT = _SCRIPT_DIR.parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_DEFAULT_OUT = _NFL_ROOT / "data" / "nfl_snap_pct_cache.json"

NFLVERSE_SNAP_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/"
    "snap_counts/snap_counts_{season}.csv.gz"
)
HEADERS = {
    "User-Agent": "PropOracle/1.0 (+https://github.com/proporacle)",
    "Accept": "application/octet-stream,*/*",
}


from utils.nfl_player_names import norm_nfl_player_name as _norm_name


def infer_default_season(today: datetime | None = None) -> int:
    """NFL season year: before Sept, prefer prior completed season."""
    d = today or datetime.now(timezone.utc)
    return d.year if d.month >= 9 else d.year - 1


def _to_pct(series: pd.Series) -> pd.Series:
    """nflverse offense_pct is usually 0–1; accept 0–100 too."""
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return s
    # If majority of values are <= 1.5, treat as fraction.
    sample = s.dropna()
    if (sample <= 1.5).mean() >= 0.8:
        return s * 100.0
    return s


def aggregate_snap_cache(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Collapse game-level snap rows → per-player L3 / season / stability."""
    if df is None or df.empty:
        return {}
    work = df.copy()
    if "game_type" in work.columns:
        # Prefer regular season; keep postseason only if no REG rows for a player.
        reg = work[work["game_type"].astype(str).str.upper().eq("REG")]
        if not reg.empty:
            work = reg
    need = {"player", "offense_pct"}
    if not need.issubset(set(work.columns)):
        raise ValueError(f"snap CSV missing columns {need - set(work.columns)}")

    work["offense_pct"] = _to_pct(work["offense_pct"])
    work["week"] = pd.to_numeric(work.get("week"), errors="coerce")
    work = work.dropna(subset=["player", "offense_pct"])
    work = work.sort_values(["player", "week"], ascending=[True, True])

    players: dict[str, dict[str, Any]] = {}
    for player, grp in work.groupby("player", sort=False):
        pcts = grp["offense_pct"].astype(float)
        season = float(pcts.mean())
        last3 = pcts.tail(3)
        l3 = float(last3.mean()) if len(last3) else season
        # Stability: inverse of recent std (scaled 0–1). Low variance → high stability.
        if len(last3) >= 2:
            std = float(last3.std(ddof=0))
            stability = max(0.0, min(1.0, 1.0 - (std / 50.0)))
        else:
            stability = 1.0 if season == season else None
        name = str(player).strip()
        key = _norm_name(name)
        pfr = ""
        if "pfr_player_id" in grp.columns:
            raw = grp["pfr_player_id"].dropna().astype(str)
            if not raw.empty:
                pfr = str(raw.iloc[-1]).strip()
        rec: dict[str, Any] = {
            "player": name,
            "pfr_player_id": pfr,
            "snap_pct_L3": round(l3, 2),
            "snap_pct_season": round(season, 2),
            "role_stability_score": None if stability is None else round(float(stability), 3),
            "offense_pct": round(season, 2),
            "games": int(len(pcts)),
        }
        players[key] = rec
        if pfr:
            players[pfr] = rec
    return players


def download_snap_counts(season: int, *, session: requests.Session | None = None) -> pd.DataFrame:
    url = NFLVERSE_SNAP_URL.format(season=season)
    sess = session or requests.Session()
    r = sess.get(url, headers=HEADERS, timeout=120)
    if r.status_code == 404:
        raise FileNotFoundError(f"nflverse snap_counts_{season} not found (404): {url}")
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), compression="gzip")


def build_cache_payload(season: int, players: dict[str, dict[str, Any]], *, source: str) -> dict[str, Any]:
    # Unique player records (name keys only for meta count)
    name_keys = [k for k, v in players.items() if k == _norm_name(v.get("player"))]
    return {
        "meta": {
            "season": season,
            "source": source,
            "built_at": datetime.now(timezone.utc).isoformat(),
            "players": len(name_keys),
        },
        "players": players,
    }


def resolve_season_with_fallback(preferred: int, *, session: requests.Session | None = None) -> tuple[int, pd.DataFrame]:
    sess = session or requests.Session()
    errors: list[str] = []
    for season in (preferred, preferred - 1, preferred - 2):
        if season < 2012:
            break
        try:
            df = download_snap_counts(season, session=sess)
            return season, df
        except Exception as exc:
            errors.append(f"{season}: {exc}")
    raise RuntimeError("Could not download nflverse snap counts. Tried: " + "; ".join(errors))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build NFL snap % cache from nflverse public CSVs")
    ap.add_argument("--season", type=int, default=None, help="NFL season year (default: infer)")
    ap.add_argument("--out", default=str(_DEFAULT_OUT), help="Output JSON path")
    ap.add_argument("--dry-run", action="store_true", help="Aggregate and print stats; do not write")
    args = ap.parse_args(argv)

    preferred = int(args.season) if args.season else infer_default_season()
    print(f"[nfl snap cache] preferred season={preferred}")
    season, df = resolve_season_with_fallback(preferred)
    source = NFLVERSE_SNAP_URL.format(season=season)
    print(f"[nfl snap cache] loaded season={season} rows={len(df)} from {source}")

    players = aggregate_snap_cache(df)
    payload = build_cache_payload(season, players, source=source)
    n = payload["meta"]["players"]
    print(f"[nfl snap cache] players={n} keys={len(players)}")

    if args.dry_run:
        sample = next(iter(v for k, v in players.items() if k == _norm_name(v.get("player"))), None)
        print("[nfl snap cache] dry-run sample:", sample)
        return 0

    out = Path(args.out)
    if not out.is_absolute():
        out = Path.cwd() / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[nfl snap cache] wrote {out}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[nfl snap cache] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
