#!/usr/bin/env python3
"""
NFL step4c — role / snap context.

Looks for ``Sports/NFL/data/nfl_snap_pct_cache.json`` (or --cache / repo
``data/nfl_snap_pct_cache.json`` fallback). When present, fills
snap_pct_L3 / snap_pct_season / role_stability_score and minutes_tier.

Refresh cache from public nflverse snap CSVs (no auth)::

  py -3.14 Sports/NFL/scripts/build_nfl_snap_pct_cache.py
  py -3.14 Sports/NFL/scripts/build_nfl_snap_pct_cache.py --season 2025

Without the cache this step writes NA placeholders and leaves minutes_tier
UNKNOWN so soft score stays neutral.

Run from NFL/ with NFL_PIPELINE_ACTIVE=1.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_NFL_ROOT = _SCRIPT_DIR.parent
_REPO_ROOT = _SCRIPT_DIR.parents[2]  # Sports/NFL/scripts → PropORACLE
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from _nfl_pipeline_active import require_nfl_pipeline_active_or_exit
from utils.nfl_prop_defense import snap_pct_to_minutes_tier
from utils.nfl_espn_context import expected_snaps_from_slot
from utils.nfl_player_names import norm_nfl_player_name as _norm_name

OUT_COLS = ("snap_pct_L3", "snap_pct_season", "role_stability_score")
# Relative to Sports/NFL/ (cwd when pipeline runs from NFL root).
DEFAULT_CACHE = "data/nfl_snap_pct_cache.json"
DEFAULT_DEPTH = "data/nfl_depth_chart_cache.json"




def _resolve_cache_path(raw: str) -> Path:
    """Prefer Sports/NFL/data cache; fall back to repo data/ if needed."""
    p = Path(raw)
    candidates: list[Path] = []
    if p.is_absolute():
        candidates.append(p)
    else:
        candidates.append(_NFL_ROOT / p)
        candidates.append(Path.cwd() / p)
        # Legacy mention: repo-root data/nfl_snap_pct_cache.json
        if p.name == "nfl_snap_pct_cache.json" or str(p).replace("\\", "/").endswith(
            "data/nfl_snap_pct_cache.json"
        ):
            candidates.append(_NFL_ROOT / "data" / "nfl_snap_pct_cache.json")
            candidates.append(_REPO_ROOT / "data" / "nfl_snap_pct_cache.json")
    for c in candidates:
        if c.is_file():
            return c
    return candidates[0] if candidates else _NFL_ROOT / DEFAULT_CACHE


def _load_snap_cache(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[NFL step4c] WARN snap cache read failed: {exc}")
        return {}
    if isinstance(data, dict) and "players" in data:
        data = data["players"]
    if not isinstance(data, dict):
        return {}
    out = {}
    for k, v in data.items():
        if isinstance(v, dict):
            out[_norm_name(k)] = v
            pid = str(v.get("player_id") or v.get("gsis_id") or "").strip()
            if pid:
                out[pid] = v
    return out


def _load_depth_cache(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[NFL step4c] WARN depth cache read failed: {exc}")
        return {}
    if isinstance(data, dict) and "by_player" in data:
        data = data["by_player"]
    if not isinstance(data, dict):
        return {}
    out = {}
    for k, v in data.items():
        if isinstance(v, dict):
            out[_norm_name(k)] = v
            aid = str(v.get("espn_athlete_id") or "").strip()
            if aid:
                out[aid] = v
    return out


def main() -> None:
    require_nfl_pipeline_active_or_exit()

    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/outputs/step3_nfl_with_defense.csv")
    ap.add_argument("--output", default="data/outputs/step3_nfl_with_defense.csv")
    ap.add_argument("--cache", default=DEFAULT_CACHE)
    ap.add_argument("--depth", default=DEFAULT_DEPTH)
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.is_file():
        print(f"[NFL step4c] Missing input: {in_path}")
        sys.exit(1)

    df = pd.read_csv(in_path, encoding="utf-8-sig")
    for c in OUT_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    cache_path = _resolve_cache_path(args.cache)
    cache = _load_snap_cache(cache_path)

    filled = 0
    if cache:
        player_col = "player_name" if "player_name" in df.columns else ("player" if "player" in df.columns else "")
        id_col = next((c for c in ("gsis_id", "player_id", "nfl_id") if c in df.columns), "")
        for idx, row in df.iterrows():
            rec = None
            if id_col:
                rec = cache.get(str(row.get(id_col, "")).strip())
            if not rec and player_col:
                rec = cache.get(_norm_name(row.get(player_col)))
            if not rec:
                continue
            l3 = rec.get("snap_pct_L3", rec.get("snap_pct_l3", rec.get("offense_pct")))
            season = rec.get("snap_pct_season", rec.get("snap_pct"))
            stab = rec.get("role_stability_score", rec.get("stability"))
            if l3 is not None:
                df.at[idx, "snap_pct_L3"] = l3
            if season is not None:
                df.at[idx, "snap_pct_season"] = season
            if stab is not None:
                df.at[idx, "role_stability_score"] = stab
            filled += 1
        print(f"[NFL step4c] Snap cache hit rows={filled}/{len(df)} from {cache_path}")
    else:
        print(
            f"[NFL step4c] No snap cache at {cache_path} — placeholders only. "
            "Populate Sports/NFL/data/nfl_snap_pct_cache.json via "
            "build_nfl_snap_pct_cache.py (nflverse public CSV) to enable minutes_tier soft."
        )

    # minutes_tier from L3 snap when available; else UNKNOWN (neutral soft).
    snap_src = df["snap_pct_L3"] if "snap_pct_L3" in df.columns else pd.Series(pd.NA, index=df.index)
    snap_src = snap_src.fillna(df["snap_pct_season"]) if "snap_pct_season" in df.columns else snap_src
    df["minutes_tier"] = snap_src.apply(snap_pct_to_minutes_tier)

    depth_path = _resolve_cache_path(args.depth)
    depth = _load_depth_cache(depth_path)
    for c in ("depth_slot", "depth_rank", "expected_snaps"):
        if c not in df.columns:
            df[c] = pd.NA
    depth_hits = 0
    if depth:
        player_col = "player_name" if "player_name" in df.columns else ("player" if "player" in df.columns else "")
        id_col = next((c for c in ("espn_athlete_id", "espn_id") if c in df.columns), "")
        for idx, row in df.iterrows():
            rec = None
            if id_col:
                rec = depth.get(str(row.get(id_col, "")).strip())
            if not rec and player_col:
                rec = depth.get(_norm_name(row.get(player_col)))
            if not rec:
                continue
            slot = rec.get("depth_slot") or ""
            df.at[idx, "depth_slot"] = slot
            df.at[idx, "depth_rank"] = rec.get("depth_rank")
            cur_exp = str(row.get("expected_snaps") or "").strip()
            if not cur_exp or cur_exp.lower() in {"nan", "none"}:
                df.at[idx, "expected_snaps"] = rec.get("expected_snaps") or expected_snaps_from_slot(str(slot))
            depth_hits += 1
        print(f"[NFL step4c] Depth cache hit rows={depth_hits}/{len(df)} from {depth_path}")
    else:
        print(
            f"[NFL step4c] No depth cache at {depth_path} — run "
            "build_nfl_depth_chart_cache.py for Week 1 slots."
        )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    mt = df["minutes_tier"].astype(str).value_counts().to_dict()
    print(f"[NFL step4c] Wrote {out_path} rows={len(df)} minutes_tier={mt}")


if __name__ == "__main__":
    main()
