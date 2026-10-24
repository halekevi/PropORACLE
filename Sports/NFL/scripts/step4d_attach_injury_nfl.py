#!/usr/bin/env python3
"""NFL step4d — attach ESPN injury / OUT / doubtful flags to each prop row."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_NFL_ROOT = _SCRIPT_DIR.parent
_REPO_ROOT = _SCRIPT_DIR.parents[2]
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from _nfl_pipeline_active import require_nfl_pipeline_active_or_exit
from utils.nfl_espn_context import canon_nfl_abbr, fetch_nfl_injuries, save_json

INJURY_CACHE = _NFL_ROOT / "data" / "nfl_injury_cache.json"
OUT_COLS = (
    "injury_status",
    "injury_type",
    "injury_detail",
    "injury_rank_penalty",
)


def _norm_name(s: object) -> str:
    return " ".join(str(s or "").lower().split())


def _penalty(status: str, typ: str) -> float:
    blob = f"{status} {typ}".upper()
    if any(k in blob for k in ("ACTIVE",)) and not any(
        k in blob for k in ("OUT", "IR", "DOUBT", "QUESTION", "SUSP")
    ):
        return 0.0
    if any(k in blob for k in ("OUT", "IR", "PUP", "NFI", "SUSP")):
        return -0.45
    if "DOUBT" in blob or blob.strip() in {"D"}:
        return -0.30
    if "QUESTION" in blob or "GTD" in blob or blob.strip() in {"Q", "DTD"}:
        return -0.12
    return 0.0


def _load_cache() -> dict:
    if not INJURY_CACHE.is_file():
        return {}
    try:
        data = json.loads(INJURY_CACHE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def main() -> int:
    require_nfl_pipeline_active_or_exit()
    ap = argparse.ArgumentParser(description="NFL step4d injury context")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()

    df = pd.read_csv(args.input, encoding="utf-8-sig") if Path(args.input).is_file() else pd.DataFrame()
    for c in OUT_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    cache = _load_cache()
    rows = cache.get("injuries") if not args.refresh else None
    if not isinstance(rows, list) or args.refresh or not rows:
        rows = fetch_nfl_injuries()
        save_json(
            INJURY_CACHE,
            {
                "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "count": len(rows),
                "injuries": rows,
            },
        )
        print(f"[NFL step4d] ESPN injuries fetched n={len(rows)}")
    else:
        print(f"[NFL step4d] Using injury cache n={len(rows)}")

    by_key: dict[tuple[str, str], dict] = {}
    by_name: dict[str, dict] = {}
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        name = _norm_name(rec.get("player"))
        team = canon_nfl_abbr(rec.get("team"))
        if not name:
            continue
        by_name[name] = rec
        if team:
            by_key[(name, team)] = rec

    player_col = next((c for c in ("player_name", "player") if c in df.columns), None)
    team_col = next((c for c in ("team", "team_abbr") if c in df.columns), None)
    hit = 0
    if player_col:
        for idx, row in df.iterrows():
            name = _norm_name(row.get(player_col))
            team = canon_nfl_abbr(row.get(team_col)) if team_col else ""
            rec = by_key.get((name, team)) or by_name.get(name)
            if not rec:
                df.at[idx, "injury_rank_penalty"] = 0.0
                continue
            status = str(rec.get("injury_status") or "")
            typ = str(rec.get("injury_type") or rec.get("injury_type_desc") or "")
            df.at[idx, "injury_status"] = status
            df.at[idx, "injury_type"] = typ
            df.at[idx, "injury_detail"] = rec.get("injury_detail") or ""
            df.at[idx, "injury_rank_penalty"] = _penalty(status, typ)
            hit += 1

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[NFL step4d] rows={len(df)} injury_hits={hit} -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
