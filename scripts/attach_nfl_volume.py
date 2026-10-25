"""Join nflverse snap % and target share onto an NFL board.

  py -3.14 scripts/attach_nfl_volume.py --input path/to/step3.csv
  py -3.14 scripts/attach_nfl_volume.py --date 2026-10-25
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nfl_player_names import norm_nfl_player_name
from utils.nfl_route_participation_gate import attach_participation
from utils.nfl_volume_freshness import assert_nfl_volume_week_fresh
from utils.nfl_volume_snapshot import append_pre_kick_snapshot


def _find_board(slate: str) -> Path:
    for src in (
        _REPO / "outputs" / slate / "nfl" / "step3_nfl_with_defense.csv",
        Path(r"H:\PropORACLE_main_cp") / "outputs" / slate / "nfl" / "step3_nfl_with_defense.csv",
        _REPO / "Sports" / "NFL" / "data" / "outputs" / "step3_nfl_with_defense.csv",
    ):
        if src.is_file():
            return src
    raise FileNotFoundError(f"No NFL step3 for {slate}")


def _fill_snap_cache(df: pd.DataFrame) -> pd.DataFrame:
    path = _REPO / "Sports" / "NFL" / "data" / "nfl_snap_pct_cache.json"
    if not path.is_file():
        return df
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return df
    players = raw.get("players") if isinstance(raw, dict) else raw
    if not isinstance(players, dict):
        return df
    pcol = "player" if "player" in df.columns else ("player_name" if "player_name" in df.columns else None)
    if not pcol:
        return df
    out = df.copy()
    if "snap_pct_L3" not in out.columns:
        out["snap_pct_L3"] = pd.NA
    if "snap_pct_season" not in out.columns:
        out["snap_pct_season"] = pd.NA
    filled = 0
    for idx, row in out.iterrows():
        rec = players.get(norm_nfl_player_name(row.get(pcol)))
        if not rec:
            continue
        if pd.isna(row.get("snap_pct_L3")) and rec.get("snap_pct_L3") is not None:
            out.at[idx, "snap_pct_L3"] = rec["snap_pct_L3"]
            filled += 1
        if pd.isna(row.get("snap_pct_season")) and rec.get("snap_pct_season") is not None:
            out.at[idx, "snap_pct_season"] = rec["snap_pct_season"]
    if filled:
        print(f"  snap cache filled {filled} missing L3 rows")
    return out


def _series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(float("nan"), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--input", default="")
    ap.add_argument("--output", default="")
    args = ap.parse_args()
    src = Path(args.input) if args.input else _find_board(str(args.date)[:10])
    df = pd.read_csv(src, low_memory=False, encoding="utf-8-sig")
    assert_nfl_volume_week_fresh(label="NFL volume attach")
    out = attach_participation(df, _REPO)
    out = _fill_snap_cache(out)
    try:
        append_pre_kick_snapshot(out, slate=str(args.date)[:10])
    except OSError as exc:
        print(f"  volume snapshot skipped: {exc}")
    dest = Path(args.output) if args.output else src.with_name(src.stem + "_volume.csv")
    dest.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dest, index=False, encoding="utf-8-sig")

    pcol = "player" if "player" in out.columns else ("player_name" if "player_name" in out.columns else None)
    players = out.drop_duplicates(subset=[pcol]).copy() if pcol else out
    snap = _series(players, "snap_pct_L3")
    tgt = _series(players, "target_share_L3")
    print(f"wrote {dest}  rows={len(out)} players={len(players)}")
    print(f"  snap_pct_L3 filled={int(snap.notna().sum())}/{len(players)}")
    print(f"  target_share_L3 filled={int(tgt.notna().sum())}/{len(players)}")
    if pcol and snap.notna().any():
        cols = [pcol, "snap_pct_L3"]
        if "target_share_L3" in players.columns:
            cols.append("target_share_L3")
        print(
            players.loc[snap.notna(), cols]
            .sort_values("snap_pct_L3", ascending=False)
            .head(12)
            .to_string(index=False)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
