"""Attach tonight starter + slate minutes to an NBA step8 board.

  py -3.14 scripts/attach_nba_slate_minutes.py --date 2026-10-25
  py -3.14 scripts/attach_nba_slate_minutes.py --date 2026-10-25 --refresh
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))

from utils.nba_slate_minutes import attach_slate_minutes_columns, load_or_fetch_snapshot


def _find_step8(slate: date) -> Path:
    dated = _REPO / "outputs" / slate.isoformat() / "nba" / "step8_all_direction.csv"
    if dated.is_file():
        return dated
    live = Path(r"H:\PropORACLE_main_cp") / "outputs" / slate.isoformat() / "nba" / "step8_all_direction.csv"
    if live.is_file():
        return live
    sport = _REPO / "Sports" / "NBA" / "outputs" / "step8_all_direction.csv"
    if sport.is_file():
        return sport
    raise FileNotFoundError(f"No NBA step8 for {slate}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--input", default="")
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()
    slate = date.fromisoformat(str(args.date)[:10])
    src = Path(args.input) if args.input else _find_step8(slate)
    df = pd.read_csv(src, low_memory=False, encoding="utf-8-sig")
    snap = load_or_fetch_snapshot(slate, refresh=args.refresh)
    out = attach_slate_minutes_columns(df, as_of=slate, snapshot=snap)
    pre = True
    if "slate_pre_tip" in out.columns:
        pre = bool(out["slate_pre_tip"].fillna(False).astype(bool).any())
    try:
        from score_nba_stat_projections import score_and_freeze_live_board

        _, n_fr = score_and_freeze_live_board(out, slate, pre_tip=pre)
        print(f"pre-tip proj_edge freeze rows={n_fr}")
    except Exception as exc:
        print(f"proj_edge freeze skipped: {exc}")
    dest = src.with_name(src.stem + "_slate_minutes.csv")
    out.to_csv(dest, index=False, encoding="utf-8-sig")
    txt = dest.with_suffix(".txt")
    players = (
        out.drop_duplicates(subset=["player", "team"])
        if "player" in out.columns
        else out
    )
    lines = [
        f"NBA slate minutes {slate}  source={src.name}",
        f"confirmed teams={sorted((snap.get('confirmed') or {}).keys())}",
        f"projected teams={sorted((snap.get('projected') or {}).keys())}",
        "",
        f"{'player':24} {'tm':4} {'tonight':10} {'src':12} {'inj':12} slate  vsRole  note",
    ]
    for _, r in players.sort_values(
        ["tonight_starter", "slate_minutes"], ascending=[True, False]
    ).iterrows():
        lines.append(
            f"{str(r.get('player',''))[:24]:24} {str(r.get('team',''))[:4]:4} "
            f"{str(r.get('tonight_starter','')):10} {str(r.get('tonight_starter_source','')):12} "
            f"{str(r.get('player_injury_status','')):12} "
            f"{r.get('slate_minutes'):5} {r.get('minutes_vs_role')!s:>6}  "
            f"{r.get('slate_minutes_note','')}"
        )
    txt.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {dest}")
    print(f"wrote {txt}")
    print(out["tonight_starter"].astype(str).value_counts().to_string())
    if "game_window" in out.columns:
        print(out["game_window"].astype(str).value_counts().to_string())
    if "slate_pre_tip" in out.columns:
        print(f"pre_tip_rows={int(out['slate_pre_tip'].fillna(False).astype(bool).sum())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
