"""Refresh official NBA starters at T−30 so tonight_starter=confirmed
arrives while a slip can still change. After tip the same fetch is
grading-only. Every run appends a pre-tip snapshot for later training.

  py -3.14 scripts/refresh_nba_starters_tminus.py --date 2026-10-25
  py -3.14 scripts/refresh_nba_starters_tminus.py --date 2026-10-25 --force
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))

from utils.nba_slate_minutes import (  # noqa: E402
    ESPN_SCOREBOARD,
    TMINUS_MINUTES,
    _cache_path,
    _get_json,
    attach_slate_minutes_columns,
    fetch_slate_snapshot,
    game_window,
    parse_scoreboard_games,
)


def _find_step8(slate: date) -> Path | None:
    for src in (
        _REPO / "outputs" / slate.isoformat() / "nba" / "step8_all_direction.csv",
        Path(r"H:\PropORACLE_main_cp") / "outputs" / slate.isoformat() / "nba" / "step8_all_direction.csv",
        _REPO / "Sports" / "NBA" / "outputs" / "step8_all_direction.csv",
    ):
        if src.is_file():
            return src
    return None


def _fmt_delta(seconds: float) -> str:
    if seconds < 0:
        return "passed"
    m = int(seconds // 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--input", default="")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Refresh even when every game is still early (projected snapshot).",
    )
    ap.add_argument(
        "--wait-tminus",
        action="store_true",
        help="Sleep until the next T-30 window, then refresh.",
    )
    args = ap.parse_args()
    slate = date.fromisoformat(str(args.date)[:10])
    now = datetime.now(timezone.utc)
    board = _get_json(ESPN_SCOREBOARD, {"dates": slate.strftime("%Y%m%d")})
    games = parse_scoreboard_games(board, now=now)

    print(f"NBA starter refresh {slate}  as_of={now.isoformat()}  T-{TMINUS_MINUTES}")
    due: list[dict] = []
    next_confirm: tuple[datetime, str] | None = None
    for g in games:
        tip_s = str(g.get("tip_utc") or "")
        window = str(g.get("window") or game_window(None))
        matchup = f"{g.get('away') or '?'}@{g.get('home') or '?'}"
        print(f"  {matchup:12} tip={tip_s or '—':25} window={window:9} status={g.get('status')}")
        if window in {"tminus30", "live"}:
            due.append(g)
        tip = None
        if tip_s:
            try:
                tip = datetime.fromisoformat(tip_s.replace("Z", "+00:00"))
            except ValueError:
                tip = None
        if tip is not None:
            t30 = tip - timedelta(minutes=TMINUS_MINUTES)
            if t30 > now and (next_confirm is None or t30 < next_confirm[0]):
                next_confirm = (t30, matchup)

    if not games:
        print("  no ESPN games for this date")
    if next_confirm:
        print(f"  next T-{TMINUS_MINUTES}: {next_confirm[1]} in {_fmt_delta((next_confirm[0] - now).total_seconds())}")
    elif games and not due:
        print("  all listed games are live or have no tip")
    if not due:
        print("  official starters not posted yet — snapshot is projected / injury (pre-tip). Re-run at T-30.")
    if args.force:
        print("  --force: snapshot + attach regardless of window")

    if args.wait_tminus and next_confirm and not due:
        wait_s = (next_confirm[0] - now).total_seconds() + 15
        print(f"  waiting {_fmt_delta(wait_s)} for {next_confirm[1]} T-{TMINUS_MINUTES}")
        sys.stdout.flush()
        if wait_s > 0:
            time.sleep(wait_s)
        now = datetime.now(timezone.utc)
        board = _get_json(ESPN_SCOREBOARD, {"dates": slate.strftime("%Y%m%d")})
        games = parse_scoreboard_games(board, now=now)
        due = [g for g in games if str(g.get("window") or "") in {"tminus30", "live"}]
        print(f"  woke at {now.isoformat()}  due={len(due)}")

    snap = fetch_slate_snapshot(slate, now=now)
    _cache_path(slate).write_text(
        __import__("json").dumps(snap, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    confirmed = sorted((snap.get("confirmed") or {}).keys())
    print(f"  confirmed teams={confirmed or 'none (projected only)'}")

    src = Path(args.input) if args.input else _find_step8(slate)
    if src is None:
        print("  no step8 board — snapshot cached, attach skipped")
        return 0

    import pandas as pd

    df = pd.read_csv(src, low_memory=False, encoding="utf-8-sig")
    out = attach_slate_minutes_columns(df, as_of=slate, snapshot=snap, write_log=True)
    pre = bool(pd.Series(out.get("slate_pre_tip")).fillna(False).astype(bool).any()) if "slate_pre_tip" in out.columns else True
    try:
        from score_nba_stat_projections import score_and_freeze_live_board

        _, n_fr = score_and_freeze_live_board(out, slate, pre_tip=pre)
        print(f"  pre-tip proj_edge freeze rows={n_fr}")
    except Exception as exc:
        print(f"  proj_edge freeze skipped: {exc}")
    dest = src.with_name(src.stem + "_slate_minutes.csv")
    out.to_csv(dest, index=False, encoding="utf-8-sig")
    pre = int(pd.Series(out.get("slate_pre_tip")).fillna(False).astype(bool).sum()) if "slate_pre_tip" in out.columns else 0
    print(f"  wrote {dest}  pre_tip_rows={pre}")
    print(out["tonight_starter"].astype(str).value_counts().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
