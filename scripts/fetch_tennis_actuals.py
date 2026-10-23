#!/usr/bin/env python3
"""
Build actuals_tennis_YYYY-MM-DD.csv for combined_ticket_grader (ESPN ATP/WTA finals).

Columns: player, team, prop_type, actual
(team left blank so grader matches on player + prop_norm; optional future use.)

Ticketed players are always requested, even when they were not on the PrizePicks
board snapshot used for the first scoreboard walk. A saved past-date file is
not replaced; missing ticketed names are merged in.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
_TENNIS_SCRIPTS = ROOT / "Sports" / "Tennis" / "scripts"
if str(_TENNIS_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_TENNIS_SCRIPTS))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from tennis_shared import iter_scoreboard_matches, norm_key  # noqa: E402
from utils.eastern_slate_date import keep_event_on_slate  # noqa: E402
from player_name_norm import fold_player_name  # noqa: E402


def _fold(name: object) -> str:
    s = str(name or "").strip()
    if not s:
        return ""
    return fold_player_name(s) or norm_key(s)


def ticketed_tennis_names(target: str, repo: Path | None = None) -> dict[str, str]:
    """fold -> original display name for every tennis player on a ticket that day."""
    from ticket_leg_index import iter_ticket_legs

    out: dict[str, str] = {}
    for rec in iter_ticket_legs(target, sport="TENNIS", repo=repo or ROOT):
        pl = str(rec.get("player") or rec.get("Player") or "").strip()
        key = _fold(pl)
        if key and key not in out:
            out[key] = pl
    return out


def _opt(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:
        return None
    return f


def rows_from_match(m: dict) -> list[dict[str, object]]:
    pl = str(m.get("player") or "").strip()
    if not pl:
        return []
    gw = _opt(m.get("games_won"))
    mt = _opt(m.get("match_total_games"))
    ac = _opt(m.get("aces"))
    dfault = _opt(m.get("double_faults"))
    sw = _opt(m.get("sets_won"))
    rows: list[dict[str, object]] = []
    if gw is not None:
        rows.append({"player": pl, "team": "", "prop_type": "games_won", "actual": gw})
    if mt is not None:
        rows.append({"player": pl, "team": "", "prop_type": "match_total_games", "actual": mt})
    if ac is not None:
        rows.append({"player": pl, "team": "", "prop_type": "aces", "actual": ac})
    if dfault is not None:
        rows.append({"player": pl, "team": "", "prop_type": "double faults", "actual": dfault})
    rows.append({"player": pl, "team": "", "prop_type": "sets won", "actual": sw if sw is not None else 0.0})
    return rows


def collect_scoreboard_rows(
    target: str,
    *,
    ticketed: dict[str, str],
) -> tuple[list[dict[str, object]], set[str]]:
    """ESPN scoreboard walk plus a second pass for ticketed names the date filter dropped."""
    query_dates = [
        (date.fromisoformat(target) - timedelta(days=1)).isoformat(),
        target,
        (date.fromisoformat(target) + timedelta(days=1)).isoformat(),
    ]
    rows: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    found_folds: set[str] = set()

    def _add(m: dict) -> None:
        pl = str(m.get("player") or "").strip()
        cid = str(m.get("competition_id") or "")
        dedupe = (pl.lower(), cid)
        if not pl or dedupe in seen:
            return
        seen.add(dedupe)
        found_folds.add(_fold(pl))
        rows.extend(rows_from_match(m))

    for tour in ("ATP", "WTA"):
        for ymd in query_dates:
            for m in iter_scoreboard_matches(tour, ymd):
                if not keep_event_on_slate(m.get("slate_date_et") or m.get("match_date_utc"), target, ymd):
                    continue
                _add(m)

    missing = {k: v for k, v in ticketed.items() if k not in found_folds}
    if missing:
        extra_dates = [
            (date.fromisoformat(target) - timedelta(days=2)).isoformat(),
            *query_dates,
            (date.fromisoformat(target) + timedelta(days=2)).isoformat(),
        ]
        want = set(missing)
        for tour in ("ATP", "WTA"):
            for ymd in extra_dates:
                for m in iter_scoreboard_matches(tour, ymd):
                    key = _fold(m.get("player"))
                    if key not in want:
                        continue
                    _add(m)
                    want.discard(key)
                    if not want:
                        break
                if not want:
                    break
            if not want:
                break
        leftover = sorted(ticketed[k] for k in want if k in ticketed)
        if leftover:
            print(f"  [tennis-actuals] ticketed players still missing after ESPN walk: {leftover}")
        else:
            print(f"  [tennis-actuals] filled {len(missing)} ticketed player(s) off the board snapshot")
    return rows, found_folds


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD (US Eastern start)")
    ap.add_argument("--output", required=True, help="Path to actuals_tennis_YYYY-MM-DD.csv")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing file even when the new board has fewer rows",
    )
    args = ap.parse_args()

    target = str(args.date).strip()[:10]
    out = Path(args.output)
    today_et = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    ticketed = ticketed_tennis_names(target, ROOT)
    rows, found_folds = collect_scoreboard_rows(target, ticketed=ticketed)

    out.parent.mkdir(parents=True, exist_ok=True)
    if out.is_file() and target < today_et and not args.force:
        try:
            existing = pd.read_csv(out)
        except Exception:
            existing = pd.DataFrame()
        have = {_fold(p) for p in existing.get("player", pd.Series(dtype=str)).astype(str)}
        extra = [
            r
            for r in rows
            if _fold(r.get("player")) in ticketed and _fold(r.get("player")) not in have
        ]
        if extra:
            combined = pd.concat([existing, pd.DataFrame(extra)], ignore_index=True)
            combined.to_csv(out, index=False)
            print(
                f"OK [tennis-actuals] date={target} merged {len(extra)} ticketed-player rows "
                f"into existing {out}"
            )
        else:
            still = [n for k, n in ticketed.items() if k not in have]
            print(
                f"OK [tennis-actuals] date={target} is before {today_et}; "
                f"left existing file in place: {out}"
                + (f" (ticketed still missing: {still})" if still else "")
            )
        return
    if out.is_file() and not args.force:
        try:
            old_n = len(pd.read_csv(out))
        except Exception:
            old_n = 0
        if old_n and len(rows) < int(old_n * 0.9):
            print(
                f"REFUSE [tennis-actuals] date={target} existing={old_n} new={len(rows)} "
                f"file left in place: {out}"
            )
            return
    pd.DataFrame(rows).to_csv(out, index=False)
    print(
        f"OK [tennis-actuals] date={target} rows={len(rows)} "
        f"ticketed={len(ticketed)} covered={sum(1 for k in ticketed if k in found_folds)} -> {out}"
    )


if __name__ == "__main__":
    main()
