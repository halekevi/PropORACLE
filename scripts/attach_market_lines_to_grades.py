#!/usr/bin/env python3
"""Join Vegas player props + ESPN game lines onto graded_props JSON.

Period slates (NBA1Q/1H, WNBA1Q/1H) never receive full-game Vegas player lines.
Game spread/total still attach as environment (pace/script), mapped to the parent sport.

  py -3.14 scripts/attach_market_lines_to_grades.py --date 2026-09-02
  py -3.14 scripts/attach_market_lines_to_grades.py --date 2026-09-02 --write
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.pp_vegas_gap import (  # noqa: E402
    build_game_index,
    build_vegas_index,
    enrich_props,
    find_vegas_csv as _find_vegas_csv,
)

MAIN = _REPO.parent / "PropORACLE_main_cp"


def find_vegas_csv(date_str: str, explicit: str = "") -> Path | None:
    return _find_vegas_csv(date_str, explicit=explicit, repos=(_REPO, MAIN))


def load_vegas_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


_GAME_LINE_SQL = (
    "SELECT sport, game_date, home_team, away_team, spread, total, "
    "home_win_prob, away_win_prob, home_team_id, away_team_id, "
    "provider, is_closing, captured_at, event_id "
    "FROM game_lines WHERE game_date = ?"
)
_GAME_LINE_SQL_LEGACY = (
    "SELECT sport, game_date, home_team, away_team, spread, total "
    "FROM game_lines WHERE game_date = ?"
)


def load_crosswalk(db_path: Path) -> tuple[dict[tuple[str, str], list[str]], dict[tuple[str, str], set[str]]]:
    """(sport, espn id) -> abbrs, and (sport, abbr) -> ids."""
    abbrs_for_id: dict[tuple[str, str], list[str]] = {}
    abbr_to_ids: dict[tuple[str, str], set[str]] = {}
    if not db_path.is_file():
        return abbrs_for_id, abbr_to_ids
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute("SELECT sport, abbr, espn_team_id FROM team_crosswalk")
        for sport, abbr, tid in cur.fetchall():
            sp = str(sport or "").strip()
            ab = str(abbr or "").strip().upper()
            team_id = str(tid or "").strip()
            if not sp or not ab or not team_id:
                continue
            abbrs_for_id.setdefault((sp, team_id), [])
            if ab not in abbrs_for_id[(sp, team_id)]:
                abbrs_for_id[(sp, team_id)].append(ab)
            abbr_to_ids.setdefault((sp, ab), set()).add(team_id)
    except sqlite3.Error:
        return {}, {}
    finally:
        conn.close()
    return abbrs_for_id, abbr_to_ids


def load_game_rows(db_path: Path, date_str: str) -> list[dict[str, Any]]:
    if not db_path.is_file():
        return []
    conn = sqlite3.connect(str(db_path))
    try:
        try:
            cur = conn.execute(_GAME_LINE_SQL, (date_str,))
        except sqlite3.OperationalError:
            cur = conn.execute(_GAME_LINE_SQL_LEGACY, (date_str,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def graded_candidates(date_str: str) -> list[Path]:
    name = f"graded_props_{date_str}.json"
    seen: set[Path] = set()
    out: list[Path] = []
    for root in (
        MAIN / "ui_runner" / "templates",
        MAIN / "mobile" / "www",
        _REPO / "ui_runner" / "templates",
        _REPO / "mobile" / "www",
    ):
        p = root / name
        if p.is_file() and p.resolve() not in seen:
            seen.add(p.resolve())
            out.append(p)
    return out


def enrich_file(
    path: Path,
    vegas_index: dict,
    game_index: dict,
    *,
    write: bool,
    abbr_to_ids: dict | None = None,
) -> tuple[int, int]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    props = raw.get("props") or raw.get("rows") or []
    if not isinstance(props, list):
        return 0, 0
    n = enrich_props(
        props,
        vegas_index=vegas_index,
        game_index=game_index,
        abbr_to_ids=abbr_to_ids,
    )
    if write and n:
        raw["props"] = props
        raw["count"] = len(props)
        path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
    return n, len(props)


def _game_join(db_path: Path, date_str: str) -> tuple[dict, dict]:
    """Load game lines for this slate date only. Empty index if DB is stale.

    Teeth: do not attach environment from an early-Sep cache onto a late-Sep
    board. Same soft-signal/hard-gate split — missing columns beat silent lies.
    """
    try:
        from utils.game_lines_freshness import game_lines_freshness_report

        rep = game_lines_freshness_report(slate_date=date_str, db_path=db_path)
        if not rep.get("ok"):
            print(
                f"[game_lines] REFUSING attach for {date_str}: "
                f"missing={rep.get('missing')} stale={rep.get('stale')} — "
                f"{rep.get('hint')}"
            )
            return {}, {}
    except Exception as exc:
        print(f"[game_lines] freshness check failed ({exc}); refusing attach")
        return {}, {}
    abbrs_for_id, abbr_to_ids = load_crosswalk(db_path)
    game_index = build_game_index(
        load_game_rows(db_path, date_str),
        abbrs_for_id=abbrs_for_id,
    )
    return game_index, abbr_to_ids


def enrich_in_memory(
    props: list[dict[str, Any]],
    date_str: str,
    *,
    repo: Path | None = None,
    vegas_csv: str = "",
) -> int:
    """Used by build_grades_html export. No file rewrite."""
    root = repo or _REPO
    vg_path = find_vegas_csv(date_str, vegas_csv)
    vegas_index = build_vegas_index(load_vegas_rows(vg_path)) if vg_path else {}
    db_path = root / "data" / "cache" / "game_lines.db"
    game_index, abbr_to_ids = _game_join(db_path, date_str)
    return enrich_props(
        props,
        vegas_index=vegas_index,
        game_index=game_index,
        abbr_to_ids=abbr_to_ids,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Attach Vegas + game lines onto graded_props JSON")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--vegas-csv", default="", help="Override vegas_props.csv path")
    ap.add_argument("--write", action="store_true", help="Rewrite graded JSON in place")
    args = ap.parse_args()
    date_str = args.date.strip()[:10]

    vg_path = find_vegas_csv(date_str, args.vegas_csv)
    vegas_index = build_vegas_index(load_vegas_rows(vg_path)) if vg_path else {}
    db_path = _REPO / "data" / "cache" / "game_lines.db"
    game_index, abbr_to_ids = _game_join(db_path, date_str)
    print(f"[attach] vegas={vg_path or 'none'} games={len(game_index)}")

    files = graded_candidates(date_str)
    if not files:
        print(f"[attach] no graded_props_{date_str}.json found")
        return
    for path in files:
        n, total = enrich_file(
            path,
            vegas_index,
            game_index,
            write=args.write,
            abbr_to_ids=abbr_to_ids,
        )
        verb = "wrote" if args.write else "would attach"
        print(f"[attach] {path}: {verb} Vegas on {n}/{total}")


if __name__ == "__main__":
    main()
