"""Rebuild an archived CBB step1 and run the four opening-night checks.

Oct 25 ForceCBB is a real no_slate. This uses the only full board kept on
disk (Sports/CBB/archive/old_outputs/step3_cbb.csv, tips 2026-02-21).
No March step1 was archived. Em-dash coverage is GARBAGE_PROPS + a
dash-injected row, not a missing March file.

  py -3.14 scripts/cbb_opening_dry_pass.py
  .\\run_pipeline.ps1 -Date 2026-02-21 -CBBOnly -ForceCBB -SkipFetch -SkipCombined -SkipPush
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

SLATE = "2026-02-21"
ARCHIVE = _REPO / "Sports" / "CBB" / "archive" / "old_outputs" / "step3_cbb.csv"
OUT = _REPO / "outputs" / SLATE / "cbb" / "step1_cbb.csv"

STEP1_COLS = [
    "proj_id",
    "player_id",
    "player",
    "team",
    "pp_team",
    "pp_opp_team",
    "pp_game_id",
    "pos",
    "stat_type",
    "line",
    "odds_type",
    "pick_type",
    "start_time",
    "league_id",
    "fetched_at",
]


def write_step1() -> Path:
    raw = pd.read_csv(ARCHIVE)
    out = pd.DataFrame()
    out["proj_id"] = raw.get("proj_id")
    out["player_id"] = raw.get("player_id")
    out["player"] = raw.get("player")
    out["team"] = raw.get("pp_team").fillna(raw.get("team"))
    out["pp_team"] = raw.get("pp_team")
    out["pp_opp_team"] = raw.get("pp_opp_team")
    out["pp_game_id"] = raw.get("pp_game_id")
    out["pos"] = raw.get("pos")
    out["stat_type"] = raw.get("stat_type").fillna(raw.get("prop_type"))
    out["line"] = raw.get("line")
    odds = raw.get("odds_type")
    pick = raw.get("pick_type")
    out["odds_type"] = odds.where(odds.notna() & (odds.astype(str).str.strip() != ""), pick)
    out["pick_type"] = pick.where(pick.notna() & (pick.astype(str).str.strip() != ""), odds)
    out["start_time"] = raw.get("start_time")
    out["league_id"] = raw.get("league_id")
    out["fetched_at"] = f"{SLATE}T12:00:00-05:00"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False, encoding="utf-8")
    return OUT


def check_pick_type(path: Path) -> None:
    from utils.pick_type_persist import assert_pick_type_persisted, pick_type_counts

    counts = assert_pick_type_persisted(path, sport="CBB")
    print(f"  pick_type {counts}")
    df = pd.read_csv(path)
    mixed = pick_type_counts(df)
    if mixed["Goblin"] + mixed["Demon"] == 0:
        raise SystemExit("face check failed: archived board has no Goblin/Demon")
    print(f"  faces Standard={mixed['Standard']} Goblin={mixed['Goblin']} Demon={mixed['Demon']}")


def check_smoke() -> None:
    men = _REPO / "Sports" / "CBB" / "data" / "reference" / "ncaa_mbb_athletes_master.csv"
    women = _REPO / "Sports" / "CBB" / "data" / "reference" / "ncaa_wbb_athletes_master.csv"
    defense = _REPO / "Sports" / "CBB" / "data" / "reference" / "cbb_def_rankings.csv"
    boxes = _REPO / "Sports" / "CBB" / "data" / "cache" / "cbb_boxscore_cache.csv"
    missing = [str(p) for p in (men, women, defense, boxes) if not p.is_file()]
    if missing:
        raise SystemExit(f"import smoke failed, missing {missing}")
    print(f"  smoke men={men.stat().st_size} women={women.stat().st_size} def={defense.stat().st_size} box={boxes.stat().st_size}")


def check_emdash(path: Path) -> None:
    from utils.face_ladder import GARBAGE_PROPS

    if "\u2014" not in GARBAGE_PROPS:
        raise SystemExit("em-dash is not in GARBAGE_PROPS")
    df = pd.read_csv(path)
    props = df["stat_type"].astype(str)
    bad = props.isin(GARBAGE_PROPS) | props.str.contains("\ufffd", regex=False) | props.str.contains("\u2014", regex=False)
    if int(bad.sum()) != 0:
        raise SystemExit(f"archived step1 has {int(bad.sum())} garbage prop names")
    injected = pd.Series(["Points", "\u2014", "Rebounds"])
    flagged = injected.isin(GARBAGE_PROPS)
    if not bool(flagged.iloc[1]) or bool(flagged.iloc[0]):
        raise SystemExit("GARBAGE_PROPS did not flag an em-dash fixture row")
    print("  em-dash: GARBAGE_PROPS holds \\u2014; archived props are clean")


def main() -> int:
    path = write_step1()
    print(f"wrote {path} rows={sum(1 for _ in path.open(encoding='utf-8')) - 1}")
    check_pick_type(path)
    check_smoke()
    check_emdash(path)
    print("CBB opening dry-pass checks OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
