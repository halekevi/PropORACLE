"""NFL PrizePicks boards: game + preseason. Season-long NFLSZN is opt-in only."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_NFL_ROOT = _REPO / "Sports" / "NFL"
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_NFL_ROOT) not in sys.path:
    sys.path.insert(0, str(_NFL_ROOT))

from prizepicks_league_ids import (  # noqa: E402
    DEFAULT_NFL_BOARDS,
    NFL,
    NFLP,
    NFLSZN,
    SEASON_BOARD_IDS,
    resolve_nfl_fetch_boards,
)


def test_default_boards_exclude_season_long():
    assert DEFAULT_NFL_BOARDS[NFL] == "NFL"
    assert DEFAULT_NFL_BOARDS[NFLP] == "NFLP"
    assert NFLSZN not in DEFAULT_NFL_BOARDS
    assert NFLSZN in SEASON_BOARD_IDS
    assert NFLSZN == "163"


def test_nfl_fetch_includes_nflp_by_default():
    boards = resolve_nfl_fetch_boards()
    assert boards[NFL] == "NFL"
    assert boards[NFLP] == "NFLP"
    assert NFLSZN not in boards


def test_nfl_league_id_nine_still_fetches_nflp():
    boards = resolve_nfl_fetch_boards(league_id="9")
    assert NFL in boards
    assert NFLP in boards


def test_nflp_league_id_still_fetches_nfl():
    boards = resolve_nfl_fetch_boards(league_id="44")
    assert NFL in boards
    assert NFLP in boards


def test_nflszn_mask_keeps_nflp_game_rows():
    df = pd.DataFrame(
        {
            "league": ["NFL", "NFLP", "NFLSZN"],
            "league_id": ["9", "44", "163"],
        }
    )
    lid = df["league_id"].astype(str)
    lg = df["league"].astype(str).str.upper()
    szn = lid.eq("163") | lg.eq("NFLSZN")
    kept = df.loc[~szn, "league"].tolist()
    assert kept == ["NFL", "NFLP"]


def test_no_preseason_drops_nflp_even_for_nfl_league():
    boards = resolve_nfl_fetch_boards(league_id="9", include_preseason=False)
    assert NFL in boards
    assert NFLP not in boards


def test_assert_pick_type_persisted_ok(tmp_path):
    from Sports.NFL.scripts.step1_fetch_prizepicks_nfl import assert_pick_type_persisted

    p = tmp_path / "step1.csv"
    # ~15% Goblin / heavy Demon / rest Standard — mirrors live board shape
    rows = (
        [{"player_name": f"S{i}", "pick_type": "Standard", "prop_type": "receiving_yards", "line": 50.5} for i in range(50)]
        + [{"player_name": f"G{i}", "pick_type": "Goblin", "prop_type": "receiving_yards", "line": 40.5} for i in range(20)]
        + [{"player_name": f"D{i}", "pick_type": "Demon", "prop_type": "receiving_yards", "line": 70.5} for i in range(50)]
    )
    pd.DataFrame(rows).to_csv(p, index=False)
    counts = assert_pick_type_persisted(p, min_rows_for_mixed=100)
    assert counts["Goblin"] == 20
    assert counts["Demon"] == 50
    assert counts["Standard"] == 50


def test_assert_pick_type_persisted_rejects_all_standard(tmp_path):
    from Sports.NFL.scripts.step1_fetch_prizepicks_nfl import assert_pick_type_persisted

    p = tmp_path / "step1.csv"
    pd.DataFrame(
        {
            "player_name": [f"P{i}" for i in range(120)],
            "pick_type": ["Standard"] * 120,
            "prop_type": ["receiving_yards"] * 120,
            "line": [50.5] * 120,
        }
    ).to_csv(p, index=False)
    try:
        assert_pick_type_persisted(p)
        raise AssertionError("expected SystemExit")
    except SystemExit as exc:
        assert "100% Standard" in str(exc)


def test_assert_pick_type_persisted_missing_column(tmp_path):
    from Sports.NFL.scripts.step1_fetch_prizepicks_nfl import assert_pick_type_persisted

    p = tmp_path / "step1.csv"
    pd.DataFrame({"player_name": ["A"], "prop_type": ["sacks"], "line": [0.5]}).to_csv(
        p, index=False
    )
    try:
        assert_pick_type_persisted(p, min_rows_for_mixed=1)
        raise AssertionError("expected SystemExit")
    except SystemExit as exc:
        assert "pick_type column missing" in str(exc)


def test_output_cols_include_pick_type():
    from Sports.NFL.scripts.step1_fetch_prizepicks_nfl import OUTPUT_COLS, norm_nfl_pick_type

    assert "pick_type" in OUTPUT_COLS
    assert norm_nfl_pick_type("goblin") == "Goblin"
    assert norm_nfl_pick_type("demon") == "Demon"
    assert norm_nfl_pick_type("standard") == "Standard"


def test_season_rows_skip_game_date_filter_logic():
    df = pd.DataFrame(
        {
            "league_id": ["9", "163"],
            "start_time": ["2026-08-15T00:00:00Z", "2026-09-10T00:00:00Z"],
        }
    )
    szn_mask = df["league_id"].astype(str).isin(SEASON_BOARD_IDS)
    assert int(szn_mask.sum()) == 1
    assert df.loc[szn_mask, "league_id"].iloc[0] == "163"
