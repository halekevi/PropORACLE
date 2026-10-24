"""PrizePicks vs Vegas signed cover, period-sport skip, catalog suspect skip."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from utils.pp_vegas_gap import (  # noqa: E402
    allows_vegas_join,
    apply_vegas_to_prop,
    build_vegas_index,
    enrich_props,
    gap_bucket,
    is_grading_suspect,
    nearest_vegas_line,
    odds_market,
    signed_cover,
)
from combined_slate_tickets import attach_alt_book_lines  # noqa: E402
from grading.build_grades_html import prop_row_for_api  # noqa: E402


def test_prop_row_exports_vegas_gap():
    row = {
        "Player": "Aaron Judge",
        "Team": "NYY",
        "prop_type": "Hits",
        "line": 1.5,
        "Dir": "OVER",
        "pick_type": "Standard",
        "result": "HIT",
        "line_vegas": 0.5,
        "game_spread": -1.5,
        "rest_days": 1,
    }
    out = prop_row_for_api(row, "MLB")
    assert out is not None
    assert out["line_vegas"] == "0.5"
    assert out["vegas_gap"] == "-1.0"
    assert out["game_spread"] == "-1.5"
    assert out["rest_days"] == "1"


def test_signed_cover_over_under():
    assert signed_cover(24.5, 26.5, "OVER") == 2.0
    assert signed_cover(24.5, 22.5, "OVER") == -2.0
    assert signed_cover(24.5, 22.5, "UNDER") == 2.0
    assert signed_cover(24.5, 26.5, "UNDER") == -2.0


def test_gap_buckets():
    assert gap_bucket(-2.5) == "against 2+"
    assert gap_bucket(-0.5) == "against 0-2"
    assert gap_bucket(0.5) == "on 0-1"
    assert gap_bucket(1.5) == "on 1-2"
    assert gap_bucket(2.0) == "on 2+"


def test_period_sports_skip_vegas():
    for sp in ("NBA1Q", "NBA1H", "WNBA1Q", "WNBA1H"):
        assert allows_vegas_join(sp) is False
        assert odds_market(sp, "Points") == ""
    assert allows_vegas_join("NBA") is True
    idx = build_vegas_index(
        [{"board_sport": "NBA", "player": "Jayson Tatum", "odds_market": "player_points", "line": 28.5}]
    )
    assert nearest_vegas_line(idx, sport="NBA1Q", player="Jayson Tatum", prop="Points", pp_line=7.5) is None
    prop = {"sport": "NBA1Q", "player": "Jayson Tatum", "prop": "Points", "line": "7.5", "direction": "OVER"}
    assert apply_vegas_to_prop(prop, idx) is False
    assert not prop.get("line_vegas")


def test_enrich_attaches_full_game_only():
    idx = build_vegas_index(
        [
            {"board_sport": "NBA", "player": "Jayson Tatum", "odds_market": "player_points", "line": 28.5},
            {"board_sport": "MLB", "player": "Aaron Judge", "odds_market": "batter_hits", "line": 1.5},
        ]
    )
    props = [
        {"sport": "NBA", "player": "Jayson Tatum", "prop": "Points", "line": "26.5", "direction": "OVER"},
        {"sport": "NBA1Q", "player": "Jayson Tatum", "prop": "Points", "line": "7.5", "direction": "OVER"},
        {"sport": "MLB", "player": "Aaron Judge", "prop": "Hits", "line": "1.5", "direction": "OVER"},
    ]
    n = enrich_props(props, vegas_index=idx)
    assert n == 2
    assert props[0]["line_vegas"] == "28.5"
    assert props[0]["vegas_gap"] == "2.0"
    assert not props[1].get("line_vegas")
    assert props[2]["line_vegas"] == "1.5"
    assert props[2]["vegas_gap"] == "0.0"


def test_real_vegas_line_drops_pp_fallback():
    from utils.pp_vegas_gap import real_vegas_line, sport_from_step8_name

    assert sport_from_step8_name("step8_mlb_direction_clean.xlsx") == "MLB"
    assert sport_from_step8_name("step8_wnba1q_direction_clean.xlsx") is None
    assert real_vegas_line(pp_line=1.5, open_line=1.5, line_vegas=None, implied_prob=None, line_movement=0) is None
    assert real_vegas_line(pp_line=1.5, open_line=0.5, line_vegas=None, implied_prob=None, line_movement=0) == 0.5
    assert real_vegas_line(pp_line=1.5, open_line=1.5, line_vegas=None, implied_prob=0.55, line_movement=0) == 1.5
    assert real_vegas_line(pp_line=1.5, open_line=None, line_vegas=0.5, implied_prob=None, line_movement=None) == 0.5


def test_archive_and_find_vegas_csv(tmp_path: Path):
    from utils.pp_vegas_gap import archive_vegas_csv, find_vegas_csv

    src = tmp_path / "outputs" / "2026-09-02" / "vegas_props.csv"
    src.parent.mkdir(parents=True)
    src.write_text("player,line\nJudge,1.5\n", encoding="utf-8")
    archived = archive_vegas_csv(src, "2026-09-02", tmp_path)
    assert archived is not None and archived.is_file()
    src.unlink()
    found = find_vegas_csv("2026-09-02", repos=(tmp_path,))
    assert found == archived


def test_grading_suspect_flag():
    assert is_grading_suspect({"grading_suspect": True})
    assert is_grading_suspect({"grading_suspect": "true"})
    assert not is_grading_suspect({"grading_suspect": False})
    assert not is_grading_suspect({})


def test_combined_clears_vegas_on_period_slate(tmp_path: Path):
    pp = pd.DataFrame(
        [
            {
                "sport": "NBA1Q",
                "player": "Jayson Tatum",
                "team": "BOS",
                "prop_type": "Points",
                "line": 7.5,
                "direction": "OVER",
                "pick_platform": "prizepicks",
                "pick_type": "Standard",
            },
            {
                "sport": "NBA",
                "player": "Jayson Tatum",
                "team": "BOS",
                "prop_type": "Points",
                "line": 26.5,
                "direction": "OVER",
                "pick_platform": "prizepicks",
                "pick_type": "Standard",
            },
        ]
    )
    vg = pd.DataFrame(
        [
            {
                "player": "jayson tatum",
                "prop_type": "Points",
                "line": 28.5,
                "pick_type": "Standard",
                "source_book": "vegas",
                "board_sport": "NBA",
                "odds_market": "player_points",
                "vegas_bookmaker": "pinnacle",
            }
        ]
    )
    vg_path = tmp_path / "vegas.csv"
    vg.to_csv(vg_path, index=False)
    out = attach_alt_book_lines(pp, vegas_csv=str(vg_path))
    q1 = out[out["sport"] == "NBA1Q"].iloc[0]
    full = out[out["sport"] == "NBA"].iloc[0]
    assert pd.isna(q1["line_vegas"])
    assert float(full["line_vegas"]) == 28.5
