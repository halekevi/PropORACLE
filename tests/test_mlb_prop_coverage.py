"""Coverage for newly tracked MLB PrizePicks markets (fantasy stays dropped)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MLB_SCRIPTS = REPO_ROOT / "Sports" / "MLB" / "scripts"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_step1_tracks_new_markets_but_not_fantasy():
    step1 = _load("mlb_step1_coverage", MLB_SCRIPTS / "step1_fetch_prizepicks_mlb.py")
    tracked = {step1.prop_track_key(p) for p in step1.TRACKABLE_PROPS}
    for label in (
        "Pitches Seen",
        "Balls Counted",
        "Strikes Counted",
        "Batters Faced",
        "Balls Thrown",
        "Strikes Thrown",
        "Pitches Thrown 95+ MPH",
        "Walks Allowed",
        "1st Inning Walks Allowed",
        "Pitcher Strikeouts (Combo)",
        "Pitcher Strikeouts + Total Bases",
    ):
        assert step1.prop_track_key(label) in tracked, label
        assert step1.prop_track_key(label) in step1.TRACKABLE_KEYS, label
    assert step1.prop_track_key("Hitter Fantasy Score") not in tracked
    assert step1.prop_track_key("Pitcher Fantasy Score") not in tracked
    assert step1.prop_track_key("Fantasy Score") not in tracked


def test_prop_track_key_folds_95_plus_and_parens():
    step1 = _load("mlb_step1_keyfold", MLB_SCRIPTS / "step1_fetch_prizepicks_mlb.py")
    assert step1.prop_track_key("Pitches Thrown 95+ MPH") == "pitchesthrown95mph"
    assert step1.prop_track_key("pitchesthrown95+mph") == "pitchesthrown95mph"
    assert step1.prop_track_key("Pitcher Strikeouts (Combo)") == "pitcherstrikeoutscombo"


def test_missing_chip_stats_reports_absent_labels():
    import pandas as pd

    step1 = _load("mlb_step1_chips", MLB_SCRIPTS / "step1_fetch_prizepicks_mlb.py")
    empty = pd.Series(["Hits", "Strikeouts"], dtype=str)
    missing = step1.missing_chip_stats(empty)
    assert "Pitches Seen" in missing
    assert "Pitches Thrown 95+ MPH" in missing
    full = pd.Series(list(step1.CHIP_STAT_LABELS), dtype=str)
    assert step1.missing_chip_stats(full) == []


def test_step1_fail_fast_does_not_cap_pages():
    src = (MLB_SCRIPTS / "step1_fetch_prizepicks_mlb.py").read_text(encoding="utf-8")
    assert "min(args.max_pages" not in src
    assert "max_pages=int(args.max_pages)" in src
    assert "default=40" in src
    cdp = (REPO_ROOT / "utils" / "prizepicks_cdp.py").read_text(encoding="utf-8")
    assert "max_pages: int = 40" in cdp
    assert "partner-api.prizepicks.com/projections" in cdp


def test_playwright_intercept_merges_projection_ids():
    src = (MLB_SCRIPTS / "step1_fetch_prizepicks_mlb.py").read_text(encoding="utf-8")
    assert 'captured["data"] = data' not in src
    assert 'captured.setdefault("_proj_ids", set())' in src


def test_step2_norm_and_player_type():
    sys.path.insert(0, str(MLB_SCRIPTS))
    from step2_attach_picktypes_mlb import norm_prop, player_type  # noqa: E402

    cases = {
        "Pitches Seen": ("pitches_seen", "hitter"),
        "Balls Counted": ("balls_counted", "hitter"),
        "Strikes Counted": ("strikes_counted", "hitter"),
        "Batters Faced": ("batters_faced", "pitcher"),
        "Balls Thrown": ("balls_thrown", "pitcher"),
        "Strikes Thrown": ("strikes_thrown", "pitcher"),
        "Pitches Thrown 95+ MPH": ("pitches_thrown_95", "pitcher"),
        "Pitcher Strikeouts (Combo)": ("strikeouts_combo", "pitcher"),
    }
    for label, (norm, ptype) in cases.items():
        got = norm_prop(label)
        assert got == norm, (label, got)
        assert player_type(got) == ptype, (label, got)

    assert player_type(norm_prop("Hitter Fantasy Score")) == "hitter"
    assert player_type(norm_prop("Pitcher Fantasy Score")) == "pitcher"


def test_combo_ks_alias_is_supported():
    sys.path.insert(0, str(REPO_ROOT))
    sys.path.insert(0, str(MLB_SCRIPTS))
    from step4_attach_player_stats_mlb import PROP_ALIASES, UNSUPPORTED_PROPS

    assert PROP_ALIASES["strikeouts_combo"] == "strikeouts"
    assert "strikeouts_combo" not in UNSUPPORTED_PROPS
    assert "strikeouts_total_bases" not in UNSUPPORTED_PROPS


def test_order_ks_tb_ids_uses_pos():
    sys.path.insert(0, str(MLB_SCRIPTS))
    from step2_attach_picktypes_mlb import order_ks_tb_ids, norm_prop, player_type

    assert norm_prop("Pitcher Strikeouts + Total Bases") == "strikeouts_total_bases"
    assert player_type("strikeouts_total_bases") == "pitcher"
    assert order_ks_tb_ids("10", "20", "P+OF") == ("10", "20")
    assert order_ks_tb_ids("10", "20", "OF+P") == ("20", "10")
    assert order_ks_tb_ids("10", "20", "") == ("10", "20")


def test_strikeouts_plus_total_bases_same_game():
    sys.path.insert(0, str(REPO_ROOT))
    sys.path.insert(0, str(MLB_SCRIPTS))
    from step4_attach_player_stats_mlb import strikeouts_plus_total_bases

    pitch = {
        "stat": {
            "inningsPitched": "6.0",
            "strikeOuts": 8,
            "numberOfPitches": 90,
            "hits": 4,
            "earnedRuns": 2,
            "baseOnBalls": 1,
            "battersFaced": 24,
        }
    }
    hit = {
        "stat": {
            "hits": 2,
            "homeRuns": 1,
            "doubles": 0,
            "triples": 0,
            "baseOnBalls": 0,
            "stolenBases": 0,
            "rbi": 2,
            "runs": 1,
            "atBats": 4,
        }
    }
    import math
    assert strikeouts_plus_total_bases(pitch, hit) == 13.0
    assert strikeouts_plus_total_bases(pitch, None) == 8.0
    assert math.isnan(strikeouts_plus_total_bases(None, hit))


def test_compose_ks_tb_same_game_and_two_player():
    sys.path.insert(0, str(REPO_ROOT))
    sys.path.insert(0, str(MLB_SCRIPTS))
    import pandas as pd
    from step4_attach_player_stats_mlb import compose_strikeouts_total_bases

    cache = pd.DataFrame(
        [
            {"MLB_PLAYER_ID": "1", "SEASON": "2026", "GAME_DATE": "2026-08-20", "GAME_ID": "100",
             "PROP_NORM": "strikeouts", "STAT_VALUE": "7", "PLAYER_TYPE": "pitcher",
             "TEAM_ID": "", "OPP_TEAM_ID": ""},
            {"MLB_PLAYER_ID": "1", "SEASON": "2026", "GAME_DATE": "2026-08-20", "GAME_ID": "100",
             "PROP_NORM": "total_bases", "STAT_VALUE": "3", "PLAYER_TYPE": "hitter",
             "TEAM_ID": "", "OPP_TEAM_ID": ""},
            {"MLB_PLAYER_ID": "1", "SEASON": "2026", "GAME_DATE": "2026-08-14", "GAME_ID": "99",
             "PROP_NORM": "strikeouts", "STAT_VALUE": "5", "PLAYER_TYPE": "pitcher",
             "TEAM_ID": "", "OPP_TEAM_ID": ""},
            {"MLB_PLAYER_ID": "2", "SEASON": "2026", "GAME_DATE": "2026-08-21", "GAME_ID": "200",
             "PROP_NORM": "total_bases", "STAT_VALUE": "4", "PLAYER_TYPE": "hitter",
             "TEAM_ID": "", "OPP_TEAM_ID": ""},
            {"MLB_PLAYER_ID": "2", "SEASON": "2026", "GAME_DATE": "2026-08-20", "GAME_ID": "201",
             "PROP_NORM": "total_bases", "STAT_VALUE": "1", "PLAYER_TYPE": "hitter",
             "TEAM_ID": "", "OPP_TEAM_ID": ""},
        ]
    )
    same = compose_strikeouts_total_bases(cache, "1", "1", "2026", n=10)
    assert same[0] == 10.0  # 7 K + 3 TB same game
    assert same[1] == 5.0   # 5 K + 0 TB (no hitting log that day)
    two = compose_strikeouts_total_bases(cache, "1", "2", "2026", n=10)
    assert two[0] == 11.0  # 7 + 4 recency
    assert two[1] == 6.0   # 5 + 1


def test_hitter_pitches_seen_from_game_log():
    sys.path.insert(0, str(REPO_ROOT))
    sys.path.insert(0, str(MLB_SCRIPTS))
    from step4_attach_player_stats_mlb import derive_hitter_stat

    game = {
        "player": {"id": 111},
        "game": {"gamePk": 999},
        "stat": {
            "hits": 1,
            "homeRuns": 0,
            "baseOnBalls": 0,
            "stolenBases": 0,
            "rbi": 0,
            "runs": 0,
            "atBats": 4,
            "doubles": 0,
            "triples": 0,
            "strikeOuts": 1,
            "numberOfPitches": 18,
        },
    }
    assert derive_hitter_stat(game, "pitches_seen") == 18.0
    assert derive_hitter_stat(game, "hits") == 1.0


def test_pitcher_balls_strikes_thrown_from_game_log():
    sys.path.insert(0, str(REPO_ROOT))
    sys.path.insert(0, str(MLB_SCRIPTS))
    from step4_attach_player_stats_mlb import derive_pitcher_stat

    game = {
        "player": {"id": 222},
        "game": {"gamePk": 999},
        "stat": {
            "inningsPitched": "5.0",
            "strikeOuts": 6,
            "numberOfPitches": 80,
            "strikes": 50,
            "hits": 4,
            "earnedRuns": 2,
            "baseOnBalls": 1,
            "battersFaced": 20,
        },
    }
    assert derive_pitcher_stat(game, "strikes_thrown") == 50.0
    assert derive_pitcher_stat(game, "balls_thrown") == 30.0
    assert derive_pitcher_stat(game, "batters_faced") == 20.0
    assert derive_pitcher_stat(game, "strikeouts_combo") == 6.0


def test_parse_pitch_level_from_feed():
    sys.path.insert(0, str(REPO_ROOT))
    sys.path.insert(0, str(MLB_SCRIPTS))
    from step4_attach_player_stats_mlb import _parse_pitch_level_from_feed

    feed = {
        "liveData": {
            "plays": {
                "allPlays": [
                    {
                        "matchup": {"batter": {"id": 1}, "pitcher": {"id": 2}},
                        "playEvents": [
                            {
                                "isPitch": True,
                                "details": {"isBall": True, "isStrike": False},
                                "pitchData": {"startSpeed": 91},
                            },
                            {
                                "isPitch": True,
                                "details": {"isBall": False, "isStrike": True},
                                "pitchData": {"startSpeed": 96},
                            },
                            {
                                "isPitch": True,
                                "details": {
                                    "isBall": False,
                                    "isStrike": False,
                                    "isInPlay": True,
                                },
                                "pitchData": {"startSpeed": 97},
                            },
                            {
                                "isPitch": False,
                                "details": {"isBall": True, "isStrike": False},
                                "pitchData": {"startSpeed": 99},
                            },
                        ],
                    }
                ]
            }
        }
    }
    parsed = _parse_pitch_level_from_feed(feed)
    pitcher = parsed["pitcher"]["2"]
    hitter = parsed["hitter"]["1"]
    assert pitcher["balls_thrown"] == 1.0
    assert pitcher["strikes_thrown"] == 2.0
    assert pitcher["pitches_thrown_95"] == 2.0
    assert hitter["pitches_seen"] == 3.0
    assert hitter["balls_counted"] == 1.0
    assert hitter["strikes_counted"] == 2.0
