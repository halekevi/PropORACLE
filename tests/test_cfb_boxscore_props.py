"""CFB boxscore parse: combo L5, rush/rec LONG, SACKS; no fake pass LONG / TGTS."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_STEP5B = _REPO / "Sports" / "CFB" / "scripts" / "pipeline" / "step5b_attach_boxscore_stats.py"


def _load_step5b():
    spec = importlib.util.spec_from_file_location("cfb_step5b_box", _STEP5B)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _grp(name: str, labels: list[str], stats: list[str]) -> dict:
    return {
        "name": name,
        "labels": labels,
        "athletes": [
            {
                "athlete": {"id": "9", "displayName": "Test Player"},
                "stats": stats,
            }
        ],
    }


def _summary(*groups: dict) -> dict:
    return {
        "boxscore": {
            "players": [
                {
                    "team": {"id": "1"},
                    "statistics": list(groups),
                }
            ]
        }
    }


def test_parse_rush_rec_long_and_sacks_not_pass_long():
    s5 = _load_step5b()
    summary = _summary(
        _grp("passing", ["C/ATT", "YDS", "AVG", "TD", "INT", "QBR"], ["18/28", "245", "8.8", "2", "1", "64.1"]),
        _grp("rushing", ["CAR", "YDS", "AVG", "TD", "LONG"], ["12", "54", "4.5", "1", "22"]),
        _grp("receiving", ["REC", "YDS", "AVG", "TD", "LONG"], ["4", "61", "15.3", "0", "31"]),
        _grp("defensive", ["TOT", "SOLO", "SACKS", "TFL"], ["7", "4", "1.5", "2"]),
    )
    rows = s5.parse_players(summary, game_date="20251129", event_id="1")
    assert len(rows) == 1
    r = rows[0]
    assert r["PASS_YDS"] == 245.0
    assert r["PASS_CMP"] == 18.0
    assert r["PASS_ATT"] == 28.0
    assert r["RUSH_ATT"] == 12.0
    assert "PASS_LONG" not in r or r.get("PASS_LONG") in (None, "")
    assert r["RUSH_LONG"] == 22.0
    assert r["REC_LONG"] == 31.0
    assert "REC_TGT" not in r or r.get("REC_TGT") in (None, "")
    assert r["SACK"] == 1.5
    assert r["TFL"] == 2.0


def test_prop_value_combos_and_longest():
    s5 = _load_step5b()
    r = {
        "PASS_YDS": 200.0,
        "RUSH_YDS": 40.0,
        "REC_YDS": 15.0,
        "PASS_TD": 2.0,
        "RUSH_TD": 1.0,
        "REC_TD": 0.0,
        "RUSH_LONG": 22.0,
        "REC_LONG": 31.0,
        "PASS_CMP": 18.0,
        "SACK": 1.5,
        "TFL": 2.0,
    }
    assert s5.prop_value("rush_rec_yds", r) == 55.0
    assert s5.prop_value("Rush+Rec Yds", r) == 55.0
    assert s5.prop_value("pass_rush_yds", r) == 240.0
    assert s5.prop_value("pass_rush_rec_td", r) == 3.0
    assert s5.prop_value("Pass+Rush+Rec TDs", r) == 3.0
    assert s5.prop_value("rush_long", r) == 22.0
    assert s5.prop_value("longest rush", r) == 22.0
    assert s5.prop_value("rec_long", r) == 31.0
    assert s5.prop_value("longest reception", r) == 31.0
    assert s5.prop_value("sacks", r) == 1.5
    assert s5.prop_value("tfl", r) == 2.0
    assert s5.prop_value("rush_att", {**r, "RUSH_ATT": 12.0}) == 12.0
    assert s5.prop_value("pass_att", {**r, "PASS_ATT": 28.0}) == 28.0
    assert s5.prop_value("pass_cmp", r) == 18.0
    assert s5.prop_value("pass_completions", r) == 18.0
    assert s5.prop_value("Pass Completions", r) == 18.0
    assert s5.prop_value("player_td", r) == 3.0
    assert s5.prop_value("player_tds", r) == 3.0
    assert s5.prop_value("Player TDs", r) == 3.0
    # Rec targets stay cannot-obtain without PBP. Longest completion needs PASS_LONG (PBP), not completions.
    assert s5.prop_value("rec_tgt", r) is None
    assert s5.prop_value("pass_long", r) is None
    assert s5.prop_value("longest completion", r) is None
    assert s5.prop_value("longest completion", {**r, "PASS_CMP": 18.0}) is None
    assert s5.prop_value("pass_long", {**r, "PASS_LONG": 51.0}) == 51.0
    assert s5.prop_value("longest completion", {**r, "PASS_LONG": 51.0}) == 51.0
    # Multi-player (Combo) label still maps to solo box column after strip.
    assert s5.prop_value(s5.base_prop_for_combo("Pass Yards (Combo)"), r) == 200.0
    assert s5.prop_value(s5.base_prop_for_combo("Rush Yards (Combo)"), r) == 40.0
    # YPC / Comp% are rates — never raw rush yards or completion counts.
    assert s5.prop_value("rush_yards_per_carry", {**r, "RUSH_ATT": 10.0}) == 4.0
    assert s5.prop_value("rush_yards_per_carry", r) is None  # no RUSH_ATT
    assert s5.prop_value(
        "completion_percentage", {**r, "PASS_ATT": 25.0}
    ) == 72.0
    assert s5.prop_value("completion_percentage", r) is None  # no PASS_ATT


def test_multiplayer_combo_arm_sum_l5():
    """A+B newest-first series pair by index (cross-team L5), not collapsed name lookup."""
    s5 = _load_step5b()
    assert s5.multiplayer_combo_arm_names("AJ Surace + Mason McKenzie") == [
        "AJ Surace",
        "Mason McKenzie",
    ]
    assert s5.multiplayer_combo_arm_names("Solo Player") == []
    assert s5.base_prop_for_combo("Receiving Yards (Combo)") == "Receiving Yards"
    assert s5.base_prop_for_combo("fg_made_combo") == "fg_made"
    a = [100.0, 80.0, 60.0, 40.0, 20.0]
    b = [50.0, 40.0, 30.0, 20.0, 10.0]
    assert s5.sum_multiplayer_combo_game_vals([a, b]) == [150.0, 120.0, 90.0, 60.0, 30.0]
    assert s5.sum_multiplayer_combo_game_vals([a, []]) is None
    assert s5.sum_multiplayer_combo_game_vals([a[:2], b]) == [150.0, 120.0]


def test_step2_norm_prop_combo_aliases():
    step2_path = _REPO / "Sports" / "CFB" / "scripts" / "pipeline" / "step2_normalize.py"
    spec = importlib.util.spec_from_file_location("cfb_step2_norm", step2_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    assert mod.norm_prop("Pass+Rush+Rec TDs") == "pass_rush_rec_td"
    assert mod.norm_prop("Rec Targets") == "rec_tgt"
    assert mod.norm_prop("Rush+Rec Yds") == "rush_rec_yds"
    assert mod.norm_prop("Pass+Rush Yds") == "pass_rush_yds"
    assert mod.norm_prop("Longest Completion") == "pass_long"
    assert mod.norm_prop("Longest Rush") == "rush_long"
    assert mod.norm_prop("Longest Reception") == "rec_long"
    assert mod.norm_prop("Player TDs") == "player_td"
    assert mod.norm_prop("Player TD") == "player_td"


def test_parse_pass_long_from_drives_jersey():
    s5 = _load_step5b()
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"id": "2638"},
                    "statistics": [
                        {
                            "name": "passing",
                            "labels": ["C/ATT", "YDS", "AVG", "TD", "INT", "QBR"],
                            "athletes": [
                                {
                                    "athlete": {
                                        "id": "5083042",
                                        "displayName": "Skyler Locklear",
                                        "jersey": "9",
                                        "lastName": "Locklear",
                                    },
                                    "stats": ["22/35", "338", "9.7", "4", "1", "80"],
                                }
                            ],
                        }
                    ],
                }
            ]
        },
        "drives": {
            "previous": [
                {
                    "plays": [
                        {
                            "type": {"text": "Pass Reception"},
                            "text": "No Huddle #9 S.Locklear pass complete short right to #14 H.Wilson for 2 yards",
                            "statYardage": 2,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Passing Touchdown"},
                            "text": "No Huddle #9 S.Locklear pass complete deep left to #89 J.Schuchts for 64 yards to the DEL00 TOUCHDOWN",
                            "statYardage": 64,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Pass Reception"},
                            "text": "#9 S.Locklear pass complete to #6 K.Odom for 68 yards",
                            "statYardage": 68,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Pass Reception"},
                            "text": "#9 S.Locklear pass complete for 99 yards",
                            "statYardage": 99,
                            "isPenalty": True,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                    ]
                }
            ]
        },
    }
    longs = s5.parse_pass_long_from_drives(summary)
    assert longs[("2638", "9")] == 68.0
    rows = s5.parse_players(summary, game_date="20251129", event_id="401757310")
    assert len(rows) == 1
    assert rows[0]["PASS_LONG"] == 68.0
    assert rows[0]["PASS_YDS"] == 338.0
    assert s5.prop_value("longest completion", rows[0]) == 68.0
    assert s5.prop_value("pass_long", rows[0]) == 68.0
    assert rows[0]["PASS_CMP"] == 22.0
    assert rows[0]["PASS_ATT"] == 35.0


def test_parse_rec_and_rush_long_from_pbp():
    s5 = _load_step5b()
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"id": "2638"},
                    "statistics": [
                        {
                            "name": "rushing",
                            "labels": ["CAR", "YDS", "AVG", "TD"],
                            "athletes": [
                                {
                                    "athlete": {
                                        "id": "10",
                                        "displayName": "K. Hunter",
                                        "jersey": "2",
                                    },
                                    "stats": ["12", "54", "4.5", "1"],
                                }
                            ],
                        },
                        {
                            "name": "receiving",
                            "labels": ["REC", "YDS", "AVG", "TD"],
                            "athletes": [
                                {
                                    "athlete": {
                                        "id": "2",
                                        "displayName": "H. Wilson",
                                        "jersey": "14",
                                    },
                                    "stats": ["2", "66", "33.0", "1"],
                                }
                            ],
                        },
                    ],
                }
            ]
        },
        "drives": {
            "previous": [
                {
                    "plays": [
                        {
                            "type": {"text": "Rush"},
                            "text": "#2 K.Hunter run for 8 yards",
                            "statYardage": 8,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Rushing Touchdown"},
                            "text": "#2 K.Hunter run for 22 yards TOUCHDOWN",
                            "statYardage": 22,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Pass Reception"},
                            "text": "#9 S.Locklear pass complete to #14 H.Wilson for 2 yards",
                            "statYardage": 2,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Passing Touchdown"},
                            "text": "#9 S.Locklear pass complete to #14 H.Wilson for 64 yards TOUCHDOWN",
                            "statYardage": 64,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                    ]
                }
            ]
        },
    }
    rows = {r["espn_athlete_id"]: r for r in s5.parse_players(summary, game_date="20251129", event_id="1")}
    assert rows["10"]["RUSH_LONG"] == 22.0
    assert rows["2"]["REC_LONG"] == 64.0
    assert s5.prop_value("longest rush", rows["10"]) == 22.0
    assert s5.prop_value("longest reception", rows["2"]) == 64.0


def test_parse_rec_targets_from_pbp_jersey():
    s5 = _load_step5b()
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"id": "2638"},
                    "statistics": [
                        {
                            "name": "passing",
                            "labels": ["C/ATT", "YDS", "AVG", "TD", "INT", "QBR"],
                            "athletes": [
                                {
                                    "athlete": {
                                        "id": "1",
                                        "displayName": "Skyler Locklear",
                                        "jersey": "9",
                                    },
                                    "stats": ["2/4", "70", "17.5", "1", "0", "80"],
                                }
                            ],
                        },
                        {
                            "name": "receiving",
                            "labels": ["REC", "YDS", "AVG", "TD", "LONG"],
                            "athletes": [
                                {
                                    "athlete": {
                                        "id": "2",
                                        "displayName": "H. Wilson",
                                        "jersey": "14",
                                    },
                                    "stats": ["2", "66", "33.0", "1", "64"],
                                }
                            ],
                        },
                    ],
                }
            ]
        },
        "drives": {
            "previous": [
                {
                    "plays": [
                        {
                            "type": {"text": "Pass Reception"},
                            "text": "#9 S.Locklear pass complete to #14 H.Wilson for 2 yards",
                            "statYardage": 2,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Passing Touchdown"},
                            "text": "#9 S.Locklear pass complete to #14 H.Wilson for 64 yards TOUCHDOWN",
                            "statYardage": 64,
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Pass Incompletion"},
                            "text": "#9 S.Locklear pass incomplete short right to #14 H.Wilson",
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Pass Interception Return"},
                            "text": "#9 S.Locklear pass intercepted intended for #14 H.Wilson",
                            "isPenalty": False,
                            "teamParticipants": [{"id": "2638", "type": "offense"}],
                        },
                    ]
                }
            ]
        },
    }
    tgts = s5.parse_rec_targets_from_drives(summary)
    assert tgts[("2638", "14")] == 4
    rows = {r["espn_athlete_id"]: r for r in s5.parse_players(summary, game_date="20251129", event_id="1")}
    assert rows["2"]["REC_TGT"] == 4.0
    assert rows["2"]["REC"] == 2.0
    assert s5.prop_value("rec_tgt", rows["2"]) == 4.0
    assert rows["1"]["PASS_ATT"] == 4.0


def test_parse_rec_targets_last_name_when_jersey_zero():
    s5 = _load_step5b()
    summary = {
        "boxscore": {
            "players": [
                {
                    "team": {"id": "52"},
                    "statistics": [
                        {
                            "name": "receiving",
                            "labels": ["REC", "YDS", "AVG", "TD", "LONG"],
                            "athletes": [
                                {
                                    "athlete": {
                                        "id": "3",
                                        "displayName": "Duce Robinson",
                                        "lastName": "Robinson",
                                        "jersey": "0",
                                    },
                                    "stats": ["4", "61", "15.3", "0", "31"],
                                }
                            ],
                        }
                    ],
                }
            ]
        },
        "drives": {
            "previous": [
                {
                    "plays": [
                        {
                            "type": {"text": "Pass Reception"},
                            "text": "Tommy Castellanos pass complete to Duce Robinson for 12 yards",
                            "isPenalty": False,
                            "teamParticipants": [{"id": "52", "type": "offense"}],
                        },
                        {
                            "type": {"text": "Pass Incompletion"},
                            "text": "Tommy Castellanos pass incomplete to Duce Robinson",
                            "isPenalty": False,
                            "teamParticipants": [{"id": "52", "type": "offense"}],
                        },
                    ]
                }
            ]
        },
    }
    rows = s5.parse_players(summary, game_date="20251129", event_id="2")
    assert len(rows) == 1
    assert rows[0]["REC_TGT"] == 2.0
    assert rows[0]["REC"] == 4.0


def test_aggregate_player_volume_share():
    import importlib.util

    spec_path = _REPO / "Sports" / "CFB" / "scripts" / "pipeline" / "build_cfb_player_volume.py"
    spec = importlib.util.spec_from_file_location("cfb_player_volume", spec_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    rows = [
        {
            "event_id": "a",
            "team_id": "10",
            "espn_athlete_id": "100",
            "player_norm": "rb one",
            "game_date": "20251101",
            "RUSH_ATT": 20.0,
            "REC_TGT": 4.0,
            "REC": 3.0,
        },
        {
            "event_id": "a",
            "team_id": "10",
            "espn_athlete_id": "101",
            "player_norm": "rb two",
            "game_date": "20251101",
            "RUSH_ATT": 5.0,
            "REC_TGT": 1.0,
            "REC": 1.0,
        },
        {
            "event_id": "b",
            "team_id": "10",
            "espn_athlete_id": "100",
            "player_norm": "rb one",
            "game_date": "20251108",
            "RUSH_ATT": 16.0,
            "REC_TGT": 2.0,
            "REC": 2.0,
        },
        {
            "event_id": "b",
            "team_id": "10",
            "espn_athlete_id": "101",
            "player_norm": "rb two",
            "game_date": "20251108",
            "RUSH_ATT": 4.0,
            "REC_TGT": 2.0,
            "REC": 1.0,
        },
    ]
    vol = mod.aggregate_player_volume(rows)
    one = vol[vol["espn_athlete_id"] == "100"].iloc[0]
    assert one["volume_games"] == 2
    assert one["rush_att_pg"] == 18.0
    assert one["rush_att_share"] == 0.8
    assert one["rec_tgt_pg"] == 3.0
    assert one["rec_tgt_share"] == 0.65
    slate = pd.DataFrame(
        [{"espn_athlete_id": "100", "player_norm": "rb one", "team_id": "10"}]
    )
    attached = mod.attach_player_volume(slate, vol)
    assert float(attached.iloc[0]["rush_att_pg"]) == 18.0
    assert float(attached.iloc[0]["rec_tgt_share"]) == 0.65

