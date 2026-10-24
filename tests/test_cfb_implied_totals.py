"""CFB implied totals + observe-only Pass Yards Goblin D-veto."""

from __future__ import annotations

import pandas as pd

from utils.cfb_clock_script import RUN_HEAVY
from utils.cfb_implied_totals import (
    attach_implied_totals,
    home_spread_from_odds,
    implied_team_total,
    is_pass_yards_prop,
    lookup_team_line,
    parse_scoreboard_games,
    pass_yards_goblin_d_veto_report,
    projected_margin,
    would_veto_pass_yards_goblin_d,
)


def test_implied_total_matches_nfl_step6b():
    # Favorite -21 / 52.5 → 36.75 / 15.75, margin +21
    assert implied_team_total(52.5, -21.0) == 36.8
    assert implied_team_total(52.5, 21.0) == 15.8
    assert projected_margin(-21.0) == 21.0


def test_home_spread_from_details_away_favorite():
    odd = {"details": "Ohio State -21.5", "spread": 21.5, "overUnder": 52.5}
    hs = home_spread_from_odds(
        odd,
        home_names=["Michigan", "MICH"],
        away_names=["Ohio State", "OSU"],
    )
    assert hs == 21.5


def test_parse_scoreboard_and_lookup():
    payload = {
        "events": [
            {
                "competitions": [
                    {
                        "competitors": [
                            {
                                "homeAway": "home",
                                "team": {
                                    "abbreviation": "MICH",
                                    "displayName": "Michigan Wolverines",
                                },
                            },
                            {
                                "homeAway": "away",
                                "team": {
                                    "abbreviation": "OSU",
                                    "displayName": "Ohio State Buckeyes",
                                },
                            },
                        ],
                        "odds": [
                            {
                                "details": "Ohio State -21.5",
                                "spread": 21.5,
                                "overUnder": 52.5,
                            }
                        ],
                    }
                ]
            }
        ]
    }
    games = parse_scoreboard_games(payload, fetch_missing_core=False)
    assert len(games) == 1
    osu = lookup_team_line("OSU", games)
    mich = lookup_team_line("MICH", games)
    assert osu is not None and mich is not None
    assert osu["spread"] == -21.5
    assert osu["projected_margin"] == 21.5
    assert osu["implied_team_total"] == implied_team_total(52.5, -21.5)
    assert mich["spread"] == 21.5
    assert mich["implied_team_total"] == implied_team_total(52.5, 21.5)


def test_pass_yards_prop_excludes_tds():
    assert is_pass_yards_prop("pass_yds")
    assert is_pass_yards_prop("Pass Yards")
    assert not is_pass_yards_prop("pass_td")
    assert not is_pass_yards_prop("rec_yds")


def test_d_veto_only_when_blowout_run_heavy_and_d_pass():
    kwargs = dict(
        prop="pass_yds",
        pick_type="Goblin",
        direction="OVER",
    )
    assert would_veto_pass_yards_goblin_d(
        blowout=True, run_heavy=True, d_ok=True, **kwargs
    )
    assert not would_veto_pass_yards_goblin_d(
        blowout=True, run_heavy=True, d_ok=False, **kwargs
    )
    assert not would_veto_pass_yards_goblin_d(
        blowout=True, run_heavy=False, d_ok=True, **kwargs
    )
    assert not would_veto_pass_yards_goblin_d(
        blowout=False, run_heavy=True, d_ok=True, **kwargs
    )
    assert not would_veto_pass_yards_goblin_d(
        blowout=True, run_heavy=True, d_ok=True, prop="rush_yds", pick_type="Goblin", direction="OVER"
    )
    assert RUN_HEAVY == 0.58


def test_attach_is_observe_only_and_sweeps_thresholds():
    games = [
        {
            "home": "NAVY",
            "away": "TEM",
            "home_spread": -21.0,
            "game_total": 52.5,
            "home_keys": {"NAVY"},
            "away_keys": {"TEM"},
            "source": "espn",
        }
    ]
    df = pd.DataFrame(
        [
            {
                "Player": "A",
                "Team": "NAVY",
                "Prop": "pass_yds",
                "Pick Type": "Goblin",
                "Direction": "OVER",
                "Def Tier": "Weak",
                "Team Rush Rate": 0.78,
                "Rank Score": 12.3,
            },
            {
                "Player": "B",
                "Team": "TEM",
                "Prop": "pass_yds",
                "Pick Type": "Goblin",
                "Direction": "OVER",
                "Def Tier": "Weak",
                "Team Rush Rate": 0.45,
                "Rank Score": 9.0,
            },
        ]
    )
    out, meta = attach_implied_totals(df, games=games, slate_date="2026-09-12")
    assert meta["observe_only"] is True
    assert list(out["Rank Score"]) == [12.3, 9.0]
    assert bool(out.loc[0, "Blowout Flag"]) is True
    assert bool(out.loc[0, "Blowout 14"]) is True
    assert bool(out.loc[0, "Blowout 21"]) is True
    assert out.loc[0, "Pass Yards D Veto"] == "Y"
    assert bool(out.loc[1, "Blowout Flag"]) is False
    assert out.loc[1, "Pass Yards D Veto"] == "N"
    report = pass_yards_goblin_d_veto_report(out)
    assert report["n"] == 2
    assert report["d_pass"] == 2
    assert report["by_threshold"]["17"]["n_veto"] == 1
    assert report["by_threshold"]["17"]["identity"]["unique_player_n"] == 1
    assert report["unique_player_n"] == 2


def test_cbb_total_thresholds_must_not_apply_to_cfb():
    import sys
    from pathlib import Path

    scripts = Path(__file__).resolve().parents[1] / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    from game_script_risk import prop_script_sensitivity, total_risk_multiplier

    assert total_risk_multiplier("CBB", 52.5) == 0.93
    assert total_risk_multiplier("CFB", 52.5) == 1.00
    assert prop_script_sensitivity("CBB", "pass_yds") == 0.85
