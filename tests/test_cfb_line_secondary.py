"""CFB OL/DL/secondary z-score composites (observe-only)."""

from __future__ import annotations

import pandas as pd

from utils.cfb_line_secondary import (
    STAT_DIRECTION,
    attach_line_secondary,
    compute_dl_index,
    compute_league_reference,
    compute_ol_index,
    compute_secondary_index,
    get_nested,
    index_rows,
    pass_db_mismatch,
    rush_line_mismatch,
    zscore,
)


def _team(
    *,
    line_yds: float,
    stuff: float,
    power: float,
    def_line_yds: float,
    def_stuff: float,
    havoc7: float,
    havoc_db: float,
    pass_exp: float,
    pass_sr: float,
) -> dict:
    return {
        "team": "Test",
        "offense": {
            "rushingPlays": {
                "lineYardsAverage": line_yds,
                "stuffRate": stuff,
                "powerSuccess": power,
            }
        },
        "defense": {
            "rushingPlays": {"lineYardsAverage": def_line_yds, "stuffRate": def_stuff},
            "havoc": {"frontSeven": havoc7, "db": havoc_db},
            "passingPlays": {"explosiveness": pass_exp, "successRate": pass_sr},
        },
    }


def test_stuff_rate_sign_inverts():
    ref = {"mean": 0.20, "std": 0.05, "n_teams": 10}
    # High stuff on offense = weaker OL
    assert zscore(0.30, ref, STAT_DIRECTION["off.rushingPlays.stuffRate"]) < 0
    # High stuff on defense = stronger DL
    assert zscore(0.30, ref, STAT_DIRECTION["def.rushingPlays.stuffRate"]) > 0


def test_zero_std_returns_zero():
    assert zscore(1.0, {"mean": 1.0, "std": 0.0, "n_teams": 2}, 1) == 0.0


def test_equal_weight_placeholder_is_mean_of_available_z():
    weak = _team(
        line_yds=1.0, stuff=0.40, power=0.30,
        def_line_yds=5.0, def_stuff=0.05, havoc7=0.05,
        havoc_db=0.05, pass_exp=2.0, pass_sr=0.60,
    )
    strong = _team(
        line_yds=4.0, stuff=0.10, power=0.80,
        def_line_yds=2.0, def_stuff=0.35, havoc7=0.20,
        havoc_db=0.18, pass_exp=0.9, pass_sr=0.35,
    )
    mid = _team(
        line_yds=2.5, stuff=0.25, power=0.55,
        def_line_yds=3.5, def_stuff=0.20, havoc7=0.12,
        havoc_db=0.11, pass_exp=1.4, pass_sr=0.48,
    )
    ref = compute_league_reference([weak, strong, mid])
    assert compute_ol_index(strong, ref) > compute_ol_index(weak, ref)
    assert compute_dl_index(strong, ref) > compute_dl_index(weak, ref)
    assert compute_secondary_index(strong, ref) > compute_secondary_index(weak, ref)


def test_get_nested_fallbacks():
    row = {"offense": {"lineYards": 2.7, "rushingPlays": {}}}
    assert get_nested(row, "off.rushingPlays.lineYardsAverage") == 2.7


def test_mismatch_signs():
    assert rush_line_mismatch(1.2, -0.4) == 1.6
    assert pass_db_mismatch(0.8) == 0.8
    assert rush_line_mismatch(None, 0.1) is None


def test_attach_observe_only():
    teams = [
        {"team": "Navy", "team_abbr": "NAVY", "ol_index": 1.1, "dl_index": 0.4, "secondary_index": -0.2, "games": 4, "early_season": False},
        {"team": "Temple", "team_abbr": "TEM", "ol_index": -0.5, "dl_index": -0.9, "secondary_index": 0.7, "games": 2, "early_season": True},
    ]
    table = pd.DataFrame(teams)
    df = pd.DataFrame(
        [
            {"Player": "A", "Team": "NAVY", "Opp": "TEM", "Rank Score": 12.0, "Prop": "rush_yds"},
            {"Player": "B", "Team": "TEM", "Opp": "NAVY", "Rank Score": 8.0, "Prop": "pass_yds"},
        ]
    )
    out, meta = attach_line_secondary(df, table=table)
    assert meta["observe_only"] is True
    assert list(out["Rank Score"]) == [12.0, 8.0]
    assert float(out.loc[0, "Rush Line Mismatch"]) == rush_line_mismatch(1.1, -0.9)
    assert float(out.loc[0, "Pass DB Mismatch"]) == 0.7
    assert bool(out.loc[1, "Line Early Season"]) is True


def test_index_rows_logs_n_teams_and_early_flag():
    rows = [
        _team(line_yds=3.0, stuff=0.2, power=0.6, def_line_yds=3.0, def_stuff=0.2, havoc7=0.1, havoc_db=0.1, pass_exp=1.2, pass_sr=0.45)
        for _ in range(4)
    ]
    rows[0]["team"] = "A"
    rows[1]["team"] = "B"
    df, meta = index_rows(rows, games_by_abbr={"A": 2, "B": 5}, season=2026)
    assert meta["n_teams"] == 4
    assert meta["ref"]["off.rushingPlays.lineYardsAverage"]["n_teams"] == 4
    assert bool(df.loc[df["team"] == "A", "early_season"].iloc[0]) is True
    assert bool(df.loc[df["team"] == "B", "early_season"].iloc[0]) is False
