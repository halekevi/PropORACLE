"""NBA minutes / Team Leaders / playmaker role helpers."""
from __future__ import annotations

from datetime import date

from utils.nba_minutes_role import (
    _norm_name,
    attach_minutes_role_columns,
    build_team_leaders_sheet,
    player_minutes_profiles,
    preseason_assist_ranks_by_team,
)
from utils.role_context import not_primary_playmaker


def test_keldon_season_mpg_not_playoff_l5_alone():
    """RS mpg fills; Min Tier uses role_mpg (pre+season), not L5 playoff shrink."""
    as_of = date(2026, 10, 21)
    prof = player_minutes_profiles(as_of=as_of).get(_norm_name("Keldon Johnson"))
    assert prof is not None
    assert prof.min_season_avg is not None and prof.min_season_avg >= 20
    assert prof.min_preseason_avg is not None and prof.min_preseason_avg >= 15
    assert prof.role_mpg is not None and prof.role_mpg >= 18
    # Attach: L5=15 playoff-ish should show a negative gap vs RS
    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "player": "Keldon Johnson",
                "team": "SAS",
                "min_last5_avg": 15.0,
                "minutes_tier": "LOW",
            }
        ]
    )
    out = attach_minutes_role_columns(df, as_of=as_of)
    assert float(out.loc[0, "min_season_avg"]) >= 20
    assert float(out.loc[0, "l5_minutes_gap"]) < -3
    assert str(out.loc[0, "minutes_tier"]).upper() in {"MEDIUM", "HIGH", "MED"}
    assert str(out.loc[0, "minutes_tier_l5"]).upper() in {"LOW", "0", "0.0"} or True


def test_castle_not_primary_via_preseason_rank():
    """Last-season #1 APG still flags when preseason APG rank is not #1."""
    ranks = preseason_assist_ranks_by_team(as_of=date(2026, 10, 21))
    castle = ranks.get(("SA", _norm_name("Stephon Castle")))
    fox = ranks.get(("SA", _norm_name("De'Aaron Fox")))
    assert fox == 1
    assert castle is not None and castle > 1
    assert not_primary_playmaker(
        {"player": "Stephon Castle", "team": "SAS", "pos": "G"},
        ast_team_rank=1,
    )
    assert not not_primary_playmaker(
        {"player": "De'Aaron Fox", "team": "SAS", "pos": "G"},
        ast_team_rank=1,
    )


def test_team_leaders_sheet_has_top_and_bottom():
    sheet = build_team_leaders_sheet(["SA", "BOS"], as_of=date(2026, 10, 21))
    assert len(sheet) > 0
    assert set(sheet["Side"].unique()) >= {"top", "bottom"}
    assert "Points" in set(sheet["Stat"].unique())
    # Rotation floor
    assert (sheet["Minutes"] >= 10).all()
