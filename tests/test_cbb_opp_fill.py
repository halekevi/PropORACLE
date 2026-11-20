from utils.cbb_opp_fill import _lookup_opp, _pair_map_from_events, fill_cbb_opponents
import pandas as pd


def test_pp_espn_alias_lookup():
    pairs = {"IU": "UK", "UK": "IU", "PUR": "OAK", "OAK": "PUR", "CCU": "TENN", "TENN": "CCU"}
    assert _lookup_opp("IND", pairs) == "UK"
    assert _lookup_opp("PUR", pairs) == "OAK"
    assert _lookup_opp("CCAR", pairs) == "TENN"


def test_fill_cbb_opponents_from_events(monkeypatch):
    events = [
        {
            "competitions": [
                {
                    "competitors": [
                        {"team": {"abbreviation": "VAN"}},
                        {"team": {"abbreviation": "UCF"}},
                    ]
                }
            ]
        },
        {
            "competitions": [
                {
                    "competitors": [
                        {"team": {"abbreviation": "IU"}},
                        {"team": {"abbreviation": "UK"}},
                    ]
                }
            ]
        },
    ]
    monkeypatch.setattr(
        "utils.cbb_opp_fill._fetch_scoreboard",
        lambda ymd, league="mens-college-basketball": events,
    )
    df = pd.DataFrame(
        [
            {"pp_team": "VAN", "pp_opp_team": "nan", "opp_team_abbr": "nan", "opp": "nan"},
            {"pp_team": "IND", "pp_opp_team": "", "opp_team_abbr": "", "opp": ""},
            {"pp_team": "TENN", "pp_opp_team": "CCAR", "opp_team_abbr": "CCAR", "opp": "CCAR"},
        ]
    )
    out, stats = fill_cbb_opponents(df, slate_date="2026-11-20")
    assert stats["filled"] == 2
    assert out.loc[0, "pp_opp_team"] == "UCF"
    assert out.loc[1, "pp_opp_team"] == "UK"
    assert out.loc[2, "pp_opp_team"] == "CCAR"
    assert stats["still_blank"] == 0


def test_pair_map_from_events():
    events = [
        {
            "competitions": [
                {
                    "competitors": [
                        {"team": {"abbreviation": "PUR"}},
                        {"team": {"abbreviation": "OAK"}},
                    ]
                }
            ]
        }
    ]
    assert _pair_map_from_events(events) == {"PUR": "OAK", "OAK": "PUR"}
