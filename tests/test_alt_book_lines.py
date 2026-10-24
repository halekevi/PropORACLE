"""Alt-book (Underdog / DraftKings / Vegas) line join onto PrizePicks rows."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from combined_slate_tickets import (  # noqa: E402
    add_cross_platform_best_lines,
    attach_alt_book_lines,
)
from utils.line_movement import flatten_vegas_lines  # noqa: E402


def _pp_row(**kwargs) -> dict:
    base = {
        "sport": "MLB",
        "player": "Aaron Judge",
        "team": "NYY",
        "prop_type": "Hits",
        "line": 1.5,
        "direction": "OVER",
        "pick_platform": "prizepicks",
        "pick_type": "Standard",
    }
    base.update(kwargs)
    return base


def test_draftkings_joins_on_prop_type_not_market_label(tmp_path: Path):
    pp = pd.DataFrame([_pp_row(sport="WNBA", player="A'ja Wilson", team="LAS", prop_type="Points", line=22.5)])
    dk = pd.DataFrame(
        [
            {
                "player": "A'ja Wilson",
                "team": "LAS",
                "prop_type": "Points",
                "dk_market_label": "A'ja Wilson Points",
                "line": 23.5,
                "board_sport": "WNBA",
                "pick_type": "Standard",
                "start_time": "",
            }
        ]
    )
    path = tmp_path / "dk.csv"
    dk.to_csv(path, index=False)
    out = attach_alt_book_lines(pp, draftkings_csv=str(path))
    assert float(out.iloc[0]["line_draftkings"]) == 23.5


def test_underdog_nearest_line_not_same_rung(tmp_path: Path):
    pp = pd.DataFrame([_pp_row()])
    ud = pd.DataFrame(
        [
            {
                "player": "Aaron Judge",
                "team": "NYY",
                "prop_type": "Hits",
                "line": 1.0,
                "ud_sport_id": "MLB",
                "opp_team": "BOS",
                "pick_type": "Standard",
                "start_time": "",
            }
        ]
    )
    ud_path = tmp_path / "ud.csv"
    ud.to_csv(ud_path, index=False)
    out = attach_alt_book_lines(pp, underdog_csv=str(ud_path))
    assert float(out.iloc[0]["line_underdog"]) == 1.0
    assert float(out.iloc[0]["line"]) == 1.5


def test_vegas_join_on_odds_market(tmp_path: Path):
    pp = pd.DataFrame([_pp_row(prop_type="Hits")])
    vg = pd.DataFrame(
        [
            {
                "player": "aaron judge",
                "prop_type": "Hits",
                "line": 0.5,
                "pick_type": "Standard",
                "source_book": "vegas",
                "board_sport": "MLB",
                "odds_market": "batter_hits",
                "vegas_bookmaker": "pinnacle",
            }
        ]
    )
    vg_path = tmp_path / "vegas.csv"
    vg.to_csv(vg_path, index=False)
    out = attach_alt_book_lines(pp, vegas_csv=str(vg_path))
    assert float(out.iloc[0]["line_vegas"]) == 0.5


def test_cross_book_includes_vegas():
    df = pd.DataFrame(
        [
            {
                "sport": "MLB",
                "player": "Aaron Judge",
                "team": "NYY",
                "prop_type": "Hits",
                "line": 1.5,
                "line_underdog": 1.5,
                "line_draftkings": 1.5,
                "line_vegas": 0.5,
                "direction": "OVER",
                "pick_platform": "prizepicks",
            }
        ]
    )
    out = add_cross_platform_best_lines(df)
    assert float(out.iloc[0]["best_cross_line"]) == 0.5
    assert "LV" in str(out.iloc[0]["best_cross_book"])
    assert float(out.iloc[0]["cross_n_books"]) == 4


def test_underdog_tennis_empty_team_and_alias(tmp_path: Path):
    pp = pd.DataFrame(
        [_pp_row(sport="Tennis", player="Jannik Sinner", team="ITA", prop_type="Total Games", line=22.5)]
    )
    ud = pd.DataFrame(
        [
            {
                "player": "Jannik Sinner",
                "team": "",
                "prop_type": "Games Played",
                "line": 23.5,
                "ud_sport_id": "TENNIS",
                "opp_team": "",
                "pick_type": "Standard",
                "start_time": "",
            }
        ]
    )
    ud_path = tmp_path / "ud.csv"
    ud.to_csv(ud_path, index=False)
    out = attach_alt_book_lines(pp, underdog_csv=str(ud_path))
    assert float(out.iloc[0]["line_underdog"]) == 23.5


def test_underdog_nearest_picks_alternate_rung(tmp_path: Path):
    pp = pd.DataFrame(
        [
            _pp_row(
                sport="Tennis",
                player="Carlos Alcaraz",
                team="ITA",
                prop_type="Games Played",
                line=34.5,
                pick_type="Goblin",
            )
        ]
    )
    shared = {
        "player": "Carlos Alcaraz",
        "team": "",
        "prop_type": "Games Played",
        "ud_sport_id": "TENNIS",
        "opp_team": "",
        "start_time": "",
    }
    ud = pd.DataFrame(
        [
            {**shared, "line": 36.5, "pick_type": "Standard"},
            {**shared, "line": 34.5, "pick_type": "Goblin"},
            {**shared, "line": 38.5, "pick_type": "Demon"},
        ]
    )
    path = tmp_path / "ud.csv"
    ud.to_csv(path, index=False)
    out = attach_alt_book_lines(pp, underdog_csv=str(path))
    assert float(out.iloc[0]["line_underdog"]) == 34.5


def test_draftkings_tennis_games_won_joins(tmp_path: Path):
    pp = pd.DataFrame(
        [_pp_row(sport="Tennis", player="Jannik Sinner", team="ITA", prop_type="Games Won", line=12.5)]
    )
    dk = pd.DataFrame(
        [
            {
                "player": "Jannik Sinner",
                "team": "",
                "prop_type": "Player Total Games Won",
                "dk_market_label": "Jannik Sinner: Player Total Games Won",
                "line": 13.5,
                "board_sport": "Tennis",
                "pick_type": "Standard",
                "start_time": "",
            }
        ]
    )
    path = tmp_path / "dk.csv"
    dk.to_csv(path, index=False)
    out = attach_alt_book_lines(pp, draftkings_csv=str(path))
    assert float(out.iloc[0]["line_draftkings"]) == 13.5


def test_underdog_soccer_empty_team_and_alias(tmp_path: Path):
    pp = pd.DataFrame(
        [_pp_row(sport="Soccer", player="Erling Haaland", team="MCI", prop_type="Shots On Target", line=1.5)]
    )
    ud = pd.DataFrame(
        [
            {
                "player": "Erling Haaland",
                "team": "",
                "prop_type": "Shots on Target",
                "line": 0.5,
                "ud_sport_id": "FIFA",
                "opp_team": "",
                "pick_type": "Standard",
                "start_time": "",
            }
        ]
    )
    ud_path = tmp_path / "ud.csv"
    ud.to_csv(ud_path, index=False)
    out = attach_alt_book_lines(pp, underdog_csv=str(ud_path))
    assert float(out.iloc[0]["line_underdog"]) == 0.5


def test_underdog_golf_pga_maps_to_golf(tmp_path: Path):
    pp = pd.DataFrame(
        [_pp_row(sport="Golf", player="Scottie Scheffler", team="", prop_type="Strokes", line=68.5)]
    )
    ud = pd.DataFrame(
        [
            {
                "player": "Scottie Scheffler",
                "team": "",
                "prop_type": "Round Strokes",
                "line": 69.5,
                "ud_sport_id": "PGA",
                "opp_team": "",
                "pick_type": "Standard",
                "start_time": "",
            }
        ]
    )
    ud_path = tmp_path / "ud.csv"
    ud.to_csv(ud_path, index=False)
    out = attach_alt_book_lines(pp, underdog_csv=str(ud_path))
    assert float(out.iloc[0]["line_underdog"]) == 69.5


def test_esports_orphans_are_not_appended(tmp_path: Path):
    pp = pd.DataFrame([_pp_row()])
    ud = pd.DataFrame(
        [
            {
                "player": "Faker",
                "team": "T1",
                "prop_type": "Kills",
                "line": 4.5,
                "ud_sport_id": "LOL",
                "opp_team": "",
                "pick_type": "Standard",
                "start_time": "",
            }
        ]
    )
    ud_path = tmp_path / "ud.csv"
    ud.to_csv(ud_path, index=False)
    out = attach_alt_book_lines(pp, underdog_csv=str(ud_path))
    assert len(out) == 1
    assert pd.isna(out.iloc[0]["line_underdog"])


def test_sportscontent_flatten_plus_line():
    from fetch_draftkings_player_props import flatten_sportscontent_payload

    payload = {
        "events": [
            {
                "id": "1",
                "startEventDate": "2026-08-30T17:00:00Z",
                "participants": [
                    {"venueRole": "Away", "metadata": {"shortName": "NYM"}},
                    {"venueRole": "Home", "metadata": {"shortName": "ATL"}},
                ],
            }
        ],
        "markets": [{"id": "m1", "eventId": "1", "name": "Pete Alonso Hits"}],
        "selections": [
            {
                "id": "s1",
                "marketId": "m1",
                "label": "1+",
                "points": None,
                "displayOdds": {"american": "+110"},
                "participants": [
                    {"name": "Pete Alonso", "type": "Player", "venueRole": "AwayPlayer"}
                ],
            }
        ],
    }
    rows = flatten_sportscontent_payload(
        payload, board_sport="MLB", category_name="Batter Props", subcategory_name="Hits"
    )
    assert len(rows) == 1
    assert rows[0]["player"] == "Pete Alonso"
    assert float(rows[0]["line"]) == 0.5
    assert rows[0]["prop_type"] == "Hits"
    assert rows[0]["team"] == "NYM"
    assert rows[0]["dk_american_odds"] == "+110"


def test_flatten_vegas_lines_prefers_vegas_line():
    snap = {
        ("aaron judge", "batter_hits", 0.5): {
            "open_line": 1.5,
            "current_line": 1.5,
            "vegas_line": 0.5,
            "implied_prob_book": "pinnacle",
        }
    }
    rows = flatten_vegas_lines(snap, board_sport="MLB")
    assert len(rows) == 1
    assert rows[0]["line"] == 0.5
    assert rows[0]["odds_market"] == "batter_hits"
    assert rows[0]["board_sport"] == "MLB"
