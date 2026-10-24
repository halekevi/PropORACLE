"""L5 ranked-by-cell pool keeps real 5-for-5 values and one leg per cell."""
from __future__ import annotations

from utils.category_hit_rate import _norm_dir, _norm_pick, _norm_prop, _norm_sport
from utils.gap_ranker import wilson_low
from utils.card_side import CardBook, index_projections
from utils.l5_ranked_by_cell import (
    TRACK,
    build_ranked_slips,
    candidate_sizes,
    five_games,
    record_l5_ranked,
    screen_leg,
)


def _key(sport: str, prop: str, pick: str, side: str) -> tuple[str, str, str, str]:
    return (_norm_sport(sport), _norm_prop(prop, sport), _norm_pick(pick), _norm_dir(side))


def _cell(sport: str, prop: str, pick: str, side: str, hit: int, n: int) -> dict:
    return {_key(sport, prop, pick, side): {"hr": hit / n, "n": n, "hit": hit}}


def _row(**kw) -> dict:
    base = {
        "sport": "NFL",
        "player": "Player A",
        "prop": "Receiving Yards",
        "side": "UNDER",
        "pick_type": "Standard",
        "line": 50.5,
        "team": "WAS",
        "opp_team": "SF",
        "season_type": "regular",
        "game_id": "g-a",
        "stat_g1": 40,
        "stat_g2": 22,
        "stat_g3": 18,
        "stat_g4": 31,
        "stat_g5": 27,
    }
    base.update(kw)
    # Unique team per game/player so default boards are not all-same-team blocked.
    if "team" not in kw:
        token = str(base.get("game_id") or base.get("player") or "TMA")
        base["team"] = "".join(ch for ch in token.upper() if ch.isalnum())[:3] or "TMA"
    if "opp_team" not in kw:
        base["opp_team"] = "OPP"
    return base


def test_five_values_are_required_and_printed():
    stamped = _row()
    del stamped["stat_g1"]
    del stamped["stat_g2"]
    del stamped["stat_g3"]
    del stamped["stat_g4"]
    del stamped["stat_g5"]
    stamped["l5_under"] = 5
    index = _cell("NFL", "Receiving Yards", "Standard", "UNDER", 87, 100)
    assert five_games(stamped) is None
    leg, reason = screen_leg(stamped, index)
    assert leg is None and reason == "no_five_values"

    kept, why = screen_leg(_row(), index)
    assert why == ""
    assert kept is not None
    assert kept["l5_values"] == [40, 22, 18, 31, 27]


def test_daniels_card_is_not_five_of_five():
    """Sacks Taken 2.5 under. Card log 5, 0, 1, 1, 4. Under needs 2 or fewer."""
    row = _row(
        player="Jayden Daniels",
        prop="Sacks Taken",
        line=2.5,
        stat_g1=5,
        stat_g2=0,
        stat_g3=1,
        stat_g4=1,
        stat_g5=4,
        l5_under=5,
    )
    index = _cell("NFL", "Sacks Taken", "Standard", "UNDER", 3, 3)
    leg, reason = screen_leg(row, index)
    assert leg is None and reason == "not_five_of_five"


def test_zero_filled_sacks_taken_stays_out():
    row = _row(
        prop="Sacks Taken",
        line=2.5,
        stat_g1=0,
        stat_g2=0,
        stat_g3=0,
        stat_g4=0,
        stat_g5=0,
        l5_under=5,
    )
    index = _cell("NFL", "Sacks Taken", "Standard", "UNDER", 73, 77)
    leg, reason = screen_leg(row, index)
    assert leg is None and reason == "sacks_taken_box_untrusted"


def test_real_sacks_series_can_clear():
    row = _row(
        prop="Sacks Taken",
        line=2.5,
        stat_g1=1,
        stat_g2=0,
        stat_g3=2,
        stat_g4=1,
        stat_g5=0,
    )
    index = _cell("NFL", "Sacks Taken", "Standard", "UNDER", 73, 77)
    leg, reason = screen_leg(row, index)
    assert reason == ""
    assert leg is not None
    assert leg["l5_values"] == [1, 0, 2, 1, 0]


def test_wilson_low_orders_groups_and_small_cells_are_report_only():
    tiny = wilson_low(3, 3)
    wide = wilson_low(73, 77)
    assert tiny is not None and wide is not None and wide > tiny
    index = {}
    index.update(_cell("NFL", "Sacks Taken", "Standard", "UNDER", 3, 3))
    index.update(_cell("NFL", "Receiving Yards", "Standard", "UNDER", 73, 77))
    board = [
        _row(
            player="Tiny",
            prop="Sacks Taken",
            line=2.5,
            game_id="g-tiny",
            stat_g1=1,
            stat_g2=0,
            stat_g3=2,
            stat_g4=1,
            stat_g5=0,
        ),
        _row(player="Wide", game_id="g-wide"),
    ]
    built = build_ranked_slips(board, index)
    assert [leg["player"] for leg in built["report_only"]] == ["Tiny"]
    assert [group["label"] for group in built["groups"]] == ["NFL receiving_yards UNDER Standard"]
    assert built["groups"][0]["rank"] == 1
    assert all(leg["player"] != "Tiny" for slip in built["slips"] for leg in slip["legs"])


def test_one_leg_per_group_skips_a_used_player_not_game():
    index = {}
    index.update(_cell("NFL", "Receiving Yards", "Standard", "UNDER", 87, 100))
    index.update(_cell("NFL", "Receptions", "Standard", "UNDER", 80, 96))
    board = [
        _row(player="Kittle", team="SF", opp_team="LAR", game_id="sf", stat_g1=10, stat_g2=8, stat_g3=9, stat_g4=7, stat_g5=6),
        _row(player="McLaurin", team="WAS", opp_team="NYG", game_id="was", stat_g1=30, stat_g2=28, stat_g3=22, stat_g4=19, stat_g5=21),
        _row(
            player="Kittle",
            team="SF",
            opp_team="LAR",
            prop="Receptions",
            line=6.5,
            game_id="sf",
            stat_g1=4,
            stat_g2=3,
            stat_g3=2,
            stat_g4=5,
            stat_g5=3,
        ),
        _row(
            player="Williams",
            team="CHI",
            opp_team="WAS",
            prop="Receptions",
            line=6.5,
            game_id="was",
            stat_g1=1,
            stat_g2=2,
            stat_g3=0,
            stat_g4=1,
            stat_g5=2,
        ),
    ]
    built = build_ranked_slips(board, index)
    names = [leg["player"] for slip in built["slips"] for leg in slip["legs"]]
    assert names.count("Kittle") == 1
    assert "Williams" in names
    # Same-game is allowed now; McLaurin still loses the RY group to Kittle.
    assert "McLaurin" not in names
    # Kittle(sf) + Williams(was) are different games — pair flag stays off here.
    assert built["slips"] and built["slips"][0].get("same_game_pair") is False


def test_same_game_legs_can_share_a_standard_slip():
    index = {}
    index.update(_cell("NFL", "Receiving Yards", "Standard", "UNDER", 90, 120))
    index.update(_cell("NFL", "Rushing Yards", "Standard", "UNDER", 88, 110))
    index.update(_cell("NFL", "Receptions", "Standard", "UNDER", 85, 100))
    board = [
        _row(
            player="Garland",
            prop="Receiving Yards",
            team="LAC",
            opp_team="ORL",
            game_id="lac-orl",
            line=50.5,
            stat_g1=40,
            stat_g2=22,
            stat_g3=18,
            stat_g4=31,
            stat_g5=27,
        ),
        _row(
            player="Wagner",
            prop="Rushing Yards",
            team="ORL",
            opp_team="LAC",
            game_id="lac-orl",
            line=40.5,
            stat_g1=20,
            stat_g2=18,
            stat_g3=15,
            stat_g4=22,
            stat_g5=19,
        ),
        _row(
            player="Jackson",
            prop="Receptions",
            team="LAC",
            opp_team="ORL",
            game_id="lac-orl",
            line=6.5,
            stat_g1=4,
            stat_g2=3,
            stat_g3=2,
            stat_g4=5,
            stat_g5=3,
        ),
    ]
    built = build_ranked_slips(board, index)
    assert built["slips"]
    slip = built["slips"][0]
    names = {leg["player"] for leg in slip["legs"]}
    assert names == {"Garland", "Wagner", "Jackson"}
    assert slip.get("same_game_pair") is True
    assert slip.get("correlation_ok") is True


def test_standard_under_is_unproven_and_goblin_slip_stays_goblin():
    index = {}
    index.update(_cell("NFL", "Receiving Yards", "Standard", "UNDER", 87, 100))
    index.update(_cell("NFL", "Receptions", "Standard", "UNDER", 80, 96))
    index.update(_cell("NFL", "Receiving Yards", "Goblin", "OVER", 80, 100))
    index.update(_cell("NFL", "Receptions", "Goblin", "OVER", 70, 90))
    board = [
        _row(player="Under Guy", game_id="g-u"),
        _row(
            player="Under Two",
            prop="Receptions",
            line=6.5,
            game_id="g-u2",
            stat_g1=2,
            stat_g2=1,
            stat_g3=3,
            stat_g4=0,
            stat_g5=2,
        ),
        _row(
            player="Over Guy",
            pick_type="Goblin",
            side="OVER",
            line=40.5,
            game_id="g-o",
            stat_g1=70,
            stat_g2=80,
            stat_g3=60,
            stat_g4=90,
            stat_g5=75,
        ),
        _row(
            player="Over Two",
            prop="Receptions",
            pick_type="Goblin",
            side="OVER",
            line=3.5,
            game_id="g-o2",
            stat_g1=6,
            stat_g2=5,
            stat_g3=7,
            stat_g4=4,
            stat_g5=8,
        ),
    ]
    built = build_ranked_slips(board, index)
    std = [slip for slip in built["slips"] if slip["family"] == "standard"]
    gob = [slip for slip in built["slips"] if slip["family"] == "goblin"]
    assert std and std[0]["legs"][0]["unproven"] is True
    assert gob and all(leg["pick_type"] == "Goblin" for slip in gob for leg in slip["legs"])
    assert all(not leg.get("unproven") for slip in gob for leg in slip["legs"])


def test_short_pool_shrinks_and_six_needs_six_groups():
    assert candidate_sizes(5, 5, "standard") == [4, 3, 2]
    assert 6 in candidate_sizes(6, 6, "standard")
    assert candidate_sizes(5, 5, "goblin") == [3, 2]
    index = {}
    props = [
        ("Receiving Yards", 87, 100),
        ("Receptions", 80, 96),
        ("Rushing Yards", 37, 56),
        ("Kicking Points", 15, 16),
        ("Pass Yards", 40, 50),
    ]
    board = []
    for i, (prop, hit, n) in enumerate(props):
        index.update(_cell("NFL", prop, "Standard", "UNDER", hit, n))
        board.append(
            _row(player=f"P{i}", prop=prop, game_id=f"g{i}", line=80.5)
        )
    built = build_ranked_slips(board, index)
    assert built["n_placed"] == 1
    assert built["slips"][0]["n_legs"] == 4
    assert built["slips"][0]["placed"] is True

    index.update(_cell("NFL", "Longest Reception", "Standard", "UNDER", 100, 125))
    board.append(_row(player="P5", prop="Longest Reception", game_id="g5", line=80.5))
    built6 = build_ranked_slips(board, index)
    assert built6["slips"][0]["n_legs"] == 4


def test_ev_negative_slip_is_logged_and_not_placed(tmp_path):
    index = {}
    index.update(_cell("NFL", "Receiving Yards", "Standard", "UNDER", 20, 40))
    index.update(_cell("NFL", "Receptions", "Standard", "UNDER", 20, 40))
    board = [
        _row(player="A", game_id="ga"),
        _row(player="B", prop="Receptions", line=8.5, game_id="gb", stat_g1=2, stat_g2=1, stat_g3=3, stat_g4=0, stat_g5=2),
    ]
    log = tmp_path / "track_comparison.jsonl"
    built = record_l5_ranked(board, date="2026-10-19", index=index, path=log)
    assert built["slips"][0]["placed"] is False
    assert built["slips"][0]["ev_label"] == "EV-negative"
    text = log.read_text(encoding="utf-8")
    assert TRACK in text
    assert "l5_values" in text
    assert "cell_wilson_low" in text
    assert "gap_z" in text
    assert "group_rank" in text


def test_postseason_and_missing_cell_stay_out():
    index = _cell("NFL", "Receiving Yards", "Standard", "UNDER", 87, 100)
    playoff, why = screen_leg(_row(season_type="3"), index)
    assert playoff is None and why == "postseason_own_track"
    missing, why2 = screen_leg(_row(prop="Interceptions"), index)
    assert missing is None and why2 == "no_regular_season_cell"


def test_goblin_packs_two_and_three(tmp_path):
    index = {}
    props = ["Points", "Rebounds", "Assists", "3-PT Made", "Steals"]
    teams = ["ATL", "CHI", "DAL", "IND", "NYL"]
    board = []
    for i, prop in enumerate(props):
        index.update(_cell("WNBA", prop, "Goblin", "OVER", 90, 100))
        board.append(
            _row(
                sport="WNBA",
                player=f"G{i}",
                prop=prop,
                pick_type="Goblin",
                side="OVER",
                line=0.5,
                game_id=f"w{i}",
                team=teams[i],
                opp_team="GSV",
                stat_g1=4,
                stat_g2=3,
                stat_g3=5,
                stat_g4=2,
                stat_g5=6,
            )
        )
    built = build_ranked_slips(board, index)
    sizes = [slip["n_legs"] for slip in built["slips"] if slip["placed"]]
    assert sizes == [3, 2]


def _projection(stat, line, odds, player_id="1", wagers=None):
    attrs = {
        "stat_type": stat,
        "line_score": line,
        "odds_type": odds,
        "status": "pre_game",
    }
    if wagers is not None:
        attrs["allowed_wager_types"] = wagers
    return {
        "attributes": attrs,
        "relationships": {"new_player": {"data": {"id": player_id}}},
    }


def _named(player_id, name):
    return {"type": "new_player", "id": player_id, "attributes": {"name": name}}


def test_no_data_log_is_not_five_of_five():
    """A NO_DATA row can carry five numbers that are not the box score."""
    index = _cell("SOCCER", "Shots", "Standard", "UNDER", 55, 74)
    row = _row(
        sport="SOCCER",
        player="Crysencio Summerville",
        prop="Shots",
        line=4.0,
        team="Hilal",
        opp_team="Hazem",
        stat_status="NO_DATA",
        stat_g1=0,
        stat_g2=1,
        stat_g3=0,
        stat_g4=1,
        stat_g5=0,
    )
    leg, reason = screen_leg(row, index)
    assert leg is None and reason == "no_box_log"


def test_nhl_series_stays_out_until_rechecked():
    index = _cell("NHL", "Shots On Goal", "Standard", "UNDER", 45, 73)
    row = _row(
        sport="NHL",
        player="Logan Cooley",
        prop="Shots On Goal",
        line=2.0,
        team="UTA",
        opp_team="PIT",
        stat_g1=1,
        stat_g2=3,
        stat_g3=3,
        stat_g4=0,
        stat_g5=0,
    )
    leg, reason = screen_leg(row, index)
    assert leg is None and reason == "nhl_log_untrusted"


def test_card_last_five_disagreement_drops_the_leg():
    index = _cell("SOCCER", "Shots", "Standard", "UNDER", 55, 74)
    row = _row(
        sport="SOCCER",
        player="Crysencio Summerville",
        prop="Shots",
        line=4.0,
        team="Hilal",
        opp_team="Hazem",
        stat_g1=2,
        stat_g2=1,
        stat_g3=3,
        stat_g4=2,
        stat_g5=2,
        card_l5=[9, 9, 9, 9, 9],
    )
    leg, reason = screen_leg(row, index)
    assert leg is None and reason == "card_log_disagreement"
    row["card_l5"] = [2, 1, 3, 2, 2]
    kept, why = screen_leg(row, index)
    assert why == "" and kept is not None


def test_demon_receiving_yards_is_not_a_standard_under():
    keys = index_projections(
        [_projection("Receiving Yards", 19.5, "demon")],
        [_named("1", "Kaelon Black")],
        "NFL",
    )
    book = CardBook(keys)
    index = _cell("NFL", "Receiving Yards", "Standard", "UNDER", 27, 27)
    row = _row(player="Kaelon Black", prop="Receiving Yards", line=19.5, side="UNDER")
    leg, reason = screen_leg(row, index, book)
    assert leg is None and reason == "card_side_missing"
    built = build_ranked_slips([row], index, book)
    assert built["slips"] == []
    assert built["dropped"]["card_side_missing"] == 1


def test_standard_shots_on_goal_keeps_the_under():
    keys = index_projections(
        [_projection("Shots On Goal", 2, "standard")],
        [_named("1", "Logan Cooley")],
        "NHL",
    )
    book = CardBook(keys)
    index = _cell("NHL", "shots_on_goal", "Standard", "UNDER", 45, 73)
    row = _row(
        sport="NHL",
        player="Logan Cooley",
        prop="Shots On Goal",
        line=2.0,
        side="UNDER",
        team="UTA",
        opp_team="PIT",
        stat_g1=0,
        stat_g2=1,
        stat_g3=0,
        stat_g4=1,
        stat_g5=0,
    )
    leg, reason = screen_leg(row, index, book)
    assert leg is None and reason == "nhl_log_untrusted"


def test_standard_over_token_still_offers_less():
    """PrizePicks lists allowed_wager_types as ['over'] on a two-sided Standard."""
    keys = index_projections(
        [_projection("Shots", 4, "standard", wagers=["over"])],
        [_named("1", "Crysencio Summerville")],
        "SOCCER",
    )
    book = CardBook(keys)
    index = _cell("SOCCER", "Shots", "Standard", "UNDER", 55, 74)
    row = _row(
        sport="SOCCER",
        player="Crysencio Summerville",
        prop="Shots",
        line=4,
        side="UNDER",
        team="Hilal",
        opp_team="Hazem",
        stat_g1=2,
        stat_g2=1,
        stat_g3=3,
        stat_g4=2,
        stat_g5=2,
    )
    leg, reason = screen_leg(row, index, book)
    assert reason == ""
    assert leg is not None


def test_all_zero_hits_blocks_and_faceoffs_stay_out():
    index = {}
    index.update(_cell("NFL", "Hits", "Standard", "UNDER", 100, 203))
    index.update(_cell("NFL", "Blocked Shots", "Standard", "UNDER", 188, 406))
    index.update(_cell("NFL", "Faceoffs Won", "Standard", "UNDER", 54, 344))
    keys = set()
    data = []
    included = []
    for i, stat in enumerate(("Hits", "Blocked Shots", "Faceoffs Won"), start=1):
        data.append(_projection(stat, 2.5, "standard", player_id=str(i)))
        included.append(_named(str(i), f"Player {i}"))
    book = CardBook(index_projections(data, included, "NFL"))
    for i, prop in enumerate(("Hits", "Blocked Shots", "Faceoffs Won"), start=1):
        row = _row(
            sport="NFL",
            player=f"Player {i}",
            prop=prop,
            line=2.5,
            team="UTA",
            opp_team="PIT",
            stat_g1=0,
            stat_g2=0,
            stat_g3=0,
            stat_g4=0,
            stat_g5=0,
        )
        leg, reason = screen_leg(row, index, book)
        assert leg is None and reason == "sacks_taken_box_untrusted"
