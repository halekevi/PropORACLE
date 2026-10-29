"""Stage 1 keep-file eligibility and stage 4 EV slip construction."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402
from utils.ticket_ev_build import (  # noqa: E402
    attach_leg_p,
    build_ev_slips,
    build_sport_then_mix,
    score_slip,
)
from utils.golf_keep_gates import golf_ticket_eligible  # noqa: E402
from utils.ticket_keep_registry import rule_for  # noqa: E402


def _leg(player: str, p: float, **kw) -> dict:
    row = {
        "sport": "WNBA",
        "player": player,
        "prop": "Points",
        "side": "OVER",
        "pick_type": "Goblin",
        "p": p,
        "matchup": kw.pop("matchup", f"{player} vs Opp"),
    }
    row.update(kw)
    return row


def test_ticket_pool_requires_l5_eq_5_after_the_keep_gate():
    from utils.ticket_70_pool import goblin_70_eligible as eligible

    base = {
        "sport": "WNBA",
        "player": "A",
        "prop": "Points",
        "side": "OVER",
        "pick_type": "Goblin",
        "l10_over": 8,
        "def": "Weak",
        "cover": 4.3,
        "prop_tier": "A",
        "season_type": "regular",
    }
    assert eligible({**base, "l5_over": 5})
    assert not eligible({**base, "l5_over": 4})


def test_out_of_season_sports_are_not_registered():
    assert rule_for("NHL") is None
    assert rule_for("NBA") is None
    assert rule_for("CBB") is None
    assert rule_for("WCBB") is None
    assert rule_for("GOLF") is None
    assert rule_for("PGA") is None
    assert not golf_ticket_eligible(
        {
            "sport": "GOLF",
            "pick_type": "Goblin",
            "side": "OVER",
            "l5_over": 5,
            "l10_over": 8,
        }
    )
    assert rule_for("WNBA")[0] == "add"
    assert rule_for("MLB")[0] == "replace"
    assert rule_for("Tennis")[0] == "replace"
    nhl = {
        "sport": "NHL",
        "player": "A",
        "prop": "sog",
        "side": "OVER",
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 9,
        "def": "Weak",
        "cover": 2.0,
    }
    assert not goblin_70_eligible(nhl)


def test_missing_cell_stores_source_and_does_not_invent_p():
    row = _leg("A", 0.5)
    attach_leg_p(row, recipe="WNBA:add", index={})
    assert row["p"] is None
    assert row["p_source"] == "none"
    assert row["p_n"] == 0
    assert "WNBA:add" in row["p_cell"]


def test_three_leg_power_beats_two_when_ev_says_so():
    legs = [_leg("A", 0.80), _leg("B", 0.75), _leg("C", 0.70)]
    scored = score_slip(legs, family="standard")
    assert scored is not None
    assert scored["product"] == "Power"
    assert scored["all_hit_x"] == 6.0
    assert scored["ev"] > 0


def test_two_leg_is_kept_when_three_leg_ev_fails():
    pool = [_leg("A", 0.60), _leg("B", 0.60), _leg("C", 0.40)]
    slips = build_ev_slips(pool, family="standard", taken=set(), fold=lambda n: str(n))
    assert len(slips) == 1
    assert [x["player"] for x in slips[0]["legs"]] == ["A", "B"]
    assert slips[0]["product"] == "Power"
    assert slips[0]["all_hit_x"] == 3.0


def test_stop_when_next_slip_is_not_positive_ev():
    pool = [_leg(p, 0.50) for p in ("A", "B", "C", "D")]
    assert build_ev_slips(pool, family="standard", taken=set(), fold=lambda n: str(n)) == []


def test_goblin_two_leg_clears_the_1_4_floor():
    pool = [_leg("A", 0.90), _leg("B", 0.90)]
    slips = build_ev_slips(pool, family="goblin", taken=set(), fold=lambda n: str(n))
    assert len(slips) == 1
    assert slips[0]["all_hit_x"] == 1.5
    assert slips[0]["stake"] == 20
    assert slips[0]["ev"] > 0


def test_mixed_floors():
    from utils.n_correct_payout import lookup_composition_p25

    one = score_slip(
        [_leg("G", 0.80), _leg("S1", 0.80, pick_type="Standard"), _leg("S2", 0.80, pick_type="Standard")]
    )
    two = score_slip(
        [
            _leg("G1", 0.80),
            _leg("G2", 0.80),
            _leg("S", 0.80, pick_type="Standard"),
        ]
    )
    one_q = lookup_composition_p25(2, 1, 3, "Power")
    two_q = lookup_composition_p25(1, 2, 3, "Power")
    assert one is not None and one_q is not None
    assert two is not None and two_q is not None
    assert one["payout_source"] == "n_correct_p25"
    assert two["payout_source"] == "n_correct_p25"
    assert one["all_hit_x"] == one_q["n_correct"][3]
    assert two["all_hit_x"] == two_q["n_correct"][3]


def test_goblin_and_standard_can_share_a_slip():
    gob = _leg("Gob", 0.85)
    std = _leg("Std", 0.80, pick_type="Standard")
    from utils.n_correct_payout import lookup_composition_p25

    slips = build_ev_slips([gob, std], taken=set(), fold=lambda n: str(n))
    quote = lookup_composition_p25(1, 1, 2, "Power")
    assert len(slips) == 1
    assert slips[0]["family"] == "mix"
    assert quote is not None
    assert slips[0]["all_hit_x"] == quote["n_correct"][2]
    assert slips[0]["payout_source"] == "n_correct_p25"
    assert slips[0]["product"] == "Power"
    assert slips[0]["ev"] > 0


def test_same_player_different_prop_can_share_a_slip():
    pool = [
        _leg("A", 0.80, prop="Points"),
        _leg("A", 0.80, prop="Assists"),
        _leg("B", 0.80, prop="Rebounds"),
    ]
    slips = build_ev_slips(pool, family="standard", taken=set(), fold=lambda n: str(n))
    assert len(slips) == 1
    assert sorted(x["prop"] for x in slips[0]["legs"]) == ["Assists", "Points", "Rebounds"]


def test_same_player_same_prop_is_used_once():
    pool = [
        _leg("A", 0.90, prop="Points"),
        _leg("A", 0.80, prop="Points"),
        _leg("B", 0.90, prop="Rebounds"),
    ]
    slips = build_ev_slips(pool, family="standard", taken=set(), fold=lambda n: str(n))
    assert len(slips) == 1
    points = [x for x in slips[0]["legs"] if x["prop"] == "Points"]
    assert len(points) == 1
    assert points[0]["p"] == 0.90


def test_best_ev_slip_is_not_the_top_three_by_hit_rate():
    gobs = [_leg(p, 0.76, prop="Points") for p in ("A", "B", "C")]
    stds = [_leg(p, 0.75, pick_type="Standard", prop="Points") for p in ("D", "E")]
    slips = build_ev_slips(gobs + stds, taken=set(), fold=lambda n: str(n))
    assert slips[0]["family"] == "standard"
    assert {x["player"] for x in slips[0]["legs"]} == {"D", "E"}


def test_same_player_stops_at_two_legs():
    pool = [
        _leg("A", 0.90, prop="Points"),
        _leg("A", 0.90, prop="Assists"),
        _leg("A", 0.90, prop="Rebounds"),
        _leg("B", 0.90, prop="Points"),
    ]
    slips = build_ev_slips(pool, family="standard", taken=set(), fold=lambda n: str(n))
    assert len(slips) == 1
    names = [x["player"] for x in slips[0]["legs"]]
    assert names.count("A") == 2
    assert "B" in names


def test_sport_that_cannot_fill_mixes_with_the_rest():
    pool = [
        _leg("A", 0.90, sport="CFB", prop="Pass Yards"),
        _leg("B", 0.90, sport="SOCCER", prop="Shots"),
    ]
    slips = build_sport_then_mix(pool, taken=set(), fold=lambda n: str(n))
    assert len(slips) == 1
    assert {x["sport"] for x in slips[0]["legs"]} == {"CFB", "SOCCER"}


def test_a_sport_that_can_fill_keeps_its_own_slip():
    pool = [
        _leg("A", 0.90, sport="CFB"),
        _leg("B", 0.90, sport="CFB", prop="Assists"),
        _leg("C", 0.90, sport="SOCCER", prop="Shots"),
        _leg("D", 0.90, sport="SOCCER", prop="Shots"),
    ]
    slips = build_sport_then_mix(pool, taken=set(), fold=lambda n: str(n))
    assert len(slips) == 2
    assert {x["sport"] for x in slips[0]["legs"]} == {"CFB"}
    assert {x["sport"] for x in slips[1]["legs"]} == {"SOCCER"}


def test_all_goblin_pair_lands_on_the_1_4_floor():
    pool = [
        _leg("Weaver", 0.763, sport="CFB", prop="Pass Yards"),
        _leg("Kromah", 0.763, sport="CFB", prop="Rush Yards"),
        _leg("Kromah", 0.700, sport="CFB", prop="Rush Yards", pick_type="Standard"),
    ]
    slips = build_sport_then_mix(pool, taken=set(), fold=lambda n: str(n))
    assert len(slips) == 1
    kinds = {x["player"]: x["pick_type"] for x in slips[0]["legs"]}
    assert kinds == {"Weaver": "Goblin", "Kromah": "Goblin"}
    assert slips[0]["dollar_test"] is False
    assert slips[0]["all_hit_x"] >= 1.4


def test_all_goblin_slip_under_a_dollar_is_kept():
    slips = build_ev_slips(
        [_leg("A", 0.70), _leg("B", 0.70)],
        family="goblin",
        taken=set(),
        fold=lambda n: str(n),
    )
    assert len(slips) == 1
    assert slips[0]["family"] == "goblin"
    assert slips[0]["all_hit_x"] >= 1.4
    assert slips[0]["ev"] < 0
    assert slips[0]["dollar_test"] is False
    assert slips[0]["dollar_test_required"] is False


def test_standard_slip_must_clear_the_dollar_test():
    kept = build_ev_slips(
        [
            _leg("A", 0.70, pick_type="Standard"),
            _leg("B", 0.70, pick_type="Standard"),
        ],
        family="standard",
        taken=set(),
        fold=lambda n: str(n),
    )
    assert len(kept) == 1
    assert kept[0]["all_hit_x"] == 3.0
    assert kept[0]["dollar_test"] is True
    assert kept[0]["dollar_test_required"] is True
    dropped = build_ev_slips(
        [
            _leg("C", 0.50, pick_type="Standard"),
            _leg("D", 0.50, pick_type="Standard"),
        ],
        family="standard",
        taken=set(),
        fold=lambda n: str(n),
    )
    assert dropped == []


def test_mixed_slip_under_a_dollar_is_dropped():
    import math

    from utils.ticket_ev_build import power_multiplier

    px, _src = power_multiplier(2, family="mix", n_goblin=1)
    assert px >= 1.4
    under = math.sqrt(0.90 / px)
    over = math.sqrt(min(0.99, 1.05 / px))
    assert 0 < under < 1
    dropped = build_ev_slips(
        [_leg("G", under), _leg("S", under, pick_type="Standard")],
        taken=set(),
        fold=lambda n: str(n),
    )
    assert dropped == []
    assert 0 < over < 1
    kept = build_ev_slips(
        [_leg("G", over), _leg("S", over, pick_type="Standard")],
        taken=set(),
        fold=lambda n: str(n),
    )
    assert len(kept) == 1
    assert kept[0]["family"] == "mix"
    assert kept[0]["dollar_test"] is True
    assert kept[0]["dollar_test_required"] is True


def test_a_dollar_clearing_mix_outranks_an_all_goblin_slip():
    slips = build_ev_slips(
        [
            _leg("A", 0.45),
            _leg("B", 0.45),
            _leg("C", 0.80),
            _leg("D", 0.80, pick_type="Standard"),
        ],
        taken=set(),
        fold=lambda n: str(n),
    )
    assert slips[0]["family"] == "mix"
    assert slips[0]["dollar_test"] is True
    assert {x["player"] for x in slips[0]["legs"]} == {"C", "D"}
    goblins = [s for s in slips if s["family"] == "goblin"]
    assert len(goblins) == 1
    assert goblins[0]["dollar_test"] is False
    assert {x["player"] for x in goblins[0]["legs"]} == {"A", "B"}


def test_same_game_legs_can_share_a_slip():
    pool = [
        _leg("A", 0.80, matchup="NY vs LA"),
        _leg("B", 0.80, matchup="NY vs LA"),
    ]
    slips = build_ev_slips(pool, family="standard", taken=set(), fold=lambda n: str(n))
    assert len(slips) == 1
    assert {x["player"] for x in slips[0]["legs"]} == {"A", "B"}
