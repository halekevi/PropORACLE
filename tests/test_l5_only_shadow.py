"""L5=5 shadow is priced ungated, logged, and kept off the placed card."""
from __future__ import annotations

import sys
from pathlib import Path

from utils.l5_only_shadow import (
    TRACK_L5_SHADOW,
    build_shadow_slips,
    price_shadow_leg,
    record_l5_shadow,
    summarize_shadow_props,
    ungated_cell,
)

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))


def _tennis(**kw) -> dict:
    base = {
        "sport": "TENNIS",
        "player": "A",
        "prop": "Total Games",
        "side": "OVER",
        "pick_type": "Goblin",
        "line": 15.5,
        "l5_over": 5,
        "l10_over": 3,
        "opp_team": "Opponent",
    }
    base.update(kw)
    return base


def test_tennis_goblin_uses_the_corrected_ungated_cell():
    inflated = {
        ("TENNIS", "match_total_games", "Goblin", "OVER"): {"hr": 37 / 47, "n": 47, "hit": 37},
        ("TENNIS", "games_won", "Goblin", "OVER"): {"hr": 28 / 40, "n": 40, "hit": 28},
    }
    tg = ungated_cell(_tennis(), inflated)
    gw = ungated_cell(_tennis(prop="Games Won", player="B"), inflated)
    assert tg is not None and (tg["hit"], tg["n"]) == (665, 1050)
    assert abs(tg["hr"] - 0.702) > 0.05
    assert gw is not None and (gw["hit"], gw["n"]) == (819, 1224)
    priced = price_shadow_leg(_tennis(), inflated)
    assert priced is not None
    assert priced["p_source"] == "ungated_cell"
    assert priced["placed"] is False
    assert abs(priced["p"] - 665 / 1050) < 1e-9


def test_keep_gate_is_not_required_and_bad_rows_stay_out():
    index = {}
    assert price_shadow_leg(_tennis(l10_over=3), index) is not None
    assert price_shadow_leg(_tennis(l5_over=4), index) is None
    assert price_shadow_leg(_tennis(opp_team="UNKNOWN"), index) is None
    assert price_shadow_leg(_tennis(minutes_status="dnp"), index) is None
    assert price_shadow_leg(_tennis(pick_type="Demon"), index) is None
    playoff = {
        "sport": "MLB",
        "player": "Jo Adell",
        "prop": "Hitter Strikeouts",
        "side": "OVER",
        "pick_type": "Goblin",
        "l5_over": 5,
        "season_type": "3",
        "team": "LAA",
        "opp_team": "CLE",
    }
    assert price_shadow_leg(playoff, index) is None
    regular = dict(playoff, season_type="regular", player="Regular")
    regular_index = {
        ("MLB", "hitter_strikeouts", "Goblin", "OVER"): {"hr": 0.55, "n": 100, "hit": 55}
    }
    # Canon key is whatever lookup uses. Stamp via the function's own lookup
    # by passing a matching catalog after checking the prop canon.
    from utils.prop_norm import canon_prop
    from utils.category_hit_rate import _norm_dir, _norm_pick, _norm_prop, _norm_sport

    key = (
        _norm_sport("MLB"),
        _norm_prop(canon_prop("MLB", "Hitter Strikeouts"), "MLB"),
        _norm_pick("Goblin"),
        _norm_dir("OVER"),
    )
    regular_index = {key: {"hr": 0.55, "n": 100.0, "hit": 55.0}}
    priced = price_shadow_leg(regular, regular_index)
    assert priced is not None
    assert abs(priced["p"] - 0.55) < 1e-9


def test_shadow_slips_are_two_legs_and_not_placed(tmp_path: Path):
    legs = [
        price_shadow_leg(_tennis(player="Bublik", prop="Games Won", line=9.5), {}),
        price_shadow_leg(_tennis(player="Stefanini", line=15.5), {}),
        price_shadow_leg(_tennis(player="Fernandez", line=17.5), {}),
        price_shadow_leg(
            _tennis(player="Krueger", pick_type="Standard", side="UNDER", l5_over=0, l5_under=5, line=21.5),
            {
                ("TENNIS", "matchtotalgames", "Standard", "UNDER"): {
                    "hr": 0.42,
                    "n": 31,
                    "hit": 13,
                }
            },
        ),
    ]
    assert all(leg is not None for leg in legs)
    slips = build_shadow_slips([leg for leg in legs if leg is not None])
    assert slips
    assert all(slip["placed"] is False and slip["track"] == TRACK_L5_SHADOW for slip in slips)
    assert all(len(slip["legs"]) == 2 for slip in slips)
    used = []
    for slip in slips:
        names = [leg["player"] for leg in slip["legs"]]
        assert len(names) == len(set(names))
        used.extend(names)
    assert len(used) == len(set(used))
    logged = record_l5_shadow(
        [_tennis(player="Bublik", prop="Games Won"), _tennis(player="Stefanini")],
        date="2026-10-13",
        index={},
        path=tmp_path / "track.jsonl",
    )
    assert logged["placed"] is False
    assert logged["n_slips"] == 1
    text = (tmp_path / "track.jsonl").read_text(encoding="utf-8")
    assert "l5_only_shadow" in text
    assert '"placed": false' in text


def test_forward_band_flags_only_when_it_clears_the_median_quote():
    def row(result: str, pick: str = "Goblin") -> dict:
        return {
            "track": TRACK_L5_SHADOW,
            "kind": "leg",
            "sport": "TENNIS",
            "prop": "Total Games",
            "pick_type": pick,
            "direction": "OVER",
            "result": result,
            "p": 0.633,
        }

    short = summarize_shadow_props([row("hit")] * 10 + [row("miss")] * 10)
    assert short[0]["clears_breakeven"] is False
    hot = summarize_shadow_props([row("hit")] * 100)
    assert hot[0]["clears_breakeven"] is True
    assert hot[0]["breakeven"] > 0.84


def test_shadow_track_is_outside_the_live_card_allowlist():
    from build_goblin70_tickets import is_g70_group

    group = {
        "group_name": "L5 shadow",
        "tickets": [{"ticket_track": "l5_only_shadow", "track": "l5_only_shadow"}],
    }
    assert is_g70_group(group) is False
