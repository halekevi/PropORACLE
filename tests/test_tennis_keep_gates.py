"""Tennis Goblin keep gates (list + Goblin-70)."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from rank_best_props_today import _clears_list_gate  # noqa: E402
from utils.tennis_keep_gates import (  # noqa: E402
    tennis_goblin_keep_eligible,
    tennis_standard_under_serve_eligible,
)
from utils.ticket_70_pool import (  # noqa: E402
    goblin_70_eligible,
    standard_flex_kind,
    standard_ticket_eligible,
    standard_ticket_p,
)


def _ten(**kwargs) -> dict:
    row = {
        "sport": "TENNIS",
        "player": "Test",
        "prop": "Total Games Won",
        "side": "OVER",
        "line": 7.5,
        "standard_line": 12.5,
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "cover": 1.5,
        "def": "Elite",
    }
    row.update(kwargs)
    return row


def test_games_won_needs_std_discount_4():
    ok = _ten()
    assert tennis_goblin_keep_eligible(ok)
    assert goblin_70_eligible(ok)  # cover floor skipped for juiced Games Won
    assert _clears_list_gate(ok)
    soft = _ten(standard_line=9.5)  # only 2 under Standard
    assert not tennis_goblin_keep_eligible(soft)
    assert not goblin_70_eligible(soft)
    assert not _clears_list_gate(soft)
    missing_std = _ten()
    missing_std.pop("standard_line")
    assert not tennis_goblin_keep_eligible(missing_std)
    # Soft gap can still keep via L10 + lefty / 2nd-won.
    assert tennis_goblin_keep_eligible(_ten(standard_line=9.5, opp_lefty="Y", l10_over=8))
    assert tennis_goblin_keep_eligible(
        _ten(standard_line=9.5, l5_second_won_pct=46.0, l10_over=9)
    )
    assert not tennis_goblin_keep_eligible(
        _ten(standard_line=9.5, l5_second_won_pct=40.0, l10_over=9, opp_lefty="N")
    )
    # Gap keep still ignores thin L10.
    thin_l10 = _ten(l10_over=3)
    assert tennis_goblin_keep_eligible(thin_l10)
    assert goblin_70_eligible(thin_l10)
    # D is not part of the tennis keep gate.
    assert tennis_goblin_keep_eligible(_ten(**{"def": "Elite"}))


def test_total_games_needs_l5_4_and_l10_8():
    tg = _ten(prop="Total Games", cover=5.0, line=21.5, standard_line=21.5)
    assert tennis_goblin_keep_eligible(tg)
    assert goblin_70_eligible(tg)
    assert _clears_list_gate(tg)
    assert not tennis_goblin_keep_eligible(dict(tg, l5_over=3))
    assert not tennis_goblin_keep_eligible(dict(tg, l10_over=7))
    assert tennis_goblin_keep_eligible(dict(tg, l5_over=4))
    # Alternate: L10>=8 + vs lefty (no L5 required).
    assert tennis_goblin_keep_eligible(dict(tg, l5_over=2, l10_over=8, opp_lefty="Y"))
    assert not tennis_goblin_keep_eligible(dict(tg, l5_over=2, l10_over=8, opp_lefty="N"))


def test_fade_aces_df_standard_demon():
    assert not tennis_goblin_keep_eligible(_ten(prop="Aces"))
    assert not goblin_70_eligible(_ten(prop="Aces", cover=4.0))
    assert not tennis_goblin_keep_eligible(_ten(prop="Double Faults"))
    std = _ten(pick_type="Standard")
    assert not tennis_goblin_keep_eligible(std)
    assert not standard_ticket_eligible(std)
    assert not _clears_list_gate(std)
    assert not _clears_list_gate(_ten(pick_type="Demon"))


def test_standard_under_aces_df_ungated_keep():
    aces = _ten(prop="Aces", pick_type="Standard", side="UNDER", cover=0.0)
    df = _ten(prop="Double Faults", pick_type="Standard", side="UNDER", cover=0.0)
    for row in (aces, df):
        assert tennis_standard_under_serve_eligible(row)
        assert standard_ticket_eligible(row)
        assert _clears_list_gate(row)
        assert standard_flex_kind(row) == "tennis_serve_under"
        assert standard_ticket_p("tennis_serve_under") == 0.90
    over_aces = _ten(prop="Aces", pick_type="Standard", side="OVER")
    assert not tennis_standard_under_serve_eligible(over_aces)
    assert not standard_ticket_eligible(over_aces)
    assert not _clears_list_gate(over_aces)


def test_standard_over_games_won_gap_or_second_won():
    from utils.tennis_keep_gates import tennis_standard_games_won_over_eligible

    gap = _ten(
        prop="Total Games Won",
        pick_type="Standard",
        side="OVER",
        l10_over=8,
        dist_l5=5.5,
        cover=2.0,
    )
    assert tennis_standard_games_won_over_eligible(gap)
    assert standard_ticket_eligible(gap)
    assert _clears_list_gate(gap)
    sw = _ten(
        prop="Total Games Won",
        pick_type="Standard",
        side="OVER",
        l10_over=9,
        l5_second_won_pct=46.0,
        cover=2.0,
    )
    sw.pop("dist_l5", None)
    assert tennis_standard_games_won_over_eligible(sw)
    assert standard_ticket_eligible(sw)
    weak = _ten(
        prop="Total Games Won",
        pick_type="Standard",
        side="OVER",
        l10_over=9,
        l5_second_won_pct=40.0,
        dist_l5=2.0,
        cover=2.0,
    )
    assert not tennis_standard_games_won_over_eligible(weak)
