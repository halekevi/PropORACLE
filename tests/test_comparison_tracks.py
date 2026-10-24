"""Pitcher L10 prices, comparison tracks, and paid N-correct stamps."""
from __future__ import annotations

from utils.category_hit_rate import _norm_dir, _norm_pick, _norm_prop, _norm_sport
from utils.comparison_tracks import (
    TRACK_MIXED,
    build_one_standard_slips,
    is_real_scraped_quote,
    qualifying_standard_cell,
    real_scraped_payout_x,
    summarize_tracks,
    track_log_row,
)
from utils.mlb_keep_gates import pitcher_l10_parent_cell
from utils.n_correct_payout import _CAPTURE_CACHE, capture_composition_samples
from utils.postseason_slate import apply_postseason_price


def _std_index(n: float, hr: float) -> dict:
    key = (_norm_sport("WNBA"), _norm_prop("Points", "WNBA"), _norm_pick("Standard"), _norm_dir("OVER"))
    return {key: {"hr": hr, "n": n, "hit": hr * n}}


def _row(**kw) -> dict:
    base = {
        "sport": "WNBA",
        "player": "A",
        "prop": "Points",
        "side": "OVER",
        "pick_type": "Standard",
        "season_type": "regular",
        "l5_over": 5,
        "p": 0.66,
    }
    base.update(kw)
    return base


def test_pitcher_price_is_the_l10_parent_not_the_l5_slice():
    outs = pitcher_l10_parent_cell({"sport": "MLB", "prop": "Pitching Outs"})
    ks = pitcher_l10_parent_cell({"sport": "MLB", "prop": "Ks"})
    assert outs is not None and ks is not None
    assert (outs["hit"], outs["n"]) == (399, 498)
    assert (ks["hit"], ks["n"]) == (743, 975)
    assert abs(outs["hr"] - 58 / 96) > 0.15
    assert abs(ks["hr"] - 46 / 64) > 0.03
    post = {"sport": "MLB", "prop": "Pitching Outs"}
    assert apply_postseason_price(post, outs["hr"])
    assert abs(post["p"] - (399 / 498 - 0.03)) < 1e-9
    assert pitcher_l10_parent_cell({"sport": "TENNIS", "prop": "Pitching Outs"}) is None


def test_standard_cell_needs_regular_season_l5_and_n30():
    index = _std_index(30, 0.64)
    assert qualifying_standard_cell(_row(), index)["n"] == 30
    assert qualifying_standard_cell(_row(l5_over=4), index) is None
    assert qualifying_standard_cell(_row(season_type="3"), index) is None
    assert qualifying_standard_cell(_row(), _std_index(29, 0.80)) is None


def test_mixed_track_is_one_standard_and_skips_a_goblin_player():
    gobs = [
        _row(player="A", pick_type="Goblin", p=0.80),
        _row(player="B", pick_type="Goblin", prop="Rebounds", p=0.78),
        _row(player="C", pick_type="Goblin", prop="Assists", p=0.76),
    ]
    stds = [_row(player="D", pick_type="Standard", p=0.64)]
    slips = build_one_standard_slips(
        gobs,
        stds,
        taken=set(),
        fold=lambda name: str(name),
        blocked_players={"A"},
        max_slips=2,
    )
    assert len(slips) == 1
    names = {leg["player"] for leg in slips[0]["legs"]}
    assert "A" not in names
    assert "D" in names
    assert sum(1 for leg in slips[0]["legs"] if leg["pick_type"] == "Standard") == 1
    logged = track_log_row(
        {**slips[0], "track": TRACK_MIXED, "id": "M2-1", "stake": 20},
        date="2026-10-13",
    )
    assert logged["track"] == TRACK_MIXED
    assert logged["n_standard"] == 1
    assert "WNBA" in logged["standard_sports"]


def test_paid_stamps_enter_the_composition_samples():
    _CAPTURE_CACHE.clear()
    two = capture_composition_samples(0, 2, 2)
    three = capture_composition_samples(0, 3, 3)
    assert 1.7 in two
    assert 1.5 in three


def test_is_real_scraped_quote_fails_closed():
    assert is_real_scraped_quote("live_cdp", "2026-10-22T21:52:19-04:00")
    assert is_real_scraped_quote("n_correct_live", "2026-09-09T22:05:48")
    assert not is_real_scraped_quote("n_correct_live", None)
    assert not is_real_scraped_quote("n_correct_live", "")
    assert not is_real_scraped_quote("mix_floor", "2026-10-22T21:52:19-04:00")
    assert not is_real_scraped_quote("goblin_3", "2026-10-22T21:52:19-04:00")
    assert not is_real_scraped_quote("n_correct_median", "2026-10-22T21:52:19-04:00")
    assert not is_real_scraped_quote("", "2026-10-22T21:52:19-04:00")
    assert not is_real_scraped_quote(None, None)
    assert real_scraped_payout_x(
        {
            "payout_source": "n_correct_live",
            "captured_at": None,
            "scraped_payout": 2.0,
        }
    ) is None
    assert real_scraped_payout_x(
        {
            "payout_source": "live_cdp",
            "captured_at": "2026-10-22T21:52:19-04:00",
            "scraped_payout": 1.6,
        }
    ) == 1.6


def test_track_summary_uses_scraped_payouts_only():
    rows = [
        {
            "track": "gated_goblin",
            "result": "win",
            "p_win": 0.49,
            "payout_source": "live_cdp",
            "captured_at": "2026-10-01T12:00:00Z",
            "scraped_payout": 1.7,
            "stake": 20,
            "standard_sports": [],
        },
        {
            "track": "gated_goblin",
            "result": "loss",
            "p_win": 0.49,
            "payout_source": "live_cdp",
            "captured_at": "2026-10-01T12:00:00Z",
            "scraped_payout": 1.7,
            "stake": 20,
            "standard_sports": [],
        },
        {
            "track": "gated_goblin",
            "result": None,
            "p_win": 0.49,
            "scraped_payout": None,
            "stake": 20,
            "standard_sports": [],
        },
        # Mislabel / pin floor — must not enter scraped ROI or real-quote n.
        {
            "track": "gated_goblin",
            "result": "win",
            "p_win": 0.49,
            "payout_source": "n_correct_live",
            "captured_at": None,
            "scraped_payout": 2.0,
            "stake": 20,
            "standard_sports": [],
        },
        {
            "track": "gated_goblin",
            "result": "win",
            "p_win": 0.49,
            "payout_source": "mix_floor",
            "captured_at": "2026-10-01T12:00:00Z",
            "scraped_payout": 2.0,
            "stake": 20,
            "standard_sports": [],
        },
        {
            "track": "gated_goblin",
            "result": "loss",
            "p_win": 0.49,
            "payout_source": "",
            "captured_at": None,
            "scraped_payout": 1.7,
            "stake": 20,
            "standard_sports": [],
        },
    ]
    summary = summarize_tracks(rows)
    assert summary[0]["graded"] == 5
    assert summary[0]["wins"] == 3
    assert summary[0]["scraped_n"] == 2
    assert summary[0]["real_quote_n"] == 2
    assert summary[0]["roi_scraped"] < 0


def test_track_log_row_omits_scraped_payout_without_capture():
    logged = track_log_row(
        {
            "id": "M2-1",
            "track": TRACK_MIXED,
            "payout_source": "n_correct_live",
            "captured_at": None,
            "all_hit_x": 2.0,
            "scraped_payout": 2.0,
            "stake": 20,
            "legs": [
                {"pick_type": "Goblin", "sport": "Soccer"},
                {"pick_type": "Standard", "sport": "NFL"},
            ],
        },
        date="2026-10-23",
    )
    assert logged["payout_source"] == "n_correct_live"
    assert logged["captured_at"] is None
    assert logged["real_quote"] is False
    assert logged["scraped_payout"] is None
    assert logged["quote"] == 2.0
