"""ATP/WTA rank → Elite–Weak, plus Sackmann opponent lookup for tennis D."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from utils.defense_tiers import d_aligned
from utils.tennis_rank_d import (
    TennisOppRankIndex,
    rank_to_tier,
    tennis_opp_tier,
)

_REPO = Path(__file__).resolve().parents[1]


def test_rank_to_tier_bands():
    assert rank_to_tier(1) == "Elite"
    assert rank_to_tier(10) == "Elite"
    assert rank_to_tier(11) == "Above Avg"
    assert rank_to_tier(25) == "Above Avg"
    assert rank_to_tier(26) == "Avg"
    assert rank_to_tier(50) == "Avg"
    assert rank_to_tier(51) == "Below Avg"
    assert rank_to_tier(100) == "Below Avg"
    assert rank_to_tier(101) == "Weak"
    assert rank_to_tier(250) == "Weak"
    assert rank_to_tier("") == ""
    assert rank_to_tier(None) == ""


def test_tennis_d_uses_opponent_rank_bands():
    assert d_aligned("TENNIS", "OVER", rank_to_tier(120)) is True
    assert d_aligned("TENNIS", "OVER", rank_to_tier(8)) is False
    assert d_aligned("TENNIS", "UNDER", rank_to_tier(8)) is True
    assert d_aligned("TENNIS", "OVER", rank_to_tier(40)) is False


def test_row_opponent_rank_beats_blank_def_tier():
    row = {"player": "Carlos Alcaraz", "opponent_rank": 80, "def_tier": ""}
    assert tennis_opp_tier(row) == "Below Avg"


def test_sackmann_hurkacz_united_cup():
    sack = _REPO / "Sports" / "Tennis" / "data" / "sackmann" / "atp_matches_2026.csv"
    if not sack.is_file():
        return
    idx = TennisOppRankIndex.from_repo(_REPO)
    rec = idx.lookup("Hubert Hurkacz", date(2026, 1, 5))
    assert rec is not None
    assert rec.player_rank == 83
    assert rec.opp_rank in (156, 9)
    assert rec.opp_tier in ("Weak", "Elite")
    row = {"player": "Hubert Hurkacz"}
    assert tennis_opp_tier(row, slate=date(2026, 1, 5), index=idx) in (
        "Weak",
        "Elite",
        "Above Avg",
        "Below Avg",
        "Avg",
    )
