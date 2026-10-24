"""Tests for MLB starter K/9 soft helpers."""

from utils.mlb_starter_k9 import (
    hitter_k_soft_bump,
    ip_to_outs,
    k9_from_so_ip,
    resolve_hitter_k_soft_k9,
    starter_k9_from_staff_rec,
)


def test_ip_to_outs_baseball_notation():
    assert ip_to_outs(132.1) == 132 * 3 + 1
    assert ip_to_outs(88.2) == 88 * 3 + 2
    assert ip_to_outs(100.0) == 300


def test_k9_from_so_ip():
    # 118 SO in 132.1 IP ≈ 8.03
    k9 = k9_from_so_ip(118, 132.1)
    assert k9 is not None
    assert 7.9 < k9 < 8.2


def test_starter_k9_from_staff_rec():
    rec = {"strikeouts": 166, "innings_pitched": 125.0}
    k9 = starter_k9_from_staff_rec(rec)
    assert k9 is not None
    assert abs(k9 - (166 * 9 / 125)) < 0.05


def test_resolve_prefers_vs_hand():
    assert resolve_hitter_k_soft_k9(vs_hand_k9=11.2, season_k9=8.0) == 11.2
    assert resolve_hitter_k_soft_k9(vs_hand_k9=None, season_k9=8.5) == 8.5


def test_hitter_k_soft_bump_polarity():
    assert hitter_k_soft_bump(11.0, "OVER") > 0
    assert hitter_k_soft_bump(11.0, "UNDER") < 0
    assert hitter_k_soft_bump(6.0, "OVER") < 0
    assert hitter_k_soft_bump(6.0, "UNDER") > 0
    assert hitter_k_soft_bump(8.5, "OVER") == 0.0
    assert hitter_k_soft_bump(None, "OVER") == 0.0
