"""Tests for scrape-gated mix / all-Goblin Power + Flex table fill."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import build_predicted_payout_tables as t  # noqa: E402


def test_parse_comp_and_flex_scale():
    assert t._parse_comp("1S+1G+0D") == (1, 1, 0)
    assert t._parse_comp("2L_2G") == (0, 2, 0)
    ladder = t.scale_flex(6, 0.68)
    assert ladder["6"] == 17.0
    assert ladder["5"] == 1.36
    assert ladder["4"] == 0.272


def test_predict_power_canonical_and_observed():
    observed = {
        (2, 2): {
            "power_x": 1.37,
            "n_scrapes": 6,
            "power_min": 1.2,
            "power_max": 1.5,
        }
    }
    canonical = t.predict_power(
        2, 0, observed={}, floors={}, dpu=0.15, typical_delta=1.5, family_n=0, global_n=0
    )
    assert canonical["status"] == "canonical"
    assert canonical["power_x"] == 3.0

    obs = t.predict_power(
        2,
        2,
        observed=observed,
        floors={},
        dpu=0.15,
        typical_delta=1.5,
        family_n=6,
        global_n=6,
    )
    assert obs["status"] == "observed"
    assert obs["power_x"] == 1.37


def test_need_scrapes_until_family_ready():
    out = t.predict_power(
        4,
        2,
        observed={},
        floors={},
        dpu=None,
        typical_delta=1.5,
        family_n=2,
        global_n=2,
        min_family=5,
        min_fit=8,
    )
    assert out["status"] == "need_scrapes"
    assert out["power_x"] is None
    assert out["need_scrapes"] == 3


def test_predicted_flex_from_power_scale_after_enough_scrapes():
    scrapes = []
    for i in range(8):
        scrapes.append(
            {
                "n_legs": 2,
                "n_s": 0,
                "n_g": 2,
                "power_x": 1.4,
                "deltas": [1.5, 1.5],
                "flex_ladder": {},
            }
        )
        scrapes.append(
            {
                "n_legs": 3,
                "n_s": 1,
                "n_g": 2,
                "power_x": 2.8,
                "deltas": [1.0, 2.0],
                "flex_ladder": {},
            }
        )
    tables = t.build_tables(scrapes=scrapes, floors={}, min_observed=3, min_family=5, min_fit=8)
    assert tables["fit"]["ready"] is True
    all_g2 = next(r for r in tables["power_grid"] if r["composition"] == "0S+2G")
    assert all_g2["status"] in {"observed", "predicted"}
    assert all_g2["power_x"] is not None
    flex2 = next(r for r in tables["flex_grid"] if r["composition"] == "0S+2G")
    assert flex2["all_correct_x"] is not None
    mix = next(r for r in tables["power_grid"] if r["composition"] == "1S+2G")
    assert mix["status"] in {"observed", "predicted"}
    # 4-leg mix should predict once global fit is ready.
    mix4 = next(r for r in tables["power_grid"] if r["composition"] == "2S+2G")
    assert mix4["status"] == "predicted"
    flex4 = next(r for r in tables["flex_grid"] if r["composition"] == "2S+2G")
    assert flex4["ladder"]["4"] > 0
    std6 = next(r for r in tables["power_grid"] if r["composition"] == "6S+0G")
    assert std6["power_x"] == 40.0
    flex6 = next(r for r in tables["flex_grid"] if r["composition"] == "6S+0G")
    assert flex6["ladder"]["6"] == 25.0
    assert flex6["ladder"]["5"] == 2.0


def test_each_goblin_adds_its_own_delta_cut():
    """Two Δ=1.0 Goblins cut twice the 1.0-bin amount, not 2 × mean(Δ) × dpu."""
    scale = t.scale_from_goblin_deltas(
        4,
        [1.0, 1.0],
        step_cuts={"1": 0.18, "2": 0.10},
        dpu=0.5,
    )
    # 1 - 0.18 - 0.18 = 0.64  (if we averaged Δ=1 and used dpu*sum, 1-0.5*2=0)
    assert abs(scale - 0.64) < 1e-6
    mixed = t.scale_from_goblin_deltas(
        4,
        [1.0, 2.0],
        step_cuts={"1": 0.18, "2": 0.10},
        dpu=0.5,
    )
    assert abs(mixed - 0.72) < 1e-6


def test_mix_by_delta_keeps_goblin_signatures_separate():
    scrapes = [
        {
            "n_legs": 5,
            "n_s": 3,
            "n_g": 2,
            "power_x": 7.0,
            "deltas": [1.0, 1.0],
            "flex_ladder": {5: 4.0, 4: 1.0, 3: 0.4},
        },
        {
            "n_legs": 5,
            "n_s": 3,
            "n_g": 2,
            "power_x": 11.5,
            "deltas": [0.5, 3.0],
            "flex_ladder": {5: 6.0, 4: 1.25, 3: 0.4},
        },
        {
            "n_legs": 5,
            "n_s": 2,
            "n_g": 3,
            "power_x": 7.75,
            "deltas": [1.0, 1.0, 1.0],
            "flex_ladder": {},
        },
    ]
    # Pad so global fit is ready.
    for _ in range(8):
        scrapes.append(
            {
                "n_legs": 2,
                "n_s": 0,
                "n_g": 2,
                "power_x": 1.4,
                "deltas": [1.0, 1.0],
                "flex_ladder": {},
            }
        )
    tables = t.build_tables(scrapes=scrapes, floors={}, min_observed=3, min_family=5, min_fit=8)
    by_key = {
        (r["composition"], r["goblin_delta_sig"]): r for r in tables["mix_by_delta"]
    }
    two_one = by_key[("3S+2G", "1+1")]
    mixed = by_key[("3S+2G", "0.5+3")]
    two_three = by_key[("2S+3G", "1+1+1")]
    assert two_one["power_x"] == 7.0
    assert mixed["power_x"] == 11.5
    assert two_three["power_x"] == 7.75
    # Composition median still exists, but must not be the only view.
    comp = next(r for r in tables["power_grid"] if r["composition"] == "3S+2G")
    assert comp["power_x"] is not None
    assert two_one["power_x"] != mixed["power_x"]
    catalog = tables["mix_catalog"]
    cat_keys = {(r["composition"], r["goblin_delta_sig"]) for r in catalog}
    assert ("3S+2G", "1+1") in cat_keys
    assert ("3S+2G", "0.5+3") in cat_keys
    assert ("2S+3G", "1+1+1") in cat_keys
    assert ("2S+3G", "0.5+1+1") in cat_keys
    assert ("1S+1G", "1") in cat_keys
    assert ("0S+6G", "1+1+1+1+1+1") in cat_keys
    std = next(r for r in catalog if r["composition"] == "5S+0G")
    assert std["status"] == "canonical"
    assert std["power_x"] == 20.0
