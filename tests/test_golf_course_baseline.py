"""Course-baseline projection: Crowe fixture + walk-forward no-leak."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from utils.golf_course_baseline import (
    apply_course_baseline,
    project_leg,
    project_strokes,
    walk_forward_strokes,
)


def test_crowe_strokes_mean_near_par_course():
    # Spec: to_par −5.4 / vs_field −2.2 at par 71 with field to-par ≈ 0 → µ ≈ 68.8
    got = project_strokes(B=71.0, S=-2.2, sd=2.8, line=68.5)
    assert abs(got["golf_proj_mu"] - 68.8) < 0.05
    assert got["golf_proj_p_over"] > 0.5
    assert got["golf_proj_p_push"] == 0.0


def _prior_rounds(n: int = 20, vs_field: float = -2.2) -> list[dict]:
    rows = []
    start = date(2026, 9, 1)
    for i in range(n):
        d = start + timedelta(days=i)
        rows.append(
            {
                "player_key": "crowe",
                "player_name": "Trace Crowe",
                "tournament_name": "Near Par Open",
                "course_id": "near_par",
                "event_id": f"e{i}",
                "round_date": d.isoformat(),
                "tournament_date": d.isoformat(),
                "round": 1,
                "strokes": 68.8,
                "to_par": -2.2,
                "vs_field": vs_field,
                "course_par": 71,
                "holes_played": 18,
                "pars": 11,
                "birdies_or_better": 5,
            }
        )
    return rows


def test_walk_forward_excludes_same_day_and_later():
    prior = _prior_rounds(20)
    as_of = "2026-10-10"
    leak = {
        "player_key": "crowe",
        "player_name": "Trace Crowe",
        "tournament_name": "Near Par Open",
        "course_id": "near_par",
        "event_id": "leak",
        "round_date": as_of,
        "tournament_date": as_of,
        "round": 1,
        "strokes": 90.0,
        "to_par": 19.0,
        "vs_field": 20.0,
        "course_par": 71,
        "holes_played": 18,
        "pars": 4,
        "birdies_or_better": 0,
    }
    cache = pd.DataFrame(prior + [leak])
    clean = project_leg(
        cache,
        player_key="crowe",
        prop_type="Strokes",
        line=68.5,
        course_id="near_par",
        as_of=as_of,
    )
    leaked = project_leg(
        pd.DataFrame(prior + [{**leak, "round_date": "2026-10-09", "tournament_date": "2026-10-09"}]),
        player_key="crowe",
        prop_type="Strokes",
        line=68.5,
        course_id="near_par",
        as_of=as_of,
    )
    assert clean["golf_proj_mu"] != leaked["golf_proj_mu"]
    # Same-day 90 cannot pull µ up toward the leak.
    assert float(clean["golf_proj_mu"]) < float(leaked["golf_proj_mu"])


def test_walk_forward_strokes_never_uses_own_round():
    rows = _prior_rounds(12)
    cache = pd.DataFrame(rows)
    out = walk_forward_strokes(cache, min_prior=8)
    assert not out.empty
    # Last projected date must be after the first 8 rounds.
    assert str(out["round_date"].min()) >= "2026-09-09"


def test_apply_course_baseline_observe_only_columns():
    props = pd.DataFrame(
        {
            "player": ["Trace Crowe"],
            "player_key": ["crowe"],
            "prop_type": ["Strokes"],
            "line": [68.5],
            "course_id": ["near_par"],
            "start_time": ["2026-10-10"],
        }
    )
    cache = pd.DataFrame(_prior_rounds(20))
    out = apply_course_baseline(props, cache, as_of="2026-10-10")
    assert "golf_proj_mu" in out.columns
    assert "golf_proj_hold" in out.columns
    assert pd.notna(out.loc[0, "golf_proj_mu"])
