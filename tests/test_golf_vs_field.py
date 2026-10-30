"""Field average must ignore suspect / short / match-play rounds."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "Sports" / "Golf" / "scripts"))
sys.path.insert(0, str(ROOT))

from step4_attach_player_stats_golf import (  # noqa: E402
    _attach_vs_field,
    _field_eligible_mask,
    repair_round_cache_frame,
)


def test_field_mean_skips_suspect_and_short_rounds():
    df = pd.DataFrame(
        [
            # Clean field: three 70s → mean 70, vs_field 0
            {
                "player_name": "A",
                "player_key": "a",
                "tournament_date": "2026-10-01",
                "tournament_name": "Test Open",
                "round": 1,
                "strokes": 70.0,
                "holes_played": 18.0,
                "pars": 12.0,
                "birdies_or_better": 3.0,
                "bogeys_or_worse": 3.0,
                "to_par": -1.0,
                "course_par": 71.0,
                "attach_suspect": 0,
            },
            {
                "player_name": "B",
                "player_key": "b",
                "tournament_date": "2026-10-01",
                "tournament_name": "Test Open",
                "round": 1,
                "strokes": 70.0,
                "holes_played": 18.0,
                "pars": 12.0,
                "birdies_or_better": 3.0,
                "bogeys_or_worse": 3.0,
                "to_par": -1.0,
                "course_par": 71.0,
                "attach_suspect": 0,
            },
            {
                "player_name": "C",
                "player_key": "c",
                "tournament_date": "2026-10-01",
                "tournament_name": "Test Open",
                "round": 1,
                "strokes": 70.0,
                "holes_played": 18.0,
                "pars": 12.0,
                "birdies_or_better": 3.0,
                "bogeys_or_worse": 3.0,
                "to_par": -1.0,
                "course_par": 71.0,
                "attach_suspect": 0,
            },
            # Would pull the mean down if included
            {
                "player_name": "WD",
                "player_key": "wd",
                "tournament_date": "2026-10-01",
                "tournament_name": "Test Open",
                "round": 1,
                "strokes": 37.0,
                "holes_played": 9.0,
                "pars": 4.0,
                "birdies_or_better": 0.0,
                "bogeys_or_worse": 5.0,
                "to_par": 1.0,
                "course_par": 36.0,
                "attach_suspect": 0,
            },
            {
                "player_name": "Zero",
                "player_key": "zero",
                "tournament_date": "2026-10-01",
                "tournament_name": "Test Open",
                "round": 1,
                "strokes": 0.0,
                "holes_played": 13.0,
                "pars": 0.0,
                "birdies_or_better": 0.0,
                "bogeys_or_worse": 0.0,
                "to_par": 0.0,
                "course_par": 71.0,
                "attach_suspect": 1,
            },
            {
                "player_name": "FiftyTwo",
                "player_key": "fiftytwo",
                "tournament_date": "2026-10-01",
                "tournament_name": "Test Open",
                "round": 1,
                "strokes": 52.0,
                "holes_played": 13.0,
                "pars": 5.0,
                "birdies_or_better": 2.0,
                "bogeys_or_worse": 6.0,
                "to_par": 0.0,
                "course_par": 52.0,
                "attach_suspect": 1,
            },
        ]
    )
    out = _attach_vs_field(df)
    clean = out.loc[_field_eligible_mask(out)]
    assert len(clean) == 3
    assert abs(float(clean["vs_field"].mean())) < 1e-9
    assert float(out.loc[out["player_key"] == "a", "vs_field"].iloc[0]) == 0.0


def test_repair_drops_presidents_cup_and_rekeys():
    df = pd.DataFrame(
        [
            {
                "player_name": "Rasmus Højgaard",
                "player_key": "rasmus hjgaard",
                "tournament_date": "2026-10-01",
                "tournament_name": "Baycurrent Classic",
                "round": 1,
                "strokes": 69.0,
                "holes_played": 18.0,
                "pars": 16.0,
                "birdies": 2.0,
                "birdies_or_better": 2.0,
                "bogeys_or_worse": 0.0,
                "to_par": -2.0,
                "course_par": 71.0,
                "attach_suspect": 1,
                "attach_flags": "pars_out_of_range",
            },
            {
                "player_name": "Scottie Scheffler",
                "player_key": "scottie scheffler",
                "tournament_date": "2026-09-25",
                "tournament_name": "Presidents Cup",
                "round": 1,
                "strokes": 5.0,
                "holes_played": "",
                "pars": "",
                "birdies_or_better": "",
                "bogeys_or_worse": "",
                "to_par": "",
                "course_par": "",
                "attach_suspect": 1,
                "attach_flags": "strokes_out_of_range|missing_holes",
            },
        ]
    )
    out = repair_round_cache_frame(df)
    assert not (out["tournament_name"].astype(str) == "Presidents Cup").any()
    assert out.loc[0, "player_key"] == "rasmus hojgaard"
    assert int(out.loc[0, "attach_suspect"]) == 0
