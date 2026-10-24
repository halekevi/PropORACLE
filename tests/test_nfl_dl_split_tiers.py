"""Assert DL CSV exposes split pass/run tiers."""

from pathlib import Path

import pandas as pd

from utils.nfl_unit_line_ranks import attach_nfl_unit_line_ranks


def test_dl_ranks_have_pass_and_run_tiers():
    path = Path("Sports/NFL/data/nfl_dl_ranks.csv")
    assert path.is_file()
    df = pd.read_csv(path)
    for col in ("dl_pass_tier", "dl_run_tier", "pass_rush_rank", "run_stop_rank", "tier"):
        assert col in df.columns, col
    # Combined tier can disagree with pass/run (that's the point).
    disagree = df[df["dl_pass_tier"] != df["dl_run_tier"]]
    assert len(disagree) >= 1


def test_attach_exposes_opp_dl_pass_and_run_tiers(tmp_path):
    data = tmp_path / "Sports" / "NFL" / "data"
    data.mkdir(parents=True)
    # Minimal stubs so attach finds tables
    for name, cols in [
        (
            "nfl_dl_ranks.csv",
            {
                "team": ["KC"],
                "dl_rank": [1],
                "pass_rush_rank": [2],
                "run_stop_rank": [20],
                "dl_pass_tier": ["Elite"],
                "dl_run_tier": ["Avg"],
                "tier": ["Above Avg"],
                "sacks_pg": [3.0],
                "yards_before_contact_allowed": [2.0],
            },
        ),
        ("nfl_ol_ranks.csv", {"team": ["BAL"], "ol_rank": [1], "tier": ["Elite"]}),
        ("nfl_pace_ranks.csv", {"team": ["BAL", "KC"], "pace_rank": [1, 2], "tier": ["Fast", "Avg"]}),
        ("nfl_secondary_ranks.csv", {"team": ["KC"], "secondary_rank": [1], "tier": ["Elite"]}),
        ("nfl_box_ranks.csv", {"team": ["KC"], "box_rank": [1], "tier": ["Elite"]}),
    ]:
        pd.DataFrame(cols).to_csv(data / name, index=False)

    slate = pd.DataFrame([{"team": "BAL", "opp_team": "KC", "player": "X"}])
    out = attach_nfl_unit_line_ranks(slate, root=tmp_path)
    assert out.iloc[0]["opp_dl_pass_tier"] == "Elite"
    assert out.iloc[0]["opp_dl_run_tier"] == "Avg"
    assert out.iloc[0]["opp_dl_tier"] == "Above Avg"
