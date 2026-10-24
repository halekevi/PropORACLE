"""Scoring-vehicle labels from team yard/TD mix."""

from utils.nfl_scoring_vehicles import _classify
import pandas as pd


def test_classify_run_pass_fg():
    run = pd.Series({"rush_share": 0.45, "pass_share": 0.55, "rush_td_share": 0.5, "fg_pg": 1.5, "kick_score_share": 0.3})
    assert _classify(run) == "RUN"
    pas = pd.Series({"rush_share": 0.27, "pass_share": 0.73, "rush_td_share": 0.2, "fg_pg": 2.2, "kick_score_share": 0.4})
    assert "PASS" in _classify(pas) and "FG" in _classify(pas)
    bad = pd.Series({"rush_share": 0.34, "pass_share": 0.66, "rush_td_share": 0.3, "fg_pg": 1.4, "kick_score_share": 0.44})
    assert _classify(bad) == "BALANCED"
