"""CFB O/D rank views reuse line-secondary z; no second composite; no R."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from utils.cfb_route_participation_gate import (
    PARTICIPATION_HARD_GATES_ENABLED,
    participation_gate_check,
)
from utils.cfb_unit_line_ranks import (
    attach_cfb_unit_line_ranks,
    cfb_week_from_date,
    od_board_authority_note,
    percentile_tier,
    rank_higher_better,
)

_REPO = Path(__file__).resolve().parents[1]
_NEW = [
    _REPO / "Sports" / "CFB" / "scripts" / "build_cfb_unit_line_ranks.py",
    _REPO / "Sports" / "CFB" / "scripts" / "build_cfb_pace_ranks.py",
    _REPO / "Sports" / "CFB" / "scripts" / "build_cfb_route_participation.py",
    _REPO / "utils" / "cfb_unit_line_ranks.py",
    _REPO / "utils" / "cfb_route_participation_gate.py",
]


def test_percentile_tier_bands_n130():
    n = 130
    assert percentile_tier(1, n) == "Elite"
    assert percentile_tier(20, n) == "Elite"  # ceil(0.15*130)=20
    assert percentile_tier(21, n) == "Above"
    assert percentile_tier(39, n) == "Above"  # ceil(0.30*130)=39
    assert percentile_tier(40, n) == "Avg"
    assert percentile_tier(85, n) == "Avg"
    assert percentile_tier(86, n) == "Below"
    assert percentile_tier(111, n) == "Below"  # ceil(0.85*130)=111
    assert percentile_tier(112, n) == "Weak"
    assert percentile_tier(130, n) == "Weak"
    assert percentile_tier(None, n) == ""


def test_week_from_slate_date_sep19_is_week3():
    assert cfb_week_from_date(date(2026, 9, 19)) == 3
    assert cfb_week_from_date(date(2026, 8, 29)) == 0


def test_od_header_names_sep19_as_permanent_hole_on_that_board():
    note = od_board_authority_note(date(2026, 9, 19))
    assert "2026-09-19" in note
    assert "never will" in note
    assert "not a missing-grade bug" in note
    later = od_board_authority_note(date(2026, 9, 26))
    assert "permanent grade hole" in later
    assert "never will" not in later  # that sentence is for the 9/19 board itself


def test_ol_rank_is_sort_of_existing_index_not_new_z():
    s = pd.Series([0.4, -1.2, 1.1], index=["A", "B", "C"])
    ranks = rank_higher_better(s)
    assert int(ranks.loc["C"]) == 1
    assert int(ranks.loc["A"]) == 2
    assert int(ranks.loc["B"]) == 3


def test_box_alias_matches_dl_rank():
    dl = pd.Series([0.2, 0.9, -0.4])
    dl_rank = rank_higher_better(dl)
    box_rank = dl_rank
    assert list(box_rank) == list(dl_rank)


def test_dl_index_and_sacks_can_disagree():
    """havoc.frontSeven ≠ sack rate — Elite DL can be Avg sacks."""
    teams = pd.DataFrame(
        {
            "team": ["AAA", "BBB"],
            "dl_index": [1.8, -0.2],
            "espn_sacks_pg": [1.1, 3.4],
        }
    )
    teams["dl_rank"] = rank_higher_better(teams["dl_index"])
    teams["sacks_rank"] = rank_higher_better(teams["espn_sacks_pg"])
    assert int(teams.loc[0, "dl_rank"]) == 1
    assert int(teams.loc[0, "sacks_rank"]) == 2
    assert int(teams.loc[0, "dl_rank"]) != int(teams.loc[0, "sacks_rank"])


def test_attach_does_not_write_def_tier(tmp_path: Path):
    props = pd.DataFrame(
        {
            "Team": ["OSU"],
            "Opp": ["MICH"],
            "def_tier": ["Avg"],
        }
    )
    out = attach_cfb_unit_line_ranks(props, root=tmp_path)
    assert list(out["def_tier"]) == ["Avg"]
    assert "def_tier" in out.columns


def test_participation_hard_gates_stay_off():
    assert PARTICIPATION_HARD_GATES_ENABLED is False
    ok, reason = participation_gate_check({"part_rec_tgt_share": 0.01})
    assert ok is True
    assert reason == "soft_only"


def test_participation_flag_true_still_does_not_suppress(monkeypatch):
    import utils.cfb_route_participation_gate as g

    monkeypatch.setattr(g, "PARTICIPATION_HARD_GATES_ENABLED", True)
    ok, reason = g.participation_gate_check({"part_rec_tgt_share": 0.01})
    assert ok is True
    assert reason == "flag_on_but_unvalidated_passthrough"
    text = (_REPO / "utils" / "cfb_route_participation_gate.py").read_text(encoding="utf-8")
    assert "graded_props" in text
    assert "inverted" in text.lower()


def test_new_scripts_have_no_r_or_cfbfastr():
    needles = ("cfbfastR", "nflreadr", "subprocess")
    for path in _NEW:
        text = path.read_text(encoding="utf-8")
        for n in needles:
            if n == "subprocess":
                assert n not in text
            else:
                assert n.lower() not in text.lower()
        if path.name == "build_cfb_unit_line_ranks.py":
            assert "index_rows" in text
            assert "OL_FIELDS" not in text
            assert "STAT_DIRECTION" not in text
