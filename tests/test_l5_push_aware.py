"""Shared push-aware L5 across sports."""

from __future__ import annotations

import pandas as pd

from utils.l5_push_aware import (
    attach_l5_push_columns,
    count_vs_line,
    is_clean_l5_five,
)


def test_jaren_style_integer_rebounds():
    # 6-rebound line: 8,6,7,6,9 → 3 over, 2 push
    c = count_vs_line([8, 6, 7, 6, 9], 6.0)
    assert c.over == 3
    assert c.push == 2
    assert c.decided == 3
    assert c.under == 0
    assert not is_clean_l5_five(c.over, c.decided, c.push)


def test_clean_five_requires_no_push():
    assert is_clean_l5_five(5, 5, 0)
    assert not is_clean_l5_five(5, 4, 1)
    assert not is_clean_l5_five(4, 5, 0)


def test_attach_columns_on_frame():
    df = pd.DataFrame(
        {
            "line": [11.0, 68.5],
            "stat_g1": [11, 66],
            "stat_g2": [12, 64],
            "stat_g3": [11, 65],
            "stat_g4": [11, 66],
            "stat_g5": [11, 67],
        }
    )
    out = attach_l5_push_columns(df)
    assert out.loc[0, "l5_over"] == 1
    assert out.loc[0, "l5_push"] == 4
    assert out.loc[0, "l5_decided"] == 1
    assert out.loc[0, "thin_decided"] == 1
    assert out.loc[1, "l5_under"] == 5
    assert out.loc[1, "l5_push"] == 0
    assert out.loc[1, "thin_decided"] == 0
