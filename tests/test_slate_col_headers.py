"""Slate Excel column keys must stay aligned with headers."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from combined_slate_tickets import (  # noqa: E402
    SLATE_COLS,
    SLATE_HDRS,
    SLATE_WIDTHS,
    _slate_hdr_for,
)


def test_slate_cols_hdrs_widths_same_length():
    assert len(SLATE_COLS) == len(SLATE_HDRS) == len(SLATE_WIDTHS)


def test_alt_book_line_headers():
    assert _slate_hdr_for("line_underdog") == "Line (UD)"
    assert _slate_hdr_for("line_draftkings") == "Line (DK)"
    assert _slate_hdr_for("line_vegas") == "Line (LV)"
    assert "line_vegas" in SLATE_COLS
    idx = SLATE_COLS.index("line_vegas")
    assert SLATE_HDRS[idx] == "Line LV"


def test_l10_streak_is_not_labeled_def_tier():
    assert _slate_hdr_for("l10_streak") == "L10 Streak"
    assert _slate_hdr_for("def_tier") == "Def Tier"
    assert _slate_hdr_for("min_tier") == "Min Tier"
    assert _slate_hdr_for("shot_role") == "Shot Role"
    assert _slate_hdr_for("usage_role") == "Usage Role"
    assert _slate_hdr_for("h2h_avg") == "H2H Avg"
    assert _slate_hdr_for("b2b_flag") == "B2B"
    assert _slate_hdr_for("cv_pct") == "CV%"
    assert _slate_hdr_for("game_time") == "Game Time"
