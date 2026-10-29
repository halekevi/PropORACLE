from pathlib import Path

from utils.cbb_season import (
    CBB_GRADE_RESUME,
    CBB_QUOTE_RESUME,
    cbb_grade_enabled,
    cbb_quote_scrape_enabled,
)

_REPO = Path(__file__).resolve().parents[1]
_GRADER = _REPO / "scripts" / "run_grader.ps1"


def test_cbb_grade_enabled_from_november_opener():
    assert CBB_GRADE_RESUME == "2026-11-01"
    assert CBB_QUOTE_RESUME == "2026-11-01"
    assert cbb_grade_enabled("2026-10-31") is False
    assert cbb_grade_enabled("2026-11-01") is True
    assert cbb_grade_enabled("2026-11-02") is True
    assert cbb_quote_scrape_enabled("2026-10-31") is False
    assert cbb_quote_scrape_enabled("2026-11-01") is True


def test_run_grader_no_longer_hard_disables_cbb():
    text = _GRADER.read_text(encoding="utf-8")
    assert "$GraderDisabledSports = @('cbb', 'wcbb')" not in text
    assert '$CBB_GRADE_RESUME = "2026-11-01"' in text
    assert "step8_cbb_direction_clean" in text
    assert "cbb_grade paused until" in text or "until $CBB_GRADE_RESUME" in text
