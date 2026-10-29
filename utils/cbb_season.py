"""CBB / WCBB season dates for fetch vs grade.

Pipeline fetch resumes earlier (exhibition / early DI). Grading for the
ticket-cell corpus starts on the 11/1 opener. Daily CDP also captures one
CBB N-correct mix per slate from that date so quote history exists before
a cell is wired. Override with ``PROPORACLE_CBB_GRADE_RESUME`` /
``PROPORACLE_CBB_QUOTE_RESUME``.
"""

from __future__ import annotations

import os

CBB_GRADE_RESUME = "2026-11-01"  # ND–Villanova; Florida–Miami is 11/2
CBB_QUOTE_RESUME = "2026-11-01"  # scrape one CBB mix/slate so quotes exist before a cell
CBB_PIPELINE_RESUME = "2026-10-20"


def cbb_grade_enabled(slate_date: str, *, resume: str = CBB_GRADE_RESUME) -> bool:
    day = str(slate_date or "").strip()[:10]
    cut = str(resume or CBB_GRADE_RESUME).strip()[:10]
    if len(day) != 10 or len(cut) != 10:
        return False
    return day >= cut


def cbb_quote_scrape_enabled(slate_date: str, *, resume: str = CBB_QUOTE_RESUME) -> bool:
    """True when the daily CDP job should capture at least one CBB N-correct mix."""
    env = str(os.environ.get("PROPORACLE_CBB_QUOTE_RESUME") or "").strip()[:10]
    cut = env if len(env) == 10 else resume
    return cbb_grade_enabled(slate_date, resume=cut)
