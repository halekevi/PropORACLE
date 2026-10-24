"""Weekly score for the provisional gap ranker.

Reads data/reports/gap_ranker_log.jsonl, fills HIT/MISS from graded_props
when those files exist, and scores only slates whose gap top-k differs
from the cell-rate top-k. Gap turns off after 20 of those graded slates
when it loses to the cell-rate order. A tie is not evidence. The off
switch is reviewed again the next month.

    py -3.14 scripts/score_gap_ranker.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from utils.gap_ranker import score_and_maybe_disable  # noqa: E402


def main() -> None:
    lines = score_and_maybe_disable()
    if not lines:
        print("gap_rank | ON | gap n/a cell n/a random n/a | days 0 differing / 0 qualifying | trial")
        return
    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
