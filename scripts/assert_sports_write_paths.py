#!/usr/bin/env python3
"""Assert Sports/ live writer paths still exist after safe cleanup.

Classify by writer contract (docs/REPO_LAYOUT.md), not by folder heuristics.
Exit 0 when required NFL intermediate outputs are present; print a checklist
for other sports' step8 mirrors / caches.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

NFL_REQUIRED = [
    ROOT / "Sports/NFL/data/outputs/step2_clean_props.csv",
    ROOT / "Sports/NFL/data/outputs/step3_nfl_with_defense.csv",
    ROOT / "Sports/NFL/data/outputs/step5_nfl_with_stats.csv",
]

NFL_OPTIONAL = [
    ROOT / "Sports/NFL/data/outputs/step6_hit_rates.csv",
    ROOT / "Sports/NFL/outputs/step8_nfl_direction_clean.xlsx",
    ROOT / "Sports/NFL/outputs/step7_nfl_ranked.xlsx",
]

MIRRORS = [
    ROOT / "Sports/MLB/outputs/step8_mlb_direction_clean.xlsx",
    ROOT / "Sports/MLB/step8_mlb_direction_clean.xlsx",
    ROOT / "Sports/Soccer/outputs/step8_soccer_direction_clean.xlsx",
    ROOT / "Sports/Tennis/outputs/step8_tennis_direction_clean.xlsx",
]


def _fmt(p: Path) -> str:
    if not p.exists():
        return f"MISSING  {p.relative_to(ROOT)}"
    st = p.stat()
    return f"ok       {p.relative_to(ROOT)}  bytes={st.st_size}"


def main() -> int:
    print("=== NFL required intermediates (Sports writers) ===")
    missing = []
    for p in NFL_REQUIRED:
        line = _fmt(p)
        print(line)
        if line.startswith("MISSING"):
            missing.append(p)
    print("=== NFL optional ===")
    for p in NFL_OPTIONAL:
        print(_fmt(p))
    print("=== Other-sport step8 mirrors (informational) ===")
    for p in MIRRORS:
        print(_fmt(p))
    if missing:
        print(f"FAIL: {len(missing)} required NFL path(s) missing", file=sys.stderr)
        return 1
    print("PASS: NFL required Sports/ writer paths present")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
