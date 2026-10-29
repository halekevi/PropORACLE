#!/usr/bin/env python3
"""Walk-forward golf strokes projection over the round cache (observe-only).

Does not ticket. Use this over the Tour break to check µ/σ calibration
before a keep-gate scan.

B is earlier *events* at the course, not this week's field. Report priced vs
held, R1/R2 vs R3/R4, and tail coverage (|z|<1 ~0.68, |z|<2 ~0.95). If z>2
clearly exceeds z<-2, the high-score tail is underpriced — do not scan Unders.

  py -3.14 scripts/golf_proj_walkforward.py
  py -3.14 scripts/golf_proj_walkforward.py --cache Sports/Golf/cache/golf_round_cache.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.golf_course_baseline import (  # noqa: E402
    print_walkforward_summary,
    walk_forward_strokes,
)

DEFAULT_CACHE = _REPO / "Sports" / "Golf" / "cache" / "golf_round_cache.csv"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=str(DEFAULT_CACHE))
    ap.add_argument("--min-prior", type=int, default=0, help="Drop rows with skill_n below this (0 keeps held)")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    path = Path(args.cache)
    if not path.is_absolute():
        path = _REPO / path
    if not path.is_file():
        print(f"[golf-proj] no cache at {path}")
        sys.exit(1)
    cache = pd.read_csv(path, low_memory=False, encoding="utf-8-sig")
    out = walk_forward_strokes(cache, min_prior=int(args.min_prior))
    print_walkforward_summary(out)
    if out.empty:
        return
    dest = Path(args.out) if str(args.out).strip() else (_REPO / "data" / "reports" / "golf_proj_walkforward.csv")
    if not dest.is_absolute():
        dest = _REPO / dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dest, index=False, encoding="utf-8-sig")
    print(f"[golf-proj] wrote {dest}")


if __name__ == "__main__":
    main()
