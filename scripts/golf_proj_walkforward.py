#!/usr/bin/env python3
"""Walk-forward golf strokes projection over the round cache (observe-only).

Does not ticket. Use this over the Tour break to check µ/σ calibration
before a keep-gate scan.

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

from utils.golf_course_baseline import walk_forward_strokes  # noqa: E402

DEFAULT_CACHE = _REPO / "Sports" / "Golf" / "cache" / "golf_round_cache.csv"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=str(DEFAULT_CACHE))
    ap.add_argument("--min-prior", type=int, default=8)
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
    if out.empty:
        print("[golf-proj] 0 walk-forward rows")
        return
    z = pd.to_numeric(out.get("z"), errors="coerce")
    print(
        f"[golf-proj] n={len(out)} mean_z={float(z.mean()):.3f} "
        f"sd_z={float(z.std(ddof=1)):.3f} "
        f"hold={int(pd.to_numeric(out.get('golf_proj_hold'), errors='coerce').fillna(0).sum())}"
    )
    dest = Path(args.out) if str(args.out).strip() else (_REPO / "data" / "reports" / "golf_proj_walkforward.csv")
    if not dest.is_absolute():
        dest = _REPO / dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dest, index=False, encoding="utf-8-sig")
    print(f"[golf-proj] wrote {dest}")


if __name__ == "__main__":
    main()
