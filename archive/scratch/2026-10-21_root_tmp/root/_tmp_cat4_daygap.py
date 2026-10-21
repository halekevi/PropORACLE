"""Bucket cat4 (cross-game same player/team) pairs by day-gap."""
from __future__ import annotations

import itertools
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scripts.build_leg_correlation_matrix import (  # noqa: E402
    CAT_BASELINE_DIFF_GAME,
    categorize,
    load_graded_props,
)

SPORTS = ["Soccer", "Tennis", "NHL"]
BUCKETS = [("<7d", 0, 6), ("7-30d", 7, 30), ("30d+", 31, 10_000)]


def day_gap(a: str, b: str) -> int | None:
    try:
        da = datetime.strptime(str(a)[:10], "%Y-%m-%d")
        db = datetime.strptime(str(b)[:10], "%Y-%m-%d")
    except ValueError:
        return None
    return abs((da - db).days)


def main() -> None:
    df = load_graded_props("ui_runner/templates")
    df = df[df["sport"].isin(SPORTS)].copy()
    print(f"rows={len(df)} sports={SPORTS}")

    # sample pairs like build_cells cat4 (player_id path only — cleaner for form)
    for sport, sub in df.groupby("sport"):
        cells: dict[str, dict] = {
            lab: {"n": 0, "joint": 0, "hits_a": 0, "hits_b": 0} for lab, _, _ in BUCKETS
        }
        n_pairs = 0
        for _pid, g in sub[sub["player_id"] != ""].groupby("player_id"):
            rows = g.to_dict("records")
            if len(rows) > 60:
                rows = rows[:60]
            for a, b in itertools.combinations(rows, 2):
                if a["game_id"] == b["game_id"]:
                    continue
                if categorize(a, b) != CAT_BASELINE_DIFF_GAME:
                    continue
                gap = day_gap(a["date"], b["date"])
                if gap is None:
                    continue
                for lab, lo, hi in BUCKETS:
                    if lo <= gap <= hi:
                        c = cells[lab]
                        c["n"] += 1
                        c["joint"] += int(a["hit"] and b["hit"])
                        c["hits_a"] += int(a["hit"])
                        c["hits_b"] += int(b["hit"])
                        n_pairs += 1
                        break
        print(f"\n=== {sport} cat4 by day-gap (same player_id, n_pairs={n_pairs}) ===")
        for lab, _, _ in BUCKETS:
            c = cells[lab]
            if c["n"] == 0:
                print(f"  {lab:6} n=0")
                continue
            emp = c["joint"] / c["n"]
            pa = c["hits_a"] / c["n"]
            pb = c["hits_b"] / c["n"]
            exp = pa * pb
            lift = emp / exp if exp > 0 else float("nan")
            print(
                f"  {lab:6} n={c['n']:7d} emp_joint={emp:.4f} "
                f"exp={exp:.4f} lift={lift:.3f}"
            )


if __name__ == "__main__":
    main()
