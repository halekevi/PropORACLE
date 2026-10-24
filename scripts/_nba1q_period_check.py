"""Compare NBA1Q actuals with full-game actuals on the same player and prop."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE_main_cp") / "outputs"


def _load(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, encoding_errors="replace")
    df["actual"] = pd.to_numeric(df.get("actual"), errors="coerce")
    df["player_key"] = df.get("player", "").astype(str).str.lower().str.strip()
    df["prop_key"] = df.get("prop_type", "").astype(str).str.lower().str.strip()
    return df.dropna(subset=["actual"])


def main() -> None:
    pairs = []
    for qpath in sorted(ROOT.glob("*/actuals_nba1q_*.csv")):
        day = qpath.parent.name
        full = qpath.parent / f"actuals_nba_{day}.csv"
        if not full.is_file():
            continue
        q = _load(qpath)
        f = _load(full)
        merged = q.merge(f, on=["player_key", "prop_key"], suffixes=("_q", "_f"))
        if merged.empty:
            print(day, "no join")
            continue
        same = (merged["actual_q"] - merged["actual_f"]).abs() < 0.51
        both_zero = (merged["actual_q"] == 0) & (merged["actual_f"] == 0)
        positive_equal = same & ~both_zero
        ratio = merged["actual_q"] / merged["actual_f"].where(merged["actual_f"] > 0)
        pairs.append(merged.assign(same=same, both_zero=both_zero, positive_equal=positive_equal, ratio=ratio, day=day))
        print(
            f"{day} joined {len(merged)} both_zero {int(both_zero.sum())} "
            f"positive_equal {int(positive_equal.sum())} ({positive_equal.mean():.1%}) "
            f"median ratio {ratio.median():.2f}"
        )
    if not pairs:
        print("no overlapping NBA / NBA1Q files")
        return
    all_rows = pd.concat(pairs, ignore_index=True)
    both_zero = all_rows["both_zero"]
    positive_equal = all_rows["positive_equal"]
    print(
        f"ALL joined {len(all_rows)} both_zero {int(both_zero.sum())} "
        f"positive_equal {int(positive_equal.sum())} ({positive_equal.mean():.1%}) "
        f"median q/full {all_rows['ratio'].median():.2f}"
    )
    points = all_rows[all_rows["prop_key"].str.contains("point", na=False)]
    if len(points):
        pos = ~((points["actual_q"] == 0) & (points["actual_f"] == 0))
        print(
            f"points n={len(points)} mean q {points['actual_q'].mean():.2f} "
            f"mean full {points['actual_f'].mean():.2f} "
            f"positive_equal {int((points['positive_equal']).sum())} "
            f"of non-both-zero {int(pos.sum())}"
        )


if __name__ == "__main__":
    main()
