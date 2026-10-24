"""Find the NHL Shots On Goal store whose rows are all zeros."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOTS = [Path(r"H:\PropORACLE_main_cp") / "outputs", Path(r"H:\PropORACLE") / "outputs"]


def main() -> None:
    files = []
    for root in ROOTS:
        if root.is_dir():
            files.extend(root.glob("**/actuals_nhl_*.csv"))
    print("actuals files", len(files))
    zeros = rows = 0
    for path in files:
        try:
            df = pd.read_csv(path, usecols=lambda c: c in {"prop_type", "actual"})
        except Exception:
            continue
        if "prop_type" not in df.columns:
            continue
        sub = df[df["prop_type"].astype(str).str.lower().isin(["shots on goal", "shots_on_goal"])]
        if sub.empty:
            continue
        actual = pd.to_numeric(sub["actual"], errors="coerce")
        rows += len(sub)
        zeros += int((actual == 0).sum())
    print(f"csv sog rows {rows} zeros {zeros}")


if __name__ == "__main__":
    main()
