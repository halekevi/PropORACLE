"""Oct 7 Bonner and Canada legs, and whether the postseason flag blocks them."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT)]
from utils.postseason_slate import is_postseason_row  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

path = ROOT.parent / "PropORACLE_main_cp" / "outputs" / "2026-10-07" / "wnba" / "step8_wnba_direction.csv"
df = pd.read_csv(path, dtype=str, encoding_errors="replace")
mask = df["player"].str.contains("Bonner|Canada", case=False, na=False)
cols = [c for c in ("player", "prop_type", "pick_type", "line", "side", "start_time", "team", "opp_team") if c in df.columns]
print(df.loc[mask, cols].to_string(index=False))
print("---")
for _, r in df.loc[mask].iterrows():
    row = r.to_dict()
    row["sport"] = "WNBA"
    row["prop"] = row.get("prop") or row.get("prop_type")
    row["side"] = row.get("side") or ""
    print(
        row.get("player"),
        row.get("prop_type"),
        row.get("pick_type"),
        "postseason",
        is_postseason_row(row),
        "g70",
        goblin_70_eligible(row),
    )
