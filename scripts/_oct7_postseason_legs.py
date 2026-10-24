"""Oct 7 MLB and WNBA Goblin legs against the postseason ticket block."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.postseason_slate import is_postseason_row, postseason_blocks_ticket  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

MAIN = ROOT.parent / "PropORACLE_main_cp"


def _load(sport: str) -> pd.DataFrame:
    for base in (MAIN, ROOT):
        path = base / "outputs" / "2026-10-07" / sport / f"step8_{sport}_direction.csv"
        if path.is_file():
            return pd.read_csv(path, dtype=str, encoding_errors="replace")
    return pd.DataFrame()


def main() -> None:
    for sport in ("mlb", "wnba"):
        df = _load(sport)
        print(f"{sport} rows {len(df)}")
        if df.empty:
            continue
        gob = df[df["pick_type"].astype(str).str.contains("Goblin", case=False, na=False)]
        print(f"  goblin rows {len(gob)}")
        shown = 0
        blocked = 0
        for _, r in gob.iterrows():
            row = r.to_dict()
            row["sport"] = sport.upper()
            row["prop"] = row.get("prop") or row.get("prop_type")
            row["side"] = row.get("side") or row.get("direction")
            if not is_postseason_row(row):
                continue
            blocked += 1
            if shown >= 12:
                continue
            shown += 1
            print(
                f"  BLOCK {row.get('player')} {row.get('prop_type') or row.get('prop')} "
                f"line={row.get('line')} start={row.get('start_time')} "
                f"g70={goblin_70_eligible(row)}"
            )
        print(f"  postseason goblin rows {blocked}")
        print(f"  sample block flag {postseason_blocks_ticket({'sport': sport.upper(), 'start_time': '2026-10-07T23:00:00'})}")


if __name__ == "__main__":
    main()
