"""Tomorrow tennis ticket card: sport-specific + mixed diversity."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, r"H:\PropORACLE")
from scripts.rank_best_props_today import bucket, load_sport, recs  # noqa: E402

ROOT = Path(r"H:\PropORACLE_main_cp")
TOM = "2026-09-20"


def main() -> None:
    df = load_sport(ROOT, "2026-09-19", "TENNIS", "tennis", "step8_tennis_direction.csv")
    st = pd.to_datetime(df["start_time"], errors="coerce", utc=True).dt.tz_convert(
        "America/New_York"
    )
    df = df.loc[st.dt.strftime("%Y-%m-%d") == TOM].copy()
    _so, _su, gob = bucket(recs(df), "TENNIS")

    print(f"gated goblins: {len(gob)}")
    for r in gob:
        badge = r.get("promo") or r.get("badge")
        print(
            f"{badge:10} {str(r.get('player')):26} {str(r.get('prop')):18} "
            f"O{r.get('line')}  L5 {r.get('l5_over')}/{r.get('l5_under')}  "
            f"L10 {r.get('l10_over')}/{r.get('l10_under')}  "
            f"cov={r.get('cover')}  L5avg={r.get('avg_l5')}  "
            f"D={r.get('def')}  miss={r.get('miss_s')!r}  "
            f"{r.get('matchup')}"
        )


if __name__ == "__main__":
    main()
