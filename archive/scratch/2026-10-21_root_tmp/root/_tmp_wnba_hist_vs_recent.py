"""Historical live G70 vs 9/18-9/22 for WNBA Goblin OVER Points / Rebounds."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(r"H:\PropORACLE")))
import _tmp_wnba_rates_gated as G  # noqa: E402

CUT = "2026-09-18"
WANT = ("points", "rebounds", "pts+ast", "pts+reb", "pra", "assists", "threes", "fga", "reb+ast")


def bucket(rows):
    h = sum(x["hit"] for x in rows)
    n = len(rows)
    return G.hr(h, n)


def main() -> None:
    rows = [
        r
        for r in G.collect(G.load_prior(), G.load_csv())
        if r["book"] == "Goblin OVER" and r["now"]
    ]
    dates = sorted({r["date"] for r in rows})
    print("catalog dates", dates[0], "->", dates[-1], "n_days", len(dates))
    print(f"{'prop':12} {'hist <9/18 G70':22} {'9/18-9/22 G70':22} {'all G70':22}")
    for prop in WANT:
        rs = [r for r in rows if r["prop"] == prop]
        hist = [r for r in rs if r["date"] < CUT]
        rec = [r for r in rs if r["date"] >= CUT]
        print(f"{prop:12} {bucket(hist):22} {bucket(rec):22} {bucket(rs):22}")
    all_hist = [r for r in rows if r["date"] < CUT]
    all_rec = [r for r in rows if r["date"] >= CUT]
    print(f"{'ALL keep':12} {bucket(all_hist):22} {bucket(all_rec):22} {bucket(rows):22}")


if __name__ == "__main__":
    main()
