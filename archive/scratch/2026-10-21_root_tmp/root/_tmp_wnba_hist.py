"""Historical WNBA hit rates for Monday-night prop set from the graded catalog."""
from __future__ import annotations

import json
from pathlib import Path

SRC = Path(r"H:\PropORACLE\data\reports\all_ranked_prop_hit_rates.json")
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
# Monday 9/21 decided props (listed + board)
PROPS = {
    "points",
    "points_combo",
    "pts+reb",
    "pts+ast",
    "pra",
    "rebounds",
    "reb+ast",
    "assists",
    "threes",
    "threes_att",
    "threes_combo",
    "fga",
    "fgm",
    "fta",
    "ftm",
    "oreb",
    "dreb",
    "stocks",
    "turnovers",
}


def cell(hits, n):
    if not n:
        return "—"
    return f"{100.0 * hits / n:.1f}% ({hits}/{n})"


def main() -> None:
    data = json.loads(SRC.read_text(encoding="utf-8"))
    print(f"window {data['from']} → {data['to']}  days={data['n_days']}  WNBA n={data['by_sport']['WNBA']['n']}")
    rows = [
        r
        for r in data["rows"]
        if r.get("sport") == "WNBA" and r.get("book") in BOOKS and r.get("prop") in PROPS
    ]
    for book in BOOKS:
        print(f"\n{book}")
        print("Prop | Ungated | Listed L5>=4 | L5=5 | L10>=8 | L5=5+L10>=8 | L5=5+L10>=8+D | D")
        br = [r for r in rows if r["book"] == book]
        br.sort(key=lambda r: -(r.get("n") or 0))
        for r in br:
            print(
                " | ".join(
                    [
                        r.get("prop_display") or r["prop"],
                        cell(r["hits"], r["n"]),
                        cell(r.get("listed_hits") or 0, r.get("listed_n") or 0),
                        cell(r.get("l5eq5_hits") or 0, r.get("l5eq5_n") or 0),
                        cell(r.get("l10ge8_hits") or 0, r.get("l10ge8_n") or 0),
                        cell(r.get("l5eq5_l10ge8_hits") or 0, r.get("l5eq5_l10ge8_n") or 0),
                        cell(r.get("l5eq5_l10ge8_d_hits") or 0, r.get("l5eq5_l10ge8_d_n") or 0),
                        cell(r.get("d_hits") or 0, r.get("d_n") or 0),
                    ]
                )
            )


if __name__ == "__main__":
    main()
