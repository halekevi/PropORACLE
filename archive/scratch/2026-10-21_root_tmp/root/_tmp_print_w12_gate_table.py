"""Print W1+W2 unique-game gate table: hits/n = % for base, L5>=4, L5=5, G70."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_prop_gate_ledger import (
    build_prop_gate_ledger,
    dedupe_unique_player_games,
    prepare_decided,
    rate,
)

MAIN = Path(r"H:\PropORACLE_main_cp")
REPO = Path(r"H:\PropORACLE")
DATA = REPO / "Sports" / "NFL" / "data"

DATES = [
    "2026-09-09",
    "2026-09-10",
    "2026-09-13",
    "2026-09-14",
    "2026-09-20",
    "2026-09-21",
]


def load_box(d: str) -> pd.DataFrame | None:
    for root in (MAIN, REPO):
        p = root / "outputs" / d / f"graded_nfl_{d}.xlsx"
        if not p.is_file():
            continue
        df = pd.read_excel(p, sheet_name="Box Raw")
        if df.empty or "result" not in df.columns:
            return None
        decided = df["result"].astype(str).str.upper().isin(["HIT", "MISS", "PUSH"])
        if int(decided.sum()) == 0:
            return None
        out = df.copy()
        out["slate"] = d
        return out
    return None


def cell(hits: int, n: int) -> str:
    if n <= 0:
        return "—"
    return f"{hits}/{n} = {rate(hits, n):.0f}%"


def gate_hits(g: pd.DataFrame) -> dict[str, tuple[int, int]]:
    """Exact hits/n for base, L5>=4, L5=5, G70 on a unique-game group."""
    n = len(g)
    h = int((g["result"] == "HIT").sum())
    g4 = g[g["l5"] >= 4]
    g5 = g[g["l5"] == 5]
    g70 = g[g["gate70"]]
    return {
        "base": (h, n),
        "l5_ge4": (int((g4["result"] == "HIT").sum()), len(g4)),
        "l5_eq5": (int((g5["result"] == "HIT").sum()), len(g5)),
        "g70": (int((g70["result"] == "HIT").sum()), len(g70)),
    }


def print_section(title: str, rows: list[dict]) -> None:
    print(f"\n## {title}")
    print(f"{'Prop':28s}  {'base':16s}  {'L5>=4':16s}  {'L5=5':16s}  {'G70':16s}")
    print("-" * 100)
    for r in rows:
        print(
            f"{r['prop']:28s}  "
            f"{cell(*r['base']):16s}  "
            f"{cell(*r['l5_ge4']):16s}  "
            f"{cell(*r['l5_eq5']):16s}  "
            f"{cell(*r['g70']):16s}"
        )


def main() -> None:
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    print(f"slates={[b['slate'].iloc[0] for b in boxes]}")
    dec = prepare_decided(boxes)
    uniq = dedupe_unique_player_games(dec)
    print(f"decided={len(dec)} unique-game={len(uniq)}")

    sections = [
        ("Standard UNDER", "Standard", "UNDER"),
        ("Standard OVER", "Standard", "OVER"),
        ("Goblin OVER", "Goblin", "OVER"),
    ]

    ledger_rows: list[dict] = []
    for title, pick, side in sections:
        rows: list[dict] = []
        sub = uniq[(uniq["pick"] == pick) & (uniq["side"] == side)]
        for prop, g in sub.groupby("prop"):
            gh = gate_hits(g)
            row = {"prop": prop, **gh}
            rows.append(row)
            ledger_rows.append(
                {
                    "prop": prop,
                    "side": side,
                    "pick_type": pick,
                    "n": gh["base"][1],
                    "hits": gh["base"][0],
                    "hr": rate(*gh["base"]),
                    "l5_ge4_n": gh["l5_ge4"][1],
                    "l5_ge4_hits": gh["l5_ge4"][0],
                    "l5_ge4_hr": rate(*gh["l5_ge4"]),
                    "l5_eq5_n": gh["l5_eq5"][1],
                    "l5_eq5_hits": gh["l5_eq5"][0],
                    "l5_eq5_hr": rate(*gh["l5_eq5"]),
                    "gate70_n": gh["g70"][1],
                    "gate70_hits": gh["g70"][0],
                    "gate70_hr": rate(*gh["g70"]),
                }
            )
        # sort by base n desc
        rows.sort(key=lambda r: r["base"][1], reverse=True)
        print_section(title, rows)

    # Also rebuild canonical ledger CSV (with hits on gate cols)
    ledger = build_prop_gate_ledger(boxes)
    # enrich with exact gate hits from our pass
    enrich = pd.DataFrame(ledger_rows)
    if not enrich.empty and not ledger.empty:
        ledger = ledger.drop(
            columns=[c for c in ledger.columns if c.endswith("_hits")],
            errors="ignore",
        )
        ledger = ledger.merge(
            enrich[
                [
                    "prop",
                    "side",
                    "pick_type",
                    "l5_ge4_hits",
                    "l5_eq5_hits",
                    "gate70_hits",
                ]
            ],
            on=["prop", "side", "pick_type"],
            how="left",
        )
    out = DATA / "nfl_prop_gate_ledger_week1_2.csv"
    ledger.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")

    print(
        "\nRead: cells are HITS/DECIDED unique-game rows. "
        "-- = no rows at that gate. "
        "G70 = L5=5 + L10>=8 + directional D."
    )


if __name__ == "__main__":
    main()
