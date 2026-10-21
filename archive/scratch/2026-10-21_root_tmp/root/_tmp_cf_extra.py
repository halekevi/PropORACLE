"""Counterfactual extras + cross_section PRA cells."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(r"H:\PropORACLE\data\reports")
cs = json.loads((ROOT / "cross_section_lift.json").read_text(encoding="utf-8"))

want_props = {
    "pra",
    "fga",
    "reb+ast",
    "threes",
    "total_bases",
    "hits+runs+rbis",
    "hitter_ks",
    "points",
}
print("=== prop_cells Goblin OVER ===")
for row in cs.get("prop_cells") or []:
    if str(row.get("book")) != "Goblin OVER":
        continue
    prop = str(row.get("prop") or "")
    sport = str(row.get("sport") or "")
    if prop not in want_props:
        continue
    print()
    print(sport, prop, "open", row.get("open_n"), "@", row.get("open_hr"))
    for k in [
        "L5>=4",
        "L5>=4 + D",
        "L5>=4 + cover",
        "L5>=4 + D + cover",
        "L5>=4 + minutes HIGH",
        "L5>=4 + top3",
        "L5=5",
        "L5=5 + D",
        "L5=5 + cover",
        "L5=5 + D + cover",
    ]:
        cell = row.get(k)
        if not isinstance(cell, dict):
            continue
        n = cell.get("n")
        hr = cell.get("hr")
        if n:
            print(f"  {k:28s} n={n:5} hr={hr}")

print()
print("=== stacks with usage/minutes/pra/fga (Goblin OVER, WNBA/MLB) ===")
for section in ("sport_stacks", "stacks_all", "single_all", "incremental_vs_listed"):
    hits = []
    for row in cs.get(section) or []:
        g = str(row.get("gate") or "").lower()
        if not any(t in g for t in ("pra", "usage", "minutes", "fga", "reb", "three", "top3")):
            continue
        if str(row.get("book")) != "Goblin OVER":
            continue
        if str(row.get("sport")) not in {"WNBA", "MLB", "ALL", "all", ""}:
            continue
        hits.append(row)
    print(section, "n_hits", len(hits))
    for row in sorted(hits, key=lambda r: -(r.get("n") or 0))[:20]:
        print(
            " ",
            row.get("sport"),
            "gate=",
            repr(row.get("gate")),
            "n=",
            row.get("n"),
            "hr=",
            row.get("hr"),
            "lift=",
            row.get("lift"),
        )

# Also check combo search standard_by_sport briefly
combo = json.loads((ROOT / "prop_gate_combo_search.json").read_text(encoding="utf-8"))
print()
print("prop_gate_combo_search: no prop-level keys; books=", list((combo.get("by_book") or {}).keys()))
print("No PRA-off counterfactual in prop_gate_combo_search.json")
print("No pra_off / exclude_pra strings in cross_section_lift.json")
