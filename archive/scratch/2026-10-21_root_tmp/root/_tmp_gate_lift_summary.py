#!/usr/bin/env python3
"""Summarize NFL/CFB discover_gates ticket clears for this session."""
from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def yn(v: object) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y")


def best_per_cell(rows: list[dict], *, ticket_only: bool = True) -> list[tuple]:
    by: dict[tuple, list] = defaultdict(list)
    for r in rows:
        if ticket_only and not yn(r.get("ticket_clear")):
            continue
        by[(r["book"], r["prop"])].append(r)
    out = []
    for k, rs in by.items():
        best = max(rs, key=lambda z: (float(z["hr"]), int(float(z["n"]))))
        out.append((k, best))
    out.sort(key=lambda x: -float(x[1]["hr"]))
    return out


def main() -> None:
    nr = list(csv.DictReader((ROOT / "data/reports/discover_gates_nfl_ranked.csv").open(encoding="utf-8")))
    print("=== NFL ticket_clear best-per-cell ===")
    for (book, prop), best in best_per_cell(nr):
        print(
            f"{float(best['hr']):5.1%} n={best['n']:>3} "
            f"oos={best['oos_hr']}/{best['oos_n']} "
            f"split={best['oos_split_reliable']} oos_clear={best['oos_ticket_clear']} "
            f"| {book} {prop} | {best['gate']} base={float(best['base_hr']):.1%}"
        )

    print("\n=== NFL non-sacks ticket_clear ===")
    for (book, prop), best in best_per_cell(nr):
        if prop == "sacks":
            continue
        print(
            f"{float(best['hr']):5.1%} n={best['n']:>3} "
            f"oos={best['oos_hr']}/{best['oos_n']} split={best['oos_split_reliable']} "
            f"| {book} {prop} | {best['gate']} lift={float(best['lift_vs_base']):+.1%}"
        )

    ex = list(csv.DictReader((ROOT / "data/reports/discover_gates_nfl_export.csv").open(encoding="utf-8")))
    print("\nNFL export books:", Counter(r.get("book", "") for r in ex))
    print("NFL Goblin rows:", sum(1 for r in ex if "Goblin" in str(r.get("book", ""))))
    print("NFL unique props:", len({r.get("prop", "") for r in ex}))

    # Any near-misses n>=20 HR>=65 not ticket_clear
    print("\n=== NFL near-misses (HR>=65 n>=20, not ticket_clear) ===")
    seen = set()
    for r in sorted(nr, key=lambda z: -float(z["hr"])):
        if yn(r.get("ticket_clear")):
            continue
        if float(r["hr"]) < 0.65 or int(float(r["n"])) < 20:
            continue
        key = (r["book"], r["prop"], r["gate"])
        if key in seen:
            continue
        seen.add(key)
        if len(seen) > 25:
            break
        print(
            f"{float(r['hr']):5.1%} n={r['n']:>3} "
            f"| {r['book']} {r['prop']} | {r['gate']} base={float(r['base_hr']):.1%}"
        )

    cr = list(csv.DictReader((ROOT / "data/reports/discover_gates_cfb_ranked.csv").open(encoding="utf-8")))
    print("\n=== CFB ticket_clear best-per-cell ===")
    for (book, prop), best in best_per_cell(cr):
        print(
            f"{float(best['hr']):5.1%} n={best['n']:>3} "
            f"oos={best.get('oos_hr')}/{best.get('oos_n')} "
            f"split={best.get('oos_split_reliable')} oos_clear={best.get('oos_ticket_clear')} "
            f"| {book} {prop} | {best['gate']} base={float(best['base_hr']):.1%}"
        )

    print("\n=== CFB clean OOS (oos_ticket_clear + split_reliable, no cover/cush) ===")
    for r in cr:
        if not yn(r.get("oos_ticket_clear")):
            continue
        if not yn(r.get("oos_split_reliable", "True")):
            continue
        g = r["gate"]
        if "cover" in g or "cush" in g.lower() or "season" in g.lower():
            continue
        print(
            f"{float(r['hr']):5.1%} n={r['n']:>3} oos={r.get('oos_hr')}/{r.get('oos_n')} "
            f"| {r['book']} {r['prop']} | {r['gate']}"
        )

    # CFB high HR with unreliable split that look like lift
    print("\n=== CFB high clears with split UNRELIABLE (do not trust OOS) ===")
    seen = set()
    for r in sorted(cr, key=lambda z: -float(z["hr"])):
        if float(r["hr"]) < 0.70 or int(float(r["n"])) < 25:
            continue
        if yn(r.get("oos_split_reliable", "True")):
            continue
        key = (r["book"], r["prop"])
        if key in seen:
            continue
        seen.add(key)
        print(
            f"{float(r['hr']):5.1%} n={r['n']:>3} oos={r.get('oos_hr')}/{r.get('oos_n')} "
            f"| {r['book']} {r['prop']} | {r['gate']}  [split unreliable]"
        )


if __name__ == "__main__":
    main()
