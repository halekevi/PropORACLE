"""Stamp pick-type break-evens onto discovery cells already on disk."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "reports" / "wo4_discovery_flags.json"
GOBLIN = (0.845, 0.769)
STANDARD = (0.577, 0.550)


def stamp(g: dict, combos: int) -> dict:
    book = str(g.get("book") or "")
    be2, be3 = STANDARD if "standard" in book.lower() else GOBLIN
    reasons = []
    if not g.get("oos_split_reliable", True):
        reasons.append("split_balance")
    if int(g.get("oos_n") or 0) == 0:
        reasons.append("holdout_empty")
    if int(g.get("n") or 0) < 30:
        reasons.append("n_under_30")
    if combos >= 500 and int(g.get("n") or 0) < 150:
        reasons.append("search_chance")
    low = g.get("wilson_low")
    if low is not None and float(low) < be3:
        reasons.append("wilson_under_3leg_breakeven")
    return {
        "book": book,
        "prop": g.get("prop"),
        "gate": g.get("gate"),
        "hits": g.get("hits"),
        "n": g.get("n"),
        "hr": g.get("hr"),
        "base_hr": g.get("base_hr"),
        "oos_hr": g.get("oos_hr"),
        "oos_n": g.get("oos_n"),
        "break_even_2leg": be2,
        "break_even_3leg": be3,
        "unreliable": bool(reasons),
        "unreliable_reasons": reasons,
    }


def main() -> None:
    report = {}
    for path in sorted((ROOT / "data" / "reports").glob("discover_gates_*_ranked.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        combos = int(raw.get("combinations_searched") or 0)
        sport = str(raw.get("sport") or path.stem)
        rows = []
        for g in (raw.get("ranked") or [])[:12]:
            if g.get("gate") == "all":
                continue
            rows.append(stamp(g, combos))
        report[sport] = {
            "combinations_searched": combos,
            "n_rows_unique": raw.get("n_rows_unique"),
            "dates": [((raw.get("dates") or [None])[0]), ((raw.get("dates") or [None])[-1])],
            "top": rows,
        }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k: {"combos": v["combinations_searched"], "n": v["n_rows_unique"], "top": v["top"][:3]} for k, v in report.items()}, indent=2)[:6000])


if __name__ == "__main__":
    main()
