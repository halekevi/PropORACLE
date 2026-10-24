"""Pull soccer shots cells and confirm graded L5 fields."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    ranked = json.loads(
        (ROOT / "data" / "reports" / "discover_gates_soccer_ranked.json").read_text(encoding="utf-8")
    )
    hits = []
    for g in ranked.get("ranked") or []:
        if g.get("prop") != "shots":
            continue
        gate = str(g.get("gate") or "")
        if "L5" not in gate and "L10" not in gate:
            continue
        if g.get("book") != "Goblin OVER":
            continue
        hits.append(
            {
                "gate": gate,
                "hr": g.get("hr"),
                "n": g.get("n"),
                "hits": g.get("hits"),
                "base": g.get("base_hr"),
                "be2": g.get("break_even_2leg"),
                "be3": g.get("break_even_3leg"),
                "split": g.get("unreliable_reasons"),
                "oos_n": g.get("oos_n"),
            }
        )
    hits.sort(key=lambda r: -(r["n"] or 0))
    print("shots gates", len(hits))
    for row in hits[:12]:
        print(row)
    # one graded soccer row keys
    folder = ROOT / "ui_runner" / "templates"
    sample = None
    for path in sorted(folder.glob("graded_props_2026-09*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if str(row.get("sport") or "") == "Soccer" and "shot" in str(row.get("prop") or "").lower():
                sample = {k: row.get(k) for k in list(row)[:40]}
                sample["_file"] = path.name
                break
        if sample:
            break
    print("sample keys", sorted(sample) if sample else None)
    if sample:
        print("l5", sample.get("l5_over"), "l10", sample.get("l10_over"), "minutes", sample.get("minutes_status"), "result", sample.get("result"))


if __name__ == "__main__":
    main()
