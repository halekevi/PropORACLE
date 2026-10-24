import json
from pathlib import Path


def top(path: str, label: str) -> None:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    cells = raw.get("cells") or []
    print("==", label)
    print("dates", raw.get("dates"))
    print("unique", raw.get("n_rows_unique"))
    print("n_gates", raw.get("n_gates"), "cells", len(cells), "combos", (raw.get("n_gates") or 0) * len(cells))
    base = {(c.get("book"), c.get("prop")): c.get("base_hr") for c in cells}
    n = 0
    for g in raw.get("ranked") or []:
        if g.get("gate") == "all":
            continue
        n += 1
        if n > 5:
            break
        key = (g.get("book"), g.get("prop"))
        reasons = ",".join(g.get("unreliable_reasons") or []) or "ok"
        print(
            g.get("book"),
            g.get("prop"),
            g.get("gate"),
            "hr",
            g.get("hr"),
            "n",
            g.get("n"),
            "base",
            base.get(key),
            "be2",
            g.get("break_even_2leg"),
            "be3",
            g.get("break_even_3leg"),
            "split_ok",
            g.get("oos_split_reliable"),
            reasons,
        )


if __name__ == "__main__":
    top("data/reports/discover_gates_cfb_ranked.json", "CFB")
    top("data/reports/discover_gates_nfl_ranked.json", "NFL")
