"""Count NHL Shots On Goal zeros in graded_props files."""
from __future__ import annotations

import json
from pathlib import Path

ROOTS = [
    Path(r"H:\PropORACLE_main_cp") / "ui_runner" / "templates",
    Path(r"H:\PropORACLE_main_cp") / "mobile" / "www",
    Path(r"H:\PropORACLE") / "mobile" / "www",
]


def main() -> None:
    files = []
    for root in ROOTS:
        if root.is_dir():
            files.extend(root.glob("graded_props_*.json"))
            files.extend(root.glob("*/graded_props_*.json"))
    seen = set()
    rows = zeros = decided = 0
    for path in files:
        key = path.name
        if key in seen:
            continue
        seen.add(key)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            if str(r.get("sport") or "").upper() != "NHL":
                continue
            prop = str(r.get("prop") or "").lower().replace("_", " ")
            if "shot" not in prop or "goal" not in prop:
                continue
            rows += 1
            try:
                actual = float(r.get("actual_value"))
            except (TypeError, ValueError):
                continue
            if actual == 0:
                zeros += 1
            if str(r.get("result") or "").upper() in {"HIT", "MISS"}:
                decided += 1
    print(f"files {len(seen)} sog rows {rows} zeros {zeros} decided {decided}")


if __name__ == "__main__":
    main()
