#!/usr/bin/env python3
"""Hit rates by signed PrizePicks vs Vegas cover (OVER: vegas−PP, UNDER: PP−vegas).

Reads graded_props_*.json. Uses line_vegas on the row when present, else the
rebuilt step8 snapshot in data/cache/vegas_props/<date>.csv. Period slates skipped.

  py -3.14 scripts/pp_vegas_gap_hit_rates.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))

from utils.pp_vegas_gap import (  # noqa: E402
    allows_vegas_join,
    build_vegas_index,
    find_vegas_csv,
    gap_bucket,
    is_grading_suspect,
    nearest_vegas_line,
    signed_cover,
)

OUT = _REPO / "data" / "reports" / "pp_vegas_gap_hit_rates.json"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]


def pick_book(pick_type: str, side: str) -> str:
    pt = str(pick_type or "").strip().lower()
    if pt == "goblin":
        return "Goblin OVER" if side == "OVER" else ""
    if pt == "demon":
        return "Demon OVER" if side == "OVER" else ""
    if pt in ("standard", "std", "—", "-", ""):
        return f"Standard {side}"
    return ""


def empty() -> dict[str, int]:
    return {"n": 0, "hits": 0}


def pct(h: int, n: int) -> float | None:
    return round(100.0 * h / n, 1) if n else None


def _load_index(date_str: str, cache: dict[str, dict]) -> dict:
    if date_str in cache:
        return cache[date_str]
    path = find_vegas_csv(date_str, repos=(_REPO, MAIN))
    if not path:
        cache[date_str] = {}
        return cache[date_str]
    import csv

    with path.open(encoding="utf-8-sig", newline="") as f:
        cache[date_str] = build_vegas_index(list(csv.DictReader(f)))
    return cache[date_str]


def _vegas_for_row(r: dict, date_str: str, indexes: dict[str, dict]) -> float | None:
    try:
        vg = float(r.get("line_vegas"))
        return vg
    except (TypeError, ValueError):
        pass
    sport = str(r.get("sport") or "").strip()
    if not allows_vegas_join(sport):
        return None
    try:
        pp = float(r.get("line"))
    except (TypeError, ValueError):
        return None
    idx = _load_index(date_str, indexes)
    if not idx:
        return None
    return nearest_vegas_line(
        idx,
        sport=sport,
        player=str(r.get("player") or ""),
        prop=str(r.get("prop") or r.get("prop_type") or ""),
        pp_line=pp,
    )


def main() -> None:
    cells: dict[tuple[str, str], dict[str, int]] = defaultdict(empty)
    books: dict[str, dict[str, int]] = defaultdict(empty)
    n_raw = 0
    n_with_vegas = 0
    indexes: dict[str, dict] = {}
    seen: set[Path] = set()
    for d in GRADED_DIRS:
        if not d.is_dir():
            continue
        for path in sorted(d.glob("graded_props_*.json")):
            key = path.resolve()
            if key in seen:
                continue
            seen.add(key)
            date = path.stem.replace("graded_props_", "")[:10]
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            props = raw.get("props") or raw.get("rows") or []
            if not isinstance(props, list):
                continue
            for r in props:
                if not isinstance(r, dict):
                    continue
                n_raw += 1
                if is_grading_suspect(r):
                    continue
                res = str(r.get("result") or "").strip().upper()
                if res not in ("HIT", "MISS"):
                    continue
                side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
                if side in ("O", "OVER"):
                    side = "OVER"
                elif side in ("U", "UNDER"):
                    side = "UNDER"
                else:
                    continue
                book = pick_book(str(r.get("pick_type") or ""), side)
                if not book:
                    continue
                try:
                    pp = float(r.get("line"))
                except (TypeError, ValueError):
                    continue
                vg = _vegas_for_row(r, date, indexes)
                if vg is None:
                    continue
                try:
                    gap = signed_cover(pp, vg, side)
                except (TypeError, ValueError):
                    continue
                n_with_vegas += 1
                hit = 1 if res == "HIT" else 0
                bkt = gap_bucket(gap)
                cells[(book, bkt)]["n"] += 1
                cells[(book, bkt)]["hits"] += hit
                books[book]["n"] += 1
                books[book]["hits"] += hit

    rows: list[dict[str, Any]] = []
    for (book, bkt), c in sorted(cells.items()):
        rows.append(
            {
                "book": book,
                "gap_bucket": bkt,
                "n": c["n"],
                "hits": c["hits"],
                "hit_pct": pct(c["hits"], c["n"]),
            }
        )
    payload = {
        "n_raw": n_raw,
        "n_with_vegas": n_with_vegas,
        "note": (
            "Signed cover: OVER vegas−PP (positive = cheaper over); "
            "UNDER PP−vegas (positive = cheaper under). Joins line_vegas on the grade "
            "or rebuilt step8 snapshots in data/cache/vegas_props/."
        ),
        "books": {
            k: {"n": v["n"], "hits": v["hits"], "hit_pct": pct(v["hits"], v["n"])}
            for k, v in sorted(books.items())
        },
        "rows": rows,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"raw={n_raw} with_vegas={n_with_vegas} -> {OUT}")
    if not n_with_vegas:
        print("no Vegas matches — run scripts/rebuild_vegas_from_slates.py then re-run this report")
        return
    for r in rows:
        print(f"  {r['book']:16} {r['gap_bucket']:12} {r['hit_pct']}%  n={r['n']}")


if __name__ == "__main__":
    main()
