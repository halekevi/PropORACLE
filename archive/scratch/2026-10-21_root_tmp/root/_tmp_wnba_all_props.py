"""Every Monday 2026-09-21 WNBA prop hit rate, listed keep and full board, by book."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from rank_best_props_today import _clears_list_gate  # noqa: E402
from utils.prop_norm import canon_prop, display_prop  # noqa: E402

GP = MAIN / "ui_runner" / "templates" / "graded_props_2026-09-21.json"
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER", "Demon OVER", "Demon UNDER")


def num(v):
    try:
        if v in (None, "", "nan"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def cell(rows):
    h = sum(1 for x in rows if x == "HIT")
    m = sum(1 for x in rows if x == "MISS")
    d = h + m
    if not d:
        return None
    return f"{100.0 * h / d:.1f}% ({h}/{d})"


def rec_from(p: dict) -> dict:
    side = str(p.get("direction") or p.get("over_under") or "").strip().upper()
    pt = str(p.get("pick_type") or "").strip()
    low = pt.lower()
    if "goblin" in low:
        pt = "Goblin"
    elif "demon" in low:
        pt = "Demon"
    elif "standard" in low:
        pt = "Standard"
    return {
        "sport": "WNBA",
        "player": p.get("player"),
        "prop": p.get("prop"),
        "side": side if side in {"OVER", "UNDER"} else "",
        "line": num(p.get("line")),
        "pick_type": pt,
        "l5_over": num(p.get("l5_over")),
        "l5_under": num(p.get("l5_under")),
        "result": str(p.get("result") or "").strip().upper(),
    }


def dump(title: str, bag: dict) -> None:
    props = sorted(bag)
    print(f"\n{title}")
    hdr = ["Prop"] + list(BOOKS)
    print(" | ".join(hdr))
    for prop in props:
        cells = [prop]
        any_n = False
        for book in BOOKS:
            t = cell(bag[prop][book])
            if t:
                any_n = True
            cells.append(t or "—")
        if any_n:
            print(" | ".join(cells))


def main() -> None:
    data = json.loads(GP.read_text(encoding="utf-8"))
    props = data["props"] if isinstance(data, dict) and "props" in data else data
    listed = defaultdict(lambda: defaultdict(list))
    board = defaultdict(lambda: defaultdict(list))
    seen = set()
    for p in props:
        if str(p.get("sport") or "").strip().upper() != "WNBA":
            continue
        r = rec_from(p)
        key = (r["player"], r["prop"], r["pick_type"], r["side"], r["line"])
        if key in seen:
            continue
        seen.add(key)
        res = r["result"]
        if res not in {"HIT", "MISS"}:
            continue
        prop = display_prop(canon_prop("WNBA", str(r["prop"] or ""))) or str(r["prop"])
        book = f"{r['pick_type']} {r['side']}"
        board[prop][book].append(res)
        if _clears_list_gate(r):
            listed[prop][book].append(res)
    dump("LISTED KEEP (directional L5>=4) — Monday 2026-09-21 WNBA", listed)
    dump("FULL BOARD (no L5 gate) — Monday 2026-09-21 WNBA", board)


if __name__ == "__main__":
    main()
