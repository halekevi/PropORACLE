"""CFB line-level Goblin / Standard hit rates from graded_props JSON."""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

REPO = Path(r"H:/PropORACLE")
DIRS = [
    REPO / "ui_runner" / "templates",
    REPO.parent / "PropORACLE_main_cp" / "ui_runner" / "templates",
]


def book_of(row: dict) -> str:
    pt = str(row.get("Pick Type") or row.get("pick_type") or row.get("pickType") or "").strip().lower()
    side = str(row.get("Direction") or row.get("side") or row.get("bet_direction") or "").strip().upper()
    if "goblin" in pt:
        return "Goblin OVER" if side in ("", "OVER") else f"Goblin {side}"
    if "demon" in pt:
        return "Demon"
    if side == "UNDER":
        return "Standard UNDER"
    if side == "OVER":
        return "Standard OVER"
    return pt or "other"


def sport_of(row: dict) -> str:
    return str(row.get("Sport") or row.get("sport") or row.get("league") or "").strip().upper()


def result_of(row: dict) -> str:
    r = str(row.get("Result") or row.get("result") or row.get("grade") or "").strip().upper()
    if r in ("HIT", "WIN", "W"):
        return "HIT"
    if r in ("MISS", "LOSS", "L"):
        return "MISS"
    return ""


def main() -> None:
    seen_files = {}
    for d in DIRS:
        if not d.is_dir():
            continue
        for p in d.glob("graded_props_*.json"):
            seen_files.setdefault(p.name, p)
    agg = defaultdict(lambda: {"n": 0, "h": 0, "dates": set()})
    props = defaultdict(lambda: {"n": 0, "h": 0})
    n_cfb = 0
    dates = set()
    for name, p in sorted(seen_files.items()):
        raw = json.loads(p.read_text(encoding="utf-8"))
        rows = raw if isinstance(raw, list) else (raw.get("props") or raw.get("rows") or raw.get("graded") or [])
        if isinstance(raw, dict) and not rows:
            for v in raw.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    rows = v
                    break
        day = name.replace("graded_props_", "").replace(".json", "")
        for row in rows:
            if not isinstance(row, dict):
                continue
            if sport_of(row) not in ("CFB", "NCAAF", "COLLEGE FOOTBALL"):
                continue
            res = result_of(row)
            if res not in ("HIT", "MISS"):
                continue
            n_cfb += 1
            dates.add(day)
            b = book_of(row)
            agg[b]["n"] += 1
            agg[b]["h"] += int(res == "HIT")
            agg[b]["dates"].add(day)
            prop = str(row.get("Prop") or row.get("prop") or row.get("stat_type") or "").strip()
            props[(b, prop)]["n"] += 1
            props[(b, prop)]["h"] += int(res == "HIT")
    print("CFB decided", n_cfb, "dates", sorted(dates))
    for b, a in sorted(agg.items(), key=lambda kv: -kv[1]["n"]):
        n, h = a["n"], a["h"]
        print(f"  {b:16} {100*h/n:.1f}% ({h}/{n}) days={len(a['dates'])}")
    print("\nBy prop n>=20:")
    for (b, prop), a in sorted(props.items(), key=lambda kv: (kv[0][0], -kv[1]["n"])):
        if a["n"] < 20:
            continue
        print(f"  {b:16} {prop:28} {100*a['h']/a['n']:.1f}% ({a['h']}/{a['n']})")


if __name__ == "__main__":
    main()
