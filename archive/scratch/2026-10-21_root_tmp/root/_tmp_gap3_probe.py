#!/usr/bin/env python3
"""Probe #3: are 'missing' ordinary props absent from catalog, filtered, or just missed in the write-up?"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parent
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from utils.fantasy_prop_filter import is_fantasy_prop_label  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from utils.prop_norm import display_prop  # noqa: E402

CATALOG = json.loads(
    (_REPO / "data" / "reports" / "all_ranked_prop_hit_rates.json").read_text(encoding="utf-8")
)
rows = CATALOG["rows"]

TARGETS = [
    ("Soccer", ("tackles", "crosses", "fantasy")),
    ("WNBA", ("fantasy",)),
    ("Tennis", ("fantasy", "total sets", "break points", "tie break", "sets", "bp")),
]


def catalog_hits(sport: str, needles: tuple[str, ...]) -> list[dict]:
    out = []
    for r in rows:
        if r["sport"] != sport:
            continue
        blob = f"{r.get('prop_display','')} {r.get('prop','')}".lower()
        if any(n in blob for n in needles):
            out.append(r)
    return out


print("=== A. Master catalog (all_ranked_prop_hit_rates.json) ===")
print(f"window {CATALOG.get('from')} -> {CATALOG.get('to')}  decided={CATALOG.get('n_decided')}")
for sport, needles in TARGETS:
    hits = catalog_hits(sport, needles)
    print(f"\n{sport} needles={needles}: {len(hits)} cells")
    for r in sorted(hits, key=lambda x: (-x["n"], x["book"])):
        print(
            f"  {r['book']:18} {r['prop_display']:30} n={r['n']:5} "
            f"hr={r['hr']}% prop={r['prop']}"
        )


# Scan graded_props for raw labels (same discovery order as catalog builder)
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]


def find_graded() -> list[Path]:
    seen: dict[str, Path] = {}
    for d in GRADED_DIRS:
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_*.json")):
            m = re.search(r"graded_props_(\d{4}-\d{2}-\d{2})", p.name)
            if not m:
                continue
            date = m.group(1)
            if date not in seen:
                seen[date] = p
    return [seen[k] for k in sorted(seen)]


raw_counts: dict[tuple[str, str], Counter] = defaultdict(Counter)
fantasy_raw: Counter = Counter()
canon_miss: Counter = Counter()  # sport|raw -> count where canon empty
fantasy_dropped: Counter = Counter()

paths = find_graded()
print(f"\n=== B. Raw graded_props scan ({len(paths)} dates) ===")
for path in paths:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        continue
    props = data.get("props") or data.get("rows") or []
    if not isinstance(props, list):
        continue
    for r in props:
        if not isinstance(r, dict):
            continue
        res = str(r.get("result") or "").strip().upper()
        if res not in ("HIT", "MISS"):
            continue
        sport = T.norm_sport(str(r.get("sport") or ""))
        raw = str(r.get("prop") or r.get("prop_type") or "").strip()
        if not raw:
            continue
        low = raw.lower()
        interesting = False
        if sport == "Soccer" and any(x in low for x in ("tackle", "cross", "fantasy")):
            interesting = True
        if sport == "WNBA" and "fantasy" in low:
            interesting = True
        if sport == "Tennis" and any(
            x in low
            for x in (
                "fantasy",
                "total set",
                "sets",
                "break point",
                "tie break",
                "tiebreak",
            )
        ):
            interesting = True
        if not interesting:
            continue
        raw_counts[(sport, raw)][res] += 1
        if is_fantasy_prop_label(raw) or "fantasy" in low:
            fantasy_raw[(sport, raw)] += 1
            fantasy_dropped[(sport, raw)] += 1
        canon = T.canon_prop(sport, raw)
        if not canon:
            canon_miss[(sport, raw)] += 1

print("\n--- Raw HIT/MISS counts (interesting labels) ---")
for (sport, raw), ctr in sorted(raw_counts.items(), key=lambda x: (-sum(x[1].values()), x[0])):
    n = sum(ctr.values())
    hits = ctr.get("HIT", 0)
    hr = 100.0 * hits / n if n else 0
    fantasy = "FANTASY-DROP" if is_fantasy_prop_label(raw) else ""
    canon = T.canon_prop(sport, raw) or "(no canon)"
    disp = display_prop(sport, canon) if canon and canon != "(no canon)" else ""
    in_catalog = any(
        r["sport"] == sport
        and (
            r.get("prop") == canon
            or raw.lower() in (r.get("prop_display") or "").lower()
            or (r.get("prop_display") or "").lower() in raw.lower()
        )
        for r in rows
        if r["sport"] == sport
    )
    print(
        f"  {sport:8} raw={raw!r:40} n={n:5} hr={hr:5.1f}% "
        f"canon={canon!r:20} catalog={'Y' if in_catalog else 'N'} {fantasy}"
    )

print("\n--- Fantasy labels that would be dropped by catalog builder ---")
for (sport, raw), n in sorted(fantasy_dropped.items(), key=lambda x: -x[1]):
    print(f"  {sport:8} {raw!r:40} n={n}")

print("\n--- Interesting labels with NO canon_prop mapping ---")
for (sport, raw), n in sorted(canon_miss.items(), key=lambda x: -x[1])[:30]:
    print(f"  {sport:8} {raw!r:40} n={n}")

# Explicit Break Points Won / Total Sets checks
print("\n=== C. Tennis Break Points / Sets / Tie Breaks deep dive ===")
bp_labels = Counter()
for path in paths:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        continue
    props = data.get("props") or data.get("rows") or []
    if not isinstance(props, list):
        continue
    for r in props:
        if not isinstance(r, dict):
            continue
        if T.norm_sport(str(r.get("sport") or "")) != "Tennis":
            continue
        raw = str(r.get("prop") or r.get("prop_type") or "")
        low = raw.lower()
        if any(x in low for x in ("break", "set", "tie", "fantasy")):
            res = str(r.get("result") or "").strip().upper() or "?"
            bp_labels[(raw, res)] += 1

for (raw, res), n in sorted(bp_labels.items(), key=lambda x: (-x[1], x[0][0], x[0][1]))[:40]:
    print(f"  {raw!r:45} {res:6} n={n}")

print("\nDONE")
