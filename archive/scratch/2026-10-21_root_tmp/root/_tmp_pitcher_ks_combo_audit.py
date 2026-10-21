#!/usr/bin/env python3
"""Pitcher Ks vs Pitcher Ks (Combo) — SOG/SOT-style identity + VOID audit."""
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO / "scripts"), str(_REPO)]

import prop_hit_tiers as T  # noqa: E402
from utils.prop_norm import display_prop  # noqa: E402

dirs = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]
by_date: dict[str, Path] = {}
for d in dirs:
    if not d.is_dir():
        continue
    for p in sorted(d.glob("graded_props_20*.json")):
        if ".bak_" in p.name:
            continue
        date = p.stem.replace("graded_props_", "")[:10]
        if date not in by_date:
            by_date[date] = p

# Collect MLB rows whose raw prop looks like pitcher Ks / combo / plain Ks
raw_ctr: Counter = Counter()  # (raw_prop, result)
canon_ctr: Counter = Counter()  # (canon, raw_prop, result)
pick_ctr: Counter = Counter()  # (raw_prop, pick_type, result)
actual_stats: dict[str, Counter] = defaultdict(Counter)  # raw -> actual bucket


def actual_bucket(v) -> str:
    if v is None or v == "":
        return "null/blank"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return f"str:{v!r}"[:40]
    if f != f:  # nan
        return "nan"
    return "zero" if f == 0 else "nonzero"


needle = re.compile(
    r"pitcher.?strikeout|pitcher.?ks|\bks\b|strikeout.*combo|combo.*strikeout",
    re.I,
)

n_files = 0
for path in by_date.values():
    n_files += 1
    raw = json.loads(path.read_text(encoding="utf-8"))
    for r in raw.get("props") or raw.get("rows") or []:
        if not isinstance(r, dict):
            continue
        if T.norm_sport(str(r.get("sport") or "")) != "MLB":
            continue
        prop = str(r.get("prop") or r.get("prop_type") or "").strip()
        if not prop:
            continue
        # broad: anything with strikeout/ks that might be pitcher-side
        low = prop.lower()
        if not (
            "strikeout" in low
            or low in ("ks", "k", "pitcher ks", "pitcher k")
            or "combo" in low
            and ("k" in low or "strike" in low)
        ):
            continue
        # exclude clear hitter Ks
        if "hitter" in low or "batter" in low:
            continue
        res = str(r.get("result") or "").strip().upper() or "?"
        pick = str(r.get("pick_type") or "").strip() or "?"
        raw_ctr[(prop, res)] += 1
        pick_ctr[(prop, pick, res)] += 1
        canon = T.canon_prop("MLB", prop) or "(none)"
        canon_ctr[(canon, prop, res)] += 1
        act = r.get("actual")
        if act is None:
            act = r.get("actual_value")
        actual_stats[prop][actual_bucket(act)] += 1

print(f"scanned {n_files} graded dates\n")
print("=== RAW prop × result (pitcher-side K family) ===")
for (prop, res), n in sorted(raw_ctr.items(), key=lambda x: (-sum(1 for _ in [0]), -x[1] if False else -sum(v for (p, r), v in raw_ctr.items() if p == x[0][0]), x[0][0], x[0][1])):
    pass

# group by raw prop first
by_prop: dict[str, Counter] = defaultdict(Counter)
for (prop, res), n in raw_ctr.items():
    by_prop[prop][res] += n

for prop, ctr in sorted(by_prop.items(), key=lambda x: -sum(x[1].values())):
    total = sum(ctr.values())
    void = ctr.get("VOID", 0)
    hit = ctr.get("HIT", 0)
    miss = ctr.get("MISS", 0)
    decided = hit + miss
    void_pct = 100.0 * void / total if total else 0
    hr = 100.0 * hit / decided if decided else float("nan")
    canon = T.canon_prop("MLB", prop) or "(none)"
    disp = display_prop("MLB", canon) if canon != "(none)" else ""
    print(
        f"\nraw={prop!r}\n"
        f"  canon={canon!r} display={disp!r}\n"
        f"  n={total} HIT={hit} MISS={miss} VOID={void} ({void_pct:.1f}% void) "
        f"decided_hr={hr if decided else 'n/a'}\n"
        f"  actuals: {dict(actual_stats[prop])}"
    )

print("\n=== Same canon, different raw labels? ===")
by_canon: dict[str, set] = defaultdict(set)
for (canon, prop, res), n in canon_ctr.items():
    by_canon[canon].add(prop)
for canon, props in sorted(by_canon.items(), key=lambda x: -len(x[1])):
    if len(props) < 2 and "combo" not in " ".join(props).lower():
        continue
    print(f"  canon={canon!r} raw_labels={sorted(props)}")

print("\n=== Combo vs plain — collapse test ===")
combo_labels = [p for p in by_prop if "combo" in p.lower()]
plain_labels = [
    p
    for p in by_prop
    if "combo" not in p.lower()
    and ("pitcher" in p.lower() or p.lower() in ("ks", "k", "strikeouts"))
]
print("combo raw labels:", combo_labels)
print("plain pitcher-ish raw labels:", plain_labels)
for cl in combo_labels:
    cc = T.canon_prop("MLB", cl)
    for pl in plain_labels:
        pc = T.canon_prop("MLB", pl)
        same = cc == pc and cc
        print(f"  {cl!r} canon={cc!r}  vs  {pl!r} canon={pc!r}  SAME={same}")

# Sample a few VOID combo rows for cascade clues
print("\n=== Sample VOID Combo rows (fields) ===")
shown = 0
for path in list(by_date.values())[-40:]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    for r in raw.get("props") or raw.get("rows") or []:
        if not isinstance(r, dict):
            continue
        if T.norm_sport(str(r.get("sport") or "")) != "MLB":
            continue
        prop = str(r.get("prop") or r.get("prop_type") or "")
        if "combo" not in prop.lower():
            continue
        if "strike" not in prop.lower() and "ks" not in prop.lower() and "k " not in prop.lower():
            if not re.search(r"\bk\b", prop.lower()):
                continue
        res = str(r.get("result") or "").upper()
        if res != "VOID":
            continue
        keys = [
            "prop",
            "prop_type",
            "result",
            "void_reason",
            "void_note",
            "actual",
            "actual_value",
            "line",
            "pick_type",
            "player",
            "grading_note",
            "note",
        ]
        snap = {k: r.get(k) for k in keys if r.get(k) not in (None, "")}
        # also any *actual* or *void* keys
        for k, v in r.items():
            if ("void" in k.lower() or "actual" in k.lower() or "combo" in k.lower()) and v not in (
                None,
                "",
            ):
                snap[k] = v
        print(f"  date={path.stem[-10:]} {snap}")
        shown += 1
        if shown >= 12:
            break
    if shown >= 12:
        break

print("\nDONE")
