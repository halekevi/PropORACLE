"""Compare historical step1 unique prop tab counts vs required board tabs.

Flags slates whose step1 has fewer required tabs than a complete board.
"""
from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from utils.pp_board_prop_map import lookup_category, required_keys

ROOTS = [
    Path(r"H:\PropORACLE_main_cp\outputs"),
    Path(r"H:\PropORACLE\outputs"),
]

SPORT_FILES = {
    "NBA": ("nba", ("step1_pp_props_today.csv", "step1_nba_props.csv")),
    "NHL": ("nhl", ("step1_nhl_props.csv",)),
    "TENNIS": ("tennis", ("step1_tennis_props.csv",)),
    "SOCCER": ("soccer", ("step1_soccer_props.csv",)),
    "WNBA": ("wnba", ("step1_wnba_props.csv",)),
    "NFL": ("nfl", ("step1_pp_props_today.csv", "step1_nfl_props.csv")),
    "MLB": ("mlb", ("step1_mlb_props.csv",)),
}


def load_types(path: Path) -> Counter[str]:
    if not path.is_file() or path.stat().st_size < 200:
        return Counter()
    with path.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return Counter()
    col = next(
        (c for c in ("prop_type", "stat_type", "Prop", "prop") if c in rows[0]),
        None,
    )
    if not col:
        return Counter()
    return Counter(
        str(r.get(col) or "").strip() for r in rows if str(r.get(col) or "").strip()
    )


def keys_present(sport: str, types: Counter[str]) -> set[str]:
    keys: set[str] = set()
    for label in types:
        cat = lookup_category(sport, label)
        if cat:
            keys.add(cat.key)
    return keys


print("Step1 staleness (required tabs missing vs complete board)\n")
incomplete: list[str] = []
for sport, (folder, names) in SPORT_FILES.items():
    required = required_keys(sport)
    if not required:
        continue
    print(f"===== {sport} =====")
    print(f"{'slate':12} {'root':22} {'rows':6} {'tabs':4} {'req_ok':6} missing/status")
    seen: set[tuple[str, str]] = set()
    for root in ROOTS:
        if not root.is_dir():
            continue
        root_tag = root.parent.name
        for day_dir in sorted(root.iterdir()):
            if not day_dir.is_dir() or not day_dir.name.startswith("2026-"):
                continue
            path = None
            for name in names:
                cand = day_dir / folder / name
                if cand.is_file():
                    path = cand
                    break
            if path is None:
                continue
            key = (day_dir.name, root_tag, sport)
            if key in seen:
                continue
            seen.add(key)
            types = load_types(path)
            if not types:
                print(
                    f"{day_dir.name:12} {root_tag:22} {'empty':>6} {'—':>4} {'—':>6} "
                    f"empty_file/no_slate"
                )
                incomplete.append(f"{sport} {day_dir.name} ({root_tag}): empty_file")
                continue
            present = keys_present(sport, types)
            miss = sorted(required - present)
            ok = "Y" if not miss else "N"
            thick = sum(types.values()) >= 40
            if miss and thick:
                incomplete.append(
                    f"{sport} {day_dir.name} ({root_tag}): missing {', '.join(miss)}"
                )
            print(
                f"{day_dir.name:12} {root_tag:22} {sum(types.values()):6} "
                f"{len(types):4} {ok:>6} {', '.join(miss) if miss else '—'}"
            )
    print()

print(f"Incomplete thick boards: {len(incomplete)}")
for line in incomplete:
    print(" ", line)
