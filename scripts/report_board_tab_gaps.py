"""Compare each sport's step1 card labels vs expected PrizePicks board tabs.

  py -3.14 scripts/report_board_tab_gaps.py
"""
from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

ROOT = Path(r"H:\PropORACLE_main_cp\outputs\2026-10-20")

# Expected tabs from today's PrizePicks boards (card/chip labels).
EXPECTED: dict[str, tuple[str, ...]] = {
    "NBA": (
        "Points",
        "Rebounds",
        "Assists",
        "3-PT Made",
        "3-PT Attempted",
        "FG Made",
        "FG Attempted",
        "Two Pointers Made",
        "Two Pointers Attempted",
        "Free Throws Made",
        "Free Throws Attempted",
        "Defensive Rebounds",
        "Offensive Rebounds",
        "Turnovers",
        "Personal Fouls",
        "Steals",
        "Blocked Shots",
        "Dunks",
        "Fantasy Score",
        "Pts+Rebs+Asts",
        "Pts+Rebs",
        "Pts+Asts",
        "Rebs+Asts",
        "Blks+Stls",
        "Triple-Double",
        "Double-Double",
        "Points (Combo)",
        "Rebounds (Combo)",
        "Assists (Combo)",
        "3-PT Made (Combo)",
    ),
    "NHL": (
        "Shots On Goal",
        "Goals",
        "Assists",
        "Points",
        "Hits",
        "Blocked Shots",
        "Faceoffs Won",
        "Goalie Saves",
        "Plus/Minus",
        "Power Play Points",
        "Time On Ice",
        "Fantasy Score",
    ),
    "WNBA": (
        "Points",
        "Rebounds",
        "Assists",
        "3-PT Made",
        "FG Made",
        "FG Attempted",
        "Pts+Rebs+Asts",
        "Pts+Rebs",
        "Pts+Asts",
        "Rebs+Asts",
        "Steals",
        "Blocked Shots",
        "Turnovers",
        "Fantasy Score",
    ),
    "NFL": (
        "Pass Yards",
        "Rush Yards",
        "Receiving Yards",
        "Receptions",
        "Pass Completions",
        "Pass Attempts",
        "Rush Attempts",
        "Rush+Rec Yds",
        "Pass+Rush Yds",
        "Longest Reception",
        "Longest Rush",
        "FG Made",
        "Kicking Points",
        "Tackles+Ast",
        "Fantasy Score",
    ),
    "SOCCER": (
        "Shots",
        "Shots On Target",
        "Goalie Saves",
        "Goals",
        "Assists",
        "Fouls",
        "Fantasy Score",
    ),
    "TENNIS": (
        "Total Games",
        "Total Games Won",
        "Aces",
        "Double Faults",
        "Break Points Won",
        "Fantasy Score",
    ),
    "MLB": (
        "Hits",
        "Total Bases",
        "Pitcher Strikeouts",
        "Hits Allowed",
        "Pitching Outs",
        "Walks Allowed",
        "Earned Runs",
        "Hits+Runs+RBIs",
        "Fantasy Score",
    ),
}

PATHS = {
    "NBA": ROOT / "nba" / "step1_pp_props_today.csv",
    "NHL": ROOT / "nhl" / "step1_nhl_props.csv",
    "WNBA": ROOT / "wnba" / "step1_wnba_props.csv",
    "NFL": ROOT / "nfl" / "step1_pp_props_today.csv",
    "SOCCER": ROOT / "soccer" / "step1_soccer_props.csv",
    "TENNIS": ROOT / "tennis" / "step1_tennis_props.csv",
    "MLB": ROOT / "mlb" / "step1_mlb_props.csv",
}


def load_types(path: Path) -> Counter[str]:
    if not path.is_file() or path.stat().st_size < 200:
        return Counter()
    with path.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return Counter()
    col = next(
        (
            c
            for c in ("prop_type", "stat_type", "Prop", "prop")
            if c in rows[0]
        ),
        None,
    )
    if not col:
        return Counter()
    return Counter(str(r.get(col) or "").strip() for r in rows if str(r.get(col) or "").strip())


print("Board-tab gaps by sport (expected tab with fetched=0):\n")
for sport, expected in EXPECTED.items():
    path = PATHS[sport]
    types = load_types(path)
    if not types:
        print(f"===== {sport} =====")
        print(f"  (no step1 file: {path})")
        continue
    missing = [t for t in expected if types.get(t, 0) == 0]
    # Fantasy may be dropped post-fetch
    missing_nf = [t for t in missing if t != "Fantasy Score"]
    print(f"===== {sport} =====  fetched_types={len(types)} rows={sum(types.values())}")
    if not missing:
        print("  OK — every expected tab has rows")
    else:
        for t in missing:
            note = " (should be listed ticketable=false)" if t == "Fantasy Score" else ""
            print(f"  GAP  {t}{note}")
    extras = sorted(k for k in types if k not in expected)[:12]
    if extras:
        print(f"  extra sample: {', '.join(extras)}")
