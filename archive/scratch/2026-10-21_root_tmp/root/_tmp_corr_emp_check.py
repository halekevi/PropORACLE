"""Post-empirical-fix: sanity + calibration_gap cross-check."""
from __future__ import annotations

import json
from pathlib import Path

cells = json.loads(Path("data/cache/leg_correlation_matrix.json").read_text(encoding="utf-8"))
CATS = [
    "cat1_same_player_diff_stat",
    "cat2_same_team_diff_player_same_stat",
    "cat3_opposing_teams_same_game",
    "cat4_baseline_diff_game",
]

print("=== n-weighted lifts by sport x cat (empirical baseline) ===")
for sport in sorted(cells):
    for cat in CATS:
        entries = cells[sport].get(cat) or []
        if not entries:
            continue
        wsum = sum(e["n"] for e in entries)
        wraw = sum(e["raw_lift"] * e["n"] for e in entries) / wsum
        wshr = sum(e["shrunk_lift"] * e["n"] for e in entries) / wsum
        print(f"{sport:12} {cat:40} sum_n={wsum:10d} w_raw={wraw:7.3f} w_shr={wshr:7.3f}")

print("\n=== calibration_gap (emp - model) on cat3 cells, by sport ===")
print("(negative = model overconfident — matches soccer/MLB OVER diagnosis)")
for sport in ["Soccer", "MLB", "Tennis", "WNBA", "NBA", "NHL"]:
    entries = cells.get(sport, {}).get("cat3_opposing_teams_same_game") or []
    if not entries:
        entries = cells.get(sport, {}).get("cat1_same_player_diff_stat") or []
    if not entries:
        continue
    wsum = sum(e["n"] for e in entries)
    gap_a = sum(e.get("calibration_gap_a", 0) * e["n"] for e in entries) / wsum
    gap_b = sum(e.get("calibration_gap_b", 0) * e["n"] for e in entries) / wsum
    print(f"{sport:12} n={wsum:10d} mean_gap_a={gap_a:+.3f} mean_gap_b={gap_b:+.3f}")

print("\n=== top Soccer cat4 cells by n (why baseline ~1.74?) ===")
soc = sorted(
    cells.get("Soccer", {}).get("cat4_baseline_diff_game") or [],
    key=lambda e: -e["n"],
)[:8]
for e in soc:
    print(
        f"  n={e['n']:6d} raw={e['raw_lift']:.3f} shr={e['shrunk_lift']:.3f} "
        f"{e['stat_a']}/{e['direction_a']} x {e['stat_b']}/{e['direction_b']} "
        f"gap_a={e.get('calibration_gap_a')} gap_b={e.get('calibration_gap_b')}"
    )
