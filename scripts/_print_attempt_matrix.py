import json
from pathlib import Path

by = json.loads(
    Path("data/reports/nba_attempt_l5eq5_2026-10-22.json").read_text(encoding="utf-8")
)["backtest"]["by_family_pick"]
print("Card-window L5=5 graded replay — hit rate by family x pick_type")
print(f"{'family':<10}{'goblin':>22}{'standard':>22}{'demon':>22}")
for fam in ("attempt", "core", "combo"):
    cells = []
    for pk in ("goblin", "standard", "demon"):
        v = by[f"{fam}|{pk}"]
        n, h = v["n"], v["hits"]
        tag = "n>=100" if n >= 100 else "obs"
        cells.append(f"{100 * h / n:5.1f}% {h}/{n} ({tag})")
    print(f"{fam:<10}{cells[0]:>22}{cells[1]:>22}{cells[2]:>22}")
print()
print("Edge check (attempts minus same-pick core / combo):")
for pk in ("goblin", "standard", "demon"):
    a = by[f"attempt|{pk}"]["hit_rate"]
    c = by[f"core|{pk}"]["hit_rate"]
    m = by[f"combo|{pk}"]["hit_rate"]
    print(
        f"  {pk}: attempt {100*a:.1f}%  vs core {100*c:.1f}% ({100*(a-c):+.1f})  "
        f"vs combo {100*m:.1f}% ({100*(a-m):+.1f})"
    )
