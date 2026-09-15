import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from export_soccer_hit_rates_excel import load_graded

WEAK = {"Weak", "Below Avg"}
STINGY = {"Elite", "Above Avg"}
focus = {"shots", "shots on target", "goals", "goal + assist", "assists", "goalie saves"}
g = load_graded()
base = g[g["list_shape"] & g["prop_key"].isin(focus)].copy()
print("prop | dir | pick | all | leaky Weak|Below | stingy Elite|Above")
for prop, sub in sorted(base.groupby("prop_key")):
    for direction in ("OVER", "UNDER"):
        for pick in ("ALL", "Goblin", "Standard"):
            p = sub[sub["direction"] == direction]
            if pick != "ALL":
                p = p[p["pick_type"] == pick]
            if len(p) < 20:
                continue
            t = p["def_tier"].fillna("").astype(str)
            leak = p[t.isin(WEAK)]
            sting = p[t.isin(STINGY)]

            def hr(s):
                n = len(s)
                if n == 0:
                    return "—"
                return f"{int(s.hit.sum())}/{n}={100 * s.hit.mean():.1f}%"

            print(f"{prop} | {direction} | {pick} | {hr(p)} | {hr(leak)} | {hr(sting)}")
